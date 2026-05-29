use alloc::collections::vec_deque::VecDeque;
use core::cell::UnsafeCell;
use core::future::Future;
use core::mem;
use core::ptr::{self, NonNull};

pub use async_task::Task;
use async_task::{Runnable, ScheduleInfo, WithInfo};
use nginx_sys::{
    ngx_del_timer, ngx_delete_posted_event, ngx_event_t, ngx_post_event, ngx_posted_next_events,
};

use crate::log::ngx_cycle_log;
use crate::{ngx_container_of, ngx_log_debug};

static SCHEDULER: Scheduler = Scheduler::new();

struct Scheduler(UnsafeCell<SchedulerInner>);

// SAFETY: Scheduler must only be used from the main thread of a worker process.
unsafe impl Send for Scheduler {}
unsafe impl Sync for Scheduler {}

impl Scheduler {
    const fn new() -> Self {
        Self(SchedulerInner::new())
    }

    pub fn schedule(&self, runnable: Runnable) {
        // SAFETY: the cell is not empty, and we have exclusive access due to being a
        // single-threaded application.
        let inner = unsafe { &mut *UnsafeCell::raw_get(&raw const self.0) };
        inner.send(runnable)
    }
}

#[repr(C)]
struct SchedulerInner {
    _ident: [usize; 4], // `ngx_event_ident` compatibility
    event: ngx_event_t,
    queue: VecDeque<Runnable>,
}

impl SchedulerInner {
    const fn new() -> UnsafeCell<Self> {
        let mut event: ngx_event_t = unsafe { mem::zeroed() };
        event.handler = Some(Self::scheduler_event_handler);

        UnsafeCell::new(Self {
            _ident: [
                0, 0, 0, 0x4153594e, // ASYN
            ],
            event,
            queue: VecDeque::new(),
        })
    }

    pub fn send(&mut self, runnable: Runnable) {
        // Cached `ngx_cycle.log` can be invalidated when reloading configuration in a single
        // process mode. Update `log` every time to avoid using stale log pointer.
        self.event.log = ngx_cycle_log().as_ptr();

        // While this event is not used as a timer at the moment, we still want to ensure that it is
        // compatible with `ngx_event_ident`.
        if self.event.data.is_null() {
            self.event.data = ptr::from_mut(self).cast();
        }

        // FIXME: VecDeque::push could panic on an allocation failure, switch to a datastructure
        // which will not and propagate the failure.
        self.queue.push_back(runnable);
        unsafe { ngx_post_event(&raw mut self.event, &raw mut ngx_posted_next_events) }
    }

    /// This event handler is called by ngx_event_process_posted at the end of
    /// ngx_process_events_and_timers.
    extern "C" fn scheduler_event_handler(ev: *mut ngx_event_t) {
        let mut runnables = {
            // SAFETY:
            // This handler always receives a non-null pointer to an event embedded into a
            // UnsafeCell<SchedulerInner> instance. We modify the contents of the `UnsafeCell`,
            // but we ensured that:
            //  - we access the cell correctly, as documented in https://doc.rust-lang.org/stable/std/cell/struct.UnsafeCell.html#memory-layout
            //  - the access is unique due to being single-threaded
            //  - the reference is dropped before we start processing queued runnables.
            let cell: NonNull<UnsafeCell<Self>> =
                ngx_container_of!(unsafe { NonNull::new_unchecked(ev) }, Self, event).cast();
            let this = unsafe { &mut *UnsafeCell::raw_get(cell.as_ptr()) };

            ngx_log_debug!(
                this.event.log,
                "async: processing {} deferred wakeups",
                this.queue.len()
            );

            // Move runnables to a new queue to avoid borrowing from the SchedulerInner and limit
            // processing to already queued wakeups. This ensures that we correctly handle tasks
            // that keep scheduling themselves (e.g. using yield_now() in a loop).
            // We can't use drain() as it borrows from self and breaks aliasing rules.
            mem::take(&mut this.queue)
        };

        for runnable in runnables.drain(..) {
            runnable.run();
        }
    }
}

impl Drop for SchedulerInner {
    fn drop(&mut self) {
        if self.event.posted() != 0 {
            unsafe { ngx_delete_posted_event(&raw mut self.event) };
        }

        if self.event.timer_set() != 0 {
            unsafe { ngx_del_timer(&raw mut self.event) };
        }
    }
}

fn schedule(runnable: Runnable, info: ScheduleInfo) {
    // Always defer the wake via `ngx_post_event`; never re-poll synchronously.
    //
    // `Waker::wake()` may fire from arbitrary contexts, including a future's
    // `Drop` while a lock is held (e.g. h2's `Streams::drop` wakes its parked
    // `Connection` task while holding `Arc<Mutex<Inner>>`). A synchronous
    // re-poll would re-enter the task and deadlock on that lock. Deferring
    // costs one event-loop tick: `ngx_event_process_posted` drains the queue
    // at the end of each cycle.
    SCHEDULER.schedule(runnable);
    if info.woken_while_running {
        ngx_log_debug!(ngx_cycle_log().as_ptr(), "async: task scheduled while running");
    } else {
        ngx_log_debug!(ngx_cycle_log().as_ptr(), "async: task scheduled (deferred)");
    }
}

/// Creates a new task running on the NGINX event loop.
pub fn spawn<F, T>(future: F) -> Task<T>
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    ngx_log_debug!(ngx_cycle_log().as_ptr(), "async: spawning new task");
    let scheduler = WithInfo(schedule);
    // Safety: single threaded embedding takes care of send/sync requirements for future and
    // scheduler. Future and scheduler are both 'static.
    let (runnable, task) = unsafe { async_task::spawn_unchecked(future, scheduler) };
    runnable.schedule();
    task
}

#[cfg(test)]
mod tests {
    //! Freestanding reproducer for the `Waker::wake()` contract violation that
    //! [`schedule`] fixes.
    //!
    //! Background: `Waker::wake()` may be called from arbitrary contexts,
    //! including inside another future's `Drop` impl while a lock is held. The
    //! motivating case is h2's `Streams::drop`, which locks an
    //! `Arc<Mutex<Inner>>` and — still holding the guard — wakes the parked
    //! `Connection` task. If the executor's `schedule()` re-polls that task
    //! *synchronously on the waker's stack*, the re-poll tries to lock the same
    //! `Mutex` and deadlocks.
    //!
    //! These tests reproduce that shape with no external crates beyond
    //! `async_task` (already a dependency). They model the two schedule
    //! strategies as custom `schedule` functions handed to
    //! `async_task::spawn_unchecked`:
    //!
    //! * the pre-patch strategy ([`sync_repoll_reproduces_held_lock_deadlock`]),
    //!   which re-polls synchronously when a task is woken outside its own poll,
    //!   and
    //! * the post-patch strategy ([`deferred_schedule_avoids_held_lock_deadlock`]),
    //!   which always queues the wake (mirroring `schedule`'s `ngx_post_event`
    //!   deferral) and drains it after the waking stack frame has unwound.
    //!
    //! The parked future probes the contended lock with `try_lock` rather than
    //! `lock`, so the *real* deadlock surfaces as an observable
    //! `TryLockError::WouldBlock` instead of hanging the test thread.
    extern crate std;

    use alloc::collections::VecDeque;
    use alloc::rc::Rc;
    use core::cell::{Cell, RefCell};
    use core::future::Future;
    use core::pin::Pin;
    use core::task::{Context, Poll, Waker};
    use std::sync::Mutex;

    use async_task::{Runnable, ScheduleInfo, WithInfo};

    /// What the parked task observed about the shared lock when it was re-polled.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Observation {
        /// The task has not been re-polled yet.
        NotRepolled,
        /// Re-poll acquired the lock — the waker's stack frame had released it.
        LockFree,
        /// Re-poll found the lock held — the deadlock signature (the real code
        /// would block here).
        LockHeld,
    }

    /// Stands in for h2's parked `Connection` task. On its first poll it parks
    /// and publishes its waker; on re-poll it probes the shared lock.
    struct ConnectionTask {
        inner: Rc<Mutex<()>>,
        waker_slot: Rc<RefCell<Option<Waker>>>,
        obs: Rc<Cell<Observation>>,
        polls: u32,
    }

    impl Future for ConnectionTask {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let this = self.get_mut();
            this.polls += 1;
            if this.polls == 1 {
                // First poll: park, stashing our waker so `StreamsDrop` can wake
                // us later.
                *this.waker_slot.borrow_mut() = Some(cx.waker().clone());
                return Poll::Pending;
            }

            // Re-poll, triggered by the wake from `StreamsDrop::drop`. Try to
            // take the same lock the waker's caller may still be holding.
            // `try_lock` (not `lock`) lets us *observe* the deadlock condition
            // without hanging: a real `lock()` here is what wedges the worker
            // pre-patch.
            match this.inner.try_lock() {
                Ok(_guard) => this.obs.set(Observation::LockFree),
                Err(_would_block) => this.obs.set(Observation::LockHeld),
            }
            Poll::Ready(())
        }
    }

    /// Stands in for h2's `Streams::drop`: takes the shared `Inner` lock and,
    /// while still holding the guard, wakes the parked `Connection` task.
    struct StreamsDrop {
        inner: Rc<Mutex<()>>,
        waker_slot: Rc<RefCell<Option<Waker>>>,
    }

    impl Drop for StreamsDrop {
        fn drop(&mut self) {
            let _guard = self.inner.lock().expect("inner mutex poisoned");
            if let Some(waker) = self.waker_slot.borrow().as_ref() {
                waker.wake_by_ref();
            }
            // `_guard` is released here, after the wake has returned.
        }
    }

    /// Run every queued runnable, releasing the queue borrow before each
    /// `run()` so a runnable may re-enqueue itself (mirrors
    /// `scheduler_event_handler` draining `ngx_posted_next_events`).
    fn drain(queue: &Rc<RefCell<VecDeque<Runnable>>>) {
        loop {
            let next = queue.borrow_mut().pop_front();
            match next {
                Some(runnable) => {
                    let _ = runnable.run();
                }
                None => break,
            }
        }
    }

    /// Shared state threaded through a single reproducer run.
    struct Fixture {
        /// Stands in for h2's `Arc<Mutex<Inner>>`.
        inner: Rc<Mutex<()>>,
        /// Where the parked `ConnectionTask` publishes its waker.
        waker_slot: Rc<RefCell<Option<Waker>>>,
        /// What the re-poll observed about `inner`.
        obs: Rc<Cell<Observation>>,
        /// Deferred-wake queue, standing in for `ngx_posted_next_events`.
        queue: Rc<RefCell<VecDeque<Runnable>>>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                inner: Rc::new(Mutex::new(())),
                waker_slot: Rc::new(RefCell::new(None)),
                obs: Rc::new(Cell::new(Observation::NotRepolled)),
                queue: Rc::new(RefCell::new(VecDeque::new())),
            }
        }

        fn connection_task(&self) -> ConnectionTask {
            ConnectionTask {
                inner: self.inner.clone(),
                waker_slot: self.waker_slot.clone(),
                obs: self.obs.clone(),
                polls: 0,
            }
        }

        fn streams_drop(&self) -> StreamsDrop {
            StreamsDrop { inner: self.inner.clone(), waker_slot: self.waker_slot.clone() }
        }
    }

    /// Pre-patch behaviour: a task woken outside its own poll
    /// (`woken_while_running == false`) is re-polled synchronously on the
    /// waker's stack. Because the wake originates from `StreamsDrop::drop`
    /// while the lock is held, the re-poll observes the lock as still held —
    /// the deadlock signature.
    #[test]
    fn sync_repoll_reproduces_held_lock_deadlock() {
        let fixture = Fixture::new();
        let future = fixture.connection_task();

        let q = fixture.queue.clone();
        let schedule = WithInfo(move |runnable: Runnable, info: ScheduleInfo| {
            if info.woken_while_running {
                // async_task's own re-entrancy guard: a wake during an active
                // poll is always deferred, even pre-patch.
                q.borrow_mut().push_back(runnable);
            } else {
                // The fc67e17 contract violation: synchronous re-poll on the
                // waker's stack.
                let _ = runnable.run();
            }
        });

        // SAFETY: this test is single-threaded; the future, its waker, and the
        // schedule closure are created, scheduled, and run entirely on the
        // current thread, so the `Send`/`Sync` bounds relaxed by
        // `spawn_unchecked` are upheld — the same rationale as `spawn`.
        let (runnable, _task) = unsafe { async_task::spawn_unchecked(future, schedule) };
        runnable.schedule(); // initial schedule -> first poll runs inline -> parks
        assert_eq!(fixture.obs.get(), Observation::NotRepolled);
        assert!(
            fixture.waker_slot.borrow().is_some(),
            "task should have parked its waker on first poll"
        );

        // Dropping `Streams` wakes the parked task *while holding the lock*.
        drop(fixture.streams_drop());

        assert_eq!(
            fixture.obs.get(),
            Observation::LockHeld,
            "synchronous re-poll re-entered while the lock was held: deadlock signature"
        );
        assert!(
            fixture.queue.borrow().is_empty(),
            "the buggy path re-polls inline; nothing should have been deferred"
        );
    }

    /// Post-patch behaviour (fc67e17): the wake is *always* deferred to the
    /// queue, regardless of `woken_while_running`. By the time the queue is
    /// drained, `StreamsDrop::drop` has returned and released the lock, so the
    /// re-poll acquires it cleanly. No deadlock.
    #[test]
    fn deferred_schedule_avoids_held_lock_deadlock() {
        let fixture = Fixture::new();
        let future = fixture.connection_task();

        let q = fixture.queue.clone();
        let schedule = WithInfo(move |runnable: Runnable, _info: ScheduleInfo| {
            // Always defer — never re-poll synchronously.
            q.borrow_mut().push_back(runnable);
        });

        // SAFETY: as in `sync_repoll_reproduces_held_lock_deadlock` — entirely
        // single-threaded.
        let (runnable, _task) = unsafe { async_task::spawn_unchecked(future, schedule) };
        runnable.schedule(); // initial schedule is queued, not run
        drain(&fixture.queue); // first poll -> parks
        assert!(
            fixture.waker_slot.borrow().is_some(),
            "task should have parked its waker on first poll"
        );
        assert_eq!(fixture.obs.get(), Observation::NotRepolled);

        // Dropping `Streams` queues the wake; the lock guard is released when
        // `drop` returns, before we drain.
        drop(fixture.streams_drop());

        drain(&fixture.queue);

        assert_eq!(
            fixture.obs.get(),
            Observation::LockFree,
            "deferred re-poll ran after the lock was released: no deadlock"
        );
    }
}
