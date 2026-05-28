#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_channel.h>

/* __has_include was a compiler-specific extension until C23,
 * but it's safe to assume that bindgen supports it via libclang.
 */
#if defined(__has_include)

#if __has_include(<ngx_http.h>)
#include <ngx_http.h>
#endif

#if __has_include(<ngx_stream.h>)
#include <ngx_stream.h>
#endif

#else
#include <ngx_http.h>
#endif

const char *NGX_RS_MODULE_SIGNATURE = NGX_MODULE_SIGNATURE;

// NGX_ALIGNMENT could be defined as a constant or an expression, with the
// latter being unsupported by bindgen.
const size_t NGX_RS_ALIGNMENT = NGX_ALIGNMENT;

// NGX_READ_EVENT / NGX_WRITE_EVENT are #define'd per event-module in
// src/event/ngx_event.h.  On kqueue (macOS) they expand to a single token
// (EVFILT_READ / EVFILT_WRITE) and bindgen lifts them to `pub const`.  On
// epoll (Linux) they expand to a parenthesised compound expression
// `(EPOLLIN|EPOLLRDHUP)` / `EPOLLOUT`, which bindgen drops.  Re-binding the
// macro through a file-scope `const` lets bindgen evaluate the initializer
// and emit a Rust constant uniformly across event mechanisms — same trick
// as NGX_RS_ALIGNMENT above.
const ngx_int_t NGX_RS_READ_EVENT  = NGX_READ_EVENT;
const ngx_int_t NGX_RS_WRITE_EVENT = NGX_WRITE_EVENT;

// `--prefix=` results in not emitting the declaration
#ifndef NGX_PREFIX
#define NGX_PREFIX ""
#endif

#ifndef NGX_CONF_PREFIX
#define NGX_CONF_PREFIX NGX_PREFIX
#endif
