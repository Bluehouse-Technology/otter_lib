-ifndef(OTTER_HRL).
-define(OTTER_HRL, "otter.hrl").

-type time_us() :: non_neg_integer().           % timestamp in microseconds
-type info()    :: binary() | iolist() | atom() | integer().
-type ip4()     :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}.
-type service() :: binary() | list() | default | {binary() | list(), ip4(), non_neg_integer()}.
-type trace_id():: non_neg_integer().
-type span_id() :: non_neg_integer().
-type action()  :: atom() | tuple().
-type tag()     :: {info(), info()} | {info(), info(), service()}.
-type log()     :: {time_us(), info()} | {time_us(), info(), service()}.

-record(span, {
          timestamp       :: time_us() | undefined,% timestamp of starting the span
          trace_id        :: trace_id() |undefined,% 64 bit integer trace id
          name            :: info() | undefined,   % name of the span
          id              :: span_id() | undefined,% 64 bit integer span id
          parent_id       :: span_id() | undefined,% 64 bit integer parent span id
          tags = []       :: [tag()],              % span tags
          logs = []       :: [log()],              % span logs
          duration        :: time_us() | undefined % microseconds between span start/end
         }).

-type span()    :: #span{}.

-define(is_span_active(Span), Span#span.timestamp =/= 0).

-endif.
