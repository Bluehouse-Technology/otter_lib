%%%-------------------------------------------------------------------
%%% Licensed to the Apache Software Foundation (ASF) under one
%%% or more contributor license agreements.  See the NOTICE file
%%% distributed with this work for additional information
%%% regarding copyright ownership.  The ASF licenses this file
%%% to you under the Apache License, Version 2.0 (the
%%% "License"); you may not use this file except in compliance
%%% with the License.  You may obtain a copy of the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing,
%%% software distributed under the License is distributed on an
%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%% KIND, either express or implied.  See the License for the
%%% specific language governing permissions and limitations
%%% under the License.
%%%
%%% @doc This module is the wrapper around the span data structure and
%%% exposes functions for span conversion to/from proplist e.g. for external
%%% applications that don't want to have the dependency of the record
%%% structure.
%%% @end
%%% -------------------------------------------------------------------

-module(otter_lib_span).
-export([
         start/1, start/2, start/3,
         start_with_tags/2, start_with_tags/3, start_with_tags/4,
         finish/1,
         log/2, log/3,
         tag/3, tag/4,
         get_id/1,
         get_ids/1,
         get_tags/1,
         get_logs/1,
         get_timestamp/1,
         get_duration/1,
         get_name/1,
         get_trace_id/1,
         get_parent_id/1,
         %% These functions are for external APIs (e.g. filter callback)
         %% if/when we decide to give up on backward compatibility in
         %% favor of maps, that'll be included here too
         to_proplist/1,
         from_proplist/1
        ]).

-include("otter.hrl").
%%--------------------------------------------------------------------
%% @doc Starts a span with the specified name. Automatically generates
%% a trace id.
%% @end
%%--------------------------------------------------------------------
-spec start(Name :: info()) -> span().
start(Name) ->
    start_with_tags(Name, []).


%%--------------------------------------------------------------------
%% @doc Starts a span with the specified name and trace id
%% @end
%% --------------------------------------------------------------------
-spec start(Name :: info(), TraceId :: trace_id()) -> span().
start(Name, TraceId) ->
    start_with_tags(Name, [], TraceId).

%%--------------------------------------------------------------------
%% @doc Starts a child span with the specified name, trace id and
%% parent id
%% @end
%% --------------------------------------------------------------------
-spec start(Name :: info(), TraceId :: trace_id(), ParentId :: span_id()) -> span().
start(Name, TraceId, ParentId) ->
    start_with_tags(Name, [], TraceId, ParentId).

%%--------------------------------------------------------------------
%% @doc Starts a span with the specified name and initial tags.
%% @end
%%--------------------------------------------------------------------
-spec start_with_tags(Name :: info(), InitialTags :: [tag()]) -> span().
start_with_tags(Name, InitialTags) ->
    start_with_tags(Name, InitialTags, otter_lib:id()).

%%--------------------------------------------------------------------
%% @doc Starts a span with the specified name, initial tags and trace id
%% @end
%% --------------------------------------------------------------------
-spec start_with_tags(Name :: info(), InitialTags :: [tag()], TraceId :: trace_id()) -> span().
start_with_tags(Name, InitialTags, TraceId) ->
    start_with_tags(Name, InitialTags, TraceId, undefined).

%%--------------------------------------------------------------------
%% @doc Starts a child span with the specified name, trace id and
%% parent id
%% @end
%% --------------------------------------------------------------------
-spec start_with_tags(Name :: info(), InitialTags :: [tag()], TraceId :: trace_id(), ParentId :: span_id() | undefined) -> span().
start_with_tags(Name, InitialTags, TraceId, ParentId) ->
    #span{
        timestamp = otter_lib:timestamp(),
        trace_id = TraceId,
        id = otter_lib:id(),
        parent_id = ParentId,
        name = Name,
        tags = InitialTags
    }.

%%--------------------------------------------------------------------
%% @doc Adds a tag to a span. If the tag already exists, its value
%% will be overwritten.
%% @end
%% --------------------------------------------------------------------
-spec tag(Span :: span(), Key :: info(), Value :: info()) -> span().
tag(Span, Key, Value) ->
    Tags = Span#span.tags,
    Span#span{
        tags = lists:keystore(Key, 1, Tags, {Key, Value})
    }.

%%--------------------------------------------------------------------
%% @doc Adds a tag to a span with a given service. If the tag already
%% exists, its value will be overwritten.
%% @end
%% --------------------------------------------------------------------
-spec tag(Span :: span(), Key :: info(), Value :: info(), Service :: service()) -> span().
tag(Span, Key, Value, Service) ->
    Tags = Span#span.tags,
    Span#span{
        tags = lists:keystore(Key, 1, Tags, {Key, Value, Service})
    }.

%%--------------------------------------------------------------------
%% @doc Adds a log message to a span.
%% @end
%% --------------------------------------------------------------------
-spec log(Span :: span(), Text :: info()) -> span().
log(Span, Text) ->
    Logs = Span#span.logs,
    Span#span{
        logs = [{otter_lib:timestamp(), Text} | Logs]
    }.

%%--------------------------------------------------------------------
%% @doc Adds a log message to a span with the specified service information.
%% @end
%% --------------------------------------------------------------------
-spec log(Span :: span(), Text :: info(), Service :: service()) -> span().
log(Span, Text, Service) ->
    Logs = Span#span.logs,
    Span#span{
        logs = [{otter_lib:timestamp(), Text, Service} | Logs]
    }.

%%--------------------------------------------------------------------
%% @doc Finish a span : add duration (current time - timestamp)
%% @end
%% --------------------------------------------------------------------
-spec finish(Span :: span()) -> span().
finish(#span{timestamp = Timestamp, logs = Logs} = Span) ->
    Span#span{
        duration = otter_lib:timestamp() - Timestamp,
        logs = lists:reverse(Logs)
    }.


%%--------------------------------------------------------------------
%% @doc Get ids (trace id and span id) of a span
%% @end
%% --------------------------------------------------------------------
-spec get_ids(Span :: span()) -> {trace_id(), span_id()}.
get_ids(#span{trace_id = TraceId, id = Id}) -> {TraceId, Id}.

%%--------------------------------------------------------------------
%% @doc Get the id of a span
%% @end
%% --------------------------------------------------------------------
-spec get_id(Span :: span()) -> span_id().
get_id(#span{id = Id}) -> Id.

%%--------------------------------------------------------------------
%% @doc Get the trace id of a span
%% @end
%% --------------------------------------------------------------------
-spec get_trace_id(Span :: span()) -> span_id().
get_trace_id(#span{trace_id = TraceId}) -> TraceId.

%%--------------------------------------------------------------------
%% @doc Get the parent id of a span
%% @end
%% --------------------------------------------------------------------
-spec get_parent_id(Span :: span()) -> span_id()|undefined.
get_parent_id(#span{parent_id = ParentId}) -> ParentId.

%%--------------------------------------------------------------------
%% @doc Get the list of tags (key-value pairs) of a span
%% @end
%% --------------------------------------------------------------------
-spec get_tags(Span :: span()) -> [tag()].
get_tags(#span{tags = Tags}) -> Tags.

%%--------------------------------------------------------------------
%% @doc Get the list of logs (timestamp-text pairs) of a span
%% @end
%% --------------------------------------------------------------------
-spec get_logs(Span :: span()) -> [log()].
get_logs(#span{logs = Logs}) -> Logs.

%%--------------------------------------------------------------------
%% @doc Get the start timestamp of a span
%% @end
%% --------------------------------------------------------------------
-spec get_timestamp(Span :: span()) -> time_us().
get_timestamp(#span{timestamp = Timestamp}) -> Timestamp.


%%--------------------------------------------------------------------
%% @doc Get the duration of a span. Returns integer ter the span is finished
%% otherwise undefined
%% @end
%% --------------------------------------------------------------------
-spec get_duration(Span :: span()) -> time_us()|undefined.
get_duration(#span{duration = Duration}) -> Duration.

%%--------------------------------------------------------------------
%% @doc Get the name of a span
%% @end
%% --------------------------------------------------------------------
-spec get_name(Span :: span()) -> info().
get_name(#span{name = Name}) -> Name.

%%--------------------------------------------------------------------
%% @doc Convert span record to a proplist
%% @end
%% --------------------------------------------------------------------
-spec to_proplist(Span :: span()) -> list().
to_proplist(
    #span{
        timestamp = Timestamp,
        duration = Duration,
        trace_id = TraceId,
        id = Id,
        parent_id = ParentId,
        name = Name,
        tags = Tags,
        logs = Logs
    }) ->
    [
        {timestamp, Timestamp},
        {duration, Duration},
        {id, Id},
        {trace_id, TraceId},
        {parent_id, ParentId},
        {name, Name},
        {tags, Tags},
        {logs, Logs}
    ].

%%--------------------------------------------------------------------
%% @doc Convert a proplist to span record. The proplist is expected to
%% have keys : timestamp, duration, id, trace_id, parent_id, name, tags,
%% logs with the correct values as described in the span record in
%% otter.hrl
%% @end
%% --------------------------------------------------------------------
-spec from_proplist(Proplist :: list()) -> span().
from_proplist(Proplist) ->
    from_proplist(Proplist, #span{}).
from_proplist([{timestamp, Timestamp} | Rest], SpanAcc) ->
    from_proplist(Rest, SpanAcc#span{timestamp = Timestamp});
from_proplist([{duration, Duration} | Rest], SpanAcc) ->
    from_proplist(Rest, SpanAcc#span{duration = Duration});
from_proplist([{id, Id} | Rest], SpanAcc) ->
    from_proplist(Rest, SpanAcc#span{id = Id});
from_proplist([{trace_id, TraceId} | Rest], SpanAcc) ->
    from_proplist(Rest, SpanAcc#span{trace_id = TraceId});
from_proplist([{parent_id, ParentId} | Rest], SpanAcc) ->
    from_proplist(Rest, SpanAcc#span{parent_id = ParentId});
from_proplist([{name, Name} | Rest], SpanAcc) ->
    from_proplist(Rest, SpanAcc#span{name = Name});
from_proplist([{tags, Tags} | Rest], SpanAcc) ->
    from_proplist(Rest, SpanAcc#span{tags = Tags});
from_proplist([{logs, Logs} | Rest], SpanAcc) ->
    from_proplist(Rest, SpanAcc#span{logs = Logs});
from_proplist([_ | Rest], SpanAcc) ->
    from_proplist(Rest, SpanAcc);
from_proplist([], SpanAcc) ->
    SpanAcc.

