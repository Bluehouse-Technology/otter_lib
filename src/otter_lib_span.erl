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
%%% @doc This module is the wrapper around the span data structure
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

start(Name) ->
    start_with_tags(Name, []).
start(Name, TraceId) ->
    start_with_tags(Name, [], TraceId).
start(Name, TraceId, ParentId) ->
    start_with_tags(Name, [], TraceId, ParentId).

start_with_tags(Name, InitialTags) ->
    start_with_tags(Name, InitialTags, otter_lib:id()).
start_with_tags(Name, InitialTags, TraceId) ->
    start_with_tags(Name, InitialTags, TraceId, undefined).
start_with_tags(Name, InitialTags, TraceId, ParentId) ->
    #span{
        timestamp = otter_lib:timestamp(),
        trace_id = TraceId,
        id = otter_lib:id(),
        parent_id = ParentId,
        name = Name,
        tags = InitialTags
    }.

tag(Span, Key, Value) ->
    Tags = Span#span.tags,
    Span#span{
        tags = lists:keystore(Key, 1, Tags, {Key, Value})
    }.

tag(Span, Key, Value, Service) ->
    Tags = Span#span.tags,
    Span#span{
        tags = lists:keystore(Key, 1, Tags, {Key, Value, Service})
    }.

log(Span, Text) ->
    Logs = Span#span.logs,
    Span#span{
        logs = [{otter_lib:timestamp(), Text} | Logs]
    }.

log(Span, Text, Service) ->
    Logs = Span#span.logs,
    Span#span{
        logs = [{otter_lib:timestamp(), Text, Service} | Logs]
    }.

finish(#span{timestamp = Timestamp, logs = Logs} = Span) ->
    Span#span{
        duration = otter_lib:timestamp() - Timestamp,
        logs = lists:reverse(Logs)
    }.

get_ids(#span{trace_id = TraceId, id = Id}) -> {TraceId, Id}.
get_id(#span{id = Id}) -> Id.
get_trace_id(#span{trace_id = TraceId}) -> TraceId.
get_parent_id(#span{parent_id = ParentId}) -> ParentId.
get_tags(#span{tags = Tags}) -> Tags.
get_logs(#span{logs = Logs}) -> Logs.
get_timestamp(#span{timestamp = Timestamp}) -> Timestamp.
get_duration(#span{duration = Duration}) -> Duration.
get_name(#span{name = Name}) -> Name.


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

