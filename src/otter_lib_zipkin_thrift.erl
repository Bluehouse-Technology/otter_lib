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
%%% @doc This module facilitates encoding/decoding of thrift data
%%% lists encoded with the binary protocol, suitable for
%%% sending/receiving spans on the zipkin interface.
%%% @end
%%% -------------------------------------------------------------------

-module(otter_lib_zipkin_thrift).
-export([
    encode_spans/3,
    decode_spans/1
]).

-include("otter.hrl").

%%--------------------------------------------------------------------
%% @doc Encodes a list of spans to a thrift (binary encoded) binary.
%% ExtraTags is a list of key-value pairs that are added for each span in
%% the list (e.g. the "lc" tag which tags the span with service information)
%% in OpenZipkin. ServiceDefaults is used to complete the service information
%% when only partial information is provided in span information e.g. default
%% or only service name.
%% @end
%%--------------------------------------------------------------------
-spec encode_spans(Spans :: [span()], ExtraTags :: list(), ServiceDefaults :: term()) -> binary().
encode_spans(Spans, ExtraTags, ServiceDefaults) ->
    otter_lib_thrift:encode_implicit_list(
        {struct, [span_to_struct(S, ExtraTags, ServiceDefaults) || S <- Spans]}
    ).

%%--------------------------------------------------------------------
%% @doc Decodes a binary to a list of spans.
%% @end
%%--------------------------------------------------------------------
-spec decode_spans(BinaryData :: binary()) -> [span()].
decode_spans(Data) ->
    {
        {struct, StructList},
        _Rest
    } = otter_lib_thrift:decode_implicit_list(Data),
    [struct_to_span(S) || S <- StructList].

span_to_struct(#span{
    id = Id,
    trace_id = TraceId,
    name = Name,
    parent_id = ParentId,
    logs = Logs,
    tags = Tags,
    timestamp = Timestamp,
    duration = Duration
}, ExtraTags, ServiceDefaults) ->
    TraceIdHigh = TraceId bsr 64,
    TraceIdLow = TraceId rem (1 bsl 64),
    [
        {1, i64, TraceIdLow},
        {3, string, otter_lib:to_bin(Name)},
        {4, i64, Id}
    ] ++
    case ParentId of
        undefined ->
            [];
        ParentId ->
            [{5, i64, ParentId}]
    end ++
    [
        {6, list, {
            struct,
            [log_to_annotation(Log, ServiceDefaults) || Log <- Logs]
        }},
        {8, list, {
            struct,
            [tag_to_binary_annotation(Tag, ServiceDefaults) || Tag <- ExtraTags ++ Tags]
        }},
        {10, i64, Timestamp},
        {11, i64, Duration}
    ] ++
    case TraceIdHigh of
        0 ->
            [];
        TraceIdHigh ->
            [{12, i64, TraceIdHigh}]
    end.

log_to_annotation({Timestamp, Text}, _ServiceDefaults) ->
    [
        {1, i64, Timestamp},
        {2, string, otter_lib:to_bin(Text)}
    ];
log_to_annotation({Timestamp, Text, Service}, ServiceDefaults) ->
    [
        {1, i64, Timestamp},
        {2, string, otter_lib:to_bin(Text)},
        {3,struct, host_to_struct(Service, ServiceDefaults)}
    ].

tag_to_binary_annotation({Key, Value}, _ServiceDefaults) ->
    [
        {1, string, otter_lib:to_bin(Key)},
        {2, string, otter_lib:to_bin(Value)},
        {3,i32,6}
    ];
tag_to_binary_annotation({Key, Value, Service}, ServiceDefaults) ->
    [
        {1, string, otter_lib:to_bin(Key)},
        {2, string, otter_lib:to_bin(Value)},
        {3,i32,6},
        {4,struct, host_to_struct(Service, ServiceDefaults)}
    ].

host_to_struct(default, {Service,Ip, Port}) ->
    host_to_struct({Service, Ip, Port});
host_to_struct(Service, {_, Ip, Port}) ->
    host_to_struct({Service, Ip, Port}).

host_to_struct({Service, Ip, Port}) ->
    [
        {1,i32, otter_lib:ip_to_i32(Ip)},
        {2,i16, Port},
        {3,string, otter_lib:to_bin(Service)}
    ].

struct_to_span(StructData) ->
    struct_to_span(StructData, #span{}).

struct_to_span([{1, i64, TraceId}| Rest], Span) ->
    struct_to_span(Rest, Span#span{trace_id = TraceId});
struct_to_span([{3, string, Name}| Rest], Span) ->
    struct_to_span(Rest, Span#span{name = Name});
struct_to_span([{4, i64, Id}| Rest], Span) ->
    struct_to_span(Rest, Span#span{id = Id});
struct_to_span([{5, i64, ParentId}| Rest], Span) ->
    struct_to_span(Rest, Span#span{parent_id = ParentId});
struct_to_span([{6, list, {struct, Annotations}}| Rest], Span) ->
    Logs = [annotation_to_log(Annotation) || Annotation <- Annotations],
    struct_to_span(Rest, Span#span{logs = Logs});
struct_to_span([{8, list, {struct, BinAnnotations}}| Rest], Span) ->
    Tags = [bin_annotation_to_tag(BinAnnotation) || BinAnnotation <- BinAnnotations],
    struct_to_span(Rest, Span#span{tags = Tags});
struct_to_span([{10, i64, Timestamp}| Rest], Span) ->
    struct_to_span(Rest, Span#span{timestamp = Timestamp});
struct_to_span([{11, i64, Duration}| Rest], Span) ->
    struct_to_span(Rest, Span#span{duration = Duration});
struct_to_span([_ | Rest], Span) ->
    struct_to_span(Rest, Span);
struct_to_span([], Span) ->
    Span.

annotation_to_log(StructData) ->
    annotation_to_log(StructData, {undefined,undefined,undefined}).

annotation_to_log([{1, i64, Timestamp} | Rest], {_, Text, Host}) ->
    annotation_to_log(Rest, {Timestamp, Text, Host});
annotation_to_log([{2, string, Text} | Rest], {Timestamp, _, Host}) ->
    annotation_to_log(Rest, {Timestamp, Text, Host});
annotation_to_log([{3, struct, HostStruct} | Rest], {Timestamp, Text, _}) ->
    annotation_to_log(Rest, {Timestamp, Text, struct_to_host(HostStruct)});
annotation_to_log([_ | Rest], Log) ->
    annotation_to_log(Rest, Log);
annotation_to_log([], {Timestamp, Text, undefined}) ->
    {Timestamp, Text};
annotation_to_log([], Log) ->
    Log.

bin_annotation_to_tag(StructData) ->
    bin_annotation_to_tag(StructData, {undefined,undefined,undefined}).

bin_annotation_to_tag([{1, string, Key} | Rest], {_, Value, Host}) ->
    bin_annotation_to_tag(Rest, {Key, Value, Host});
bin_annotation_to_tag([{2, string, Value} | Rest], {Key, _, Host}) ->
    bin_annotation_to_tag(Rest, {Key, Value, Host});
bin_annotation_to_tag([{4, struct, HostStruct} | Rest], {Key, Value, _}) ->
    bin_annotation_to_tag(Rest, {Key, Value, struct_to_host(HostStruct)});
bin_annotation_to_tag([_ | Rest], Tag) ->
    bin_annotation_to_tag(Rest, Tag);
bin_annotation_to_tag([], {Key, Value, undefined}) ->
    {Key, Value};
bin_annotation_to_tag([], Tag) ->
    Tag.

struct_to_host(StructData) ->
    struct_to_host(StructData, {undefined, undefined, undefined}).

struct_to_host([{1, i32, IntIp} | Rest], {Service, _, Port}) ->
    <<Ip1, Ip2, Ip3, Ip4>> = <<IntIp:32>>,
    struct_to_host(Rest, {Service, {Ip1, Ip2, Ip3, Ip4}, Port});
struct_to_host([{2, i16, Port} | Rest], {Service, Ip, _}) ->
    struct_to_host(Rest, {Service, Ip, Port});
struct_to_host([{3, string, Service} | Rest], {_, Ip, Port}) ->
    struct_to_host(Rest, {Service, Ip, Port});
struct_to_host([_ | Rest], Host) ->
    struct_to_host(Rest, Host);
struct_to_host([], Host) ->
    Host.
