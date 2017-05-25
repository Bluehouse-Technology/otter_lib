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
%%% encoded with the binary protocol.
%%% @end
%%% -------------------------------------------------------------------

-module(otter_lib_thrift).
-export([
    encode/1,
    decode/1,
    encode_implicit_list/1,
    decode_implicit_list/1
]).

%% encode/decode basic thrift binary data
%% e.g. The transport (e.g. HTTP) data is an "implicit" list starting
%% with the element type, and number of elements ..

%%--------------------------------------------------------------------
%% @doc Decodes a binary input as implicit list to a thrift decoded data
%% structure, i.e. list of {Id, Type, Value} tuples. Implicit list is e.g.
%% how OpenZipkin expects the spans to be send. It is called implicit as it
%% does not contain a list type tag in the beginning.
%% @end
%%--------------------------------------------------------------------
-spec decode_implicit_list(BinaryData :: binary()) -> term().
decode_implicit_list(BinaryData) ->
    decode(list, BinaryData).

%%--------------------------------------------------------------------
%% @doc Encodes a list of thrift data structures to binary e.g. to be sent
%% on the wire.
%% @end
%%--------------------------------------------------------------------
-spec encode_implicit_list(term()) -> binary().
encode_implicit_list(Data) ->
    encode({list, Data}).

%%--------------------------------------------------------------------
%% @doc Encodes a {Id, Type, Data} tuple to binary.
%% @end
%%--------------------------------------------------------------------
-spec encode({Id :: integer(), Type :: atom(), Data :: term()}) -> binary().
encode({Id, Type, Data}) ->
    TypeId = map_type(Type),
    EData = encode({Type, Data}),
    <<TypeId, Id:16, EData/bytes>>;
%% .. and without Id (i.e. part of list/set/map)
encode({bool, true}) ->
    <<1>>;
encode({bool, false}) ->
    <<0>>;
encode({byte, Val}) ->
    <<Val>>;
encode({double, Val}) ->
    <<Val:64>>;
encode({i16, Val}) ->
    <<Val:16>>;
encode({i32, Val}) ->
    <<Val:32>>;
encode({i64, Val}) ->
    <<Val:64>>;
encode({string, Val}) when is_list(Val) ->
    Size = length(Val),
    % Might want to convert this to UTF-8 binary first, however for now
    % I'll leave it to the next encoding when binary can be provided in
    % UTF-8 format. In this part is kindly expected it to be ASCII
    % string
    Bytes = list_to_binary(Val),
    <<Size:32, Bytes/bytes>>;
encode({string, Val}) when is_binary(Val) ->
    Size = byte_size(Val),
    <<Size:32, Val/bytes>>;
encode({list, {ElementType, Data}}) ->
    ElementTypeId = map_type(ElementType),
    Size = length(Data),
    EData = list_to_binary([
        encode({ElementType, Element}) ||
        Element <- Data
    ]),
    <<ElementTypeId, Size:32, EData/bytes>>;
encode({set, Data}) ->
    encode({list, Data});
encode({struct, Data}) ->
    EData = list_to_binary([
        encode(StructElement) ||
        StructElement <- Data
    ]),
    <<EData/bytes, 0>>;
encode({map, {KeyType, ValType, Data}}) ->
    KeyTypeId = map_type(KeyType),
    ValTypeId = map_type(ValType),
    Size = length(Data),
    EData = list_to_binary([
        [encode({KeyType, Key}), encode({ValType, Val})] ||
        {Key, Val} <- Data
    ]),
    <<KeyTypeId, ValTypeId, Size:32, EData/bytes>>.

%% Decoding functions

%%--------------------------------------------------------------------
%% @doc Decodes a binary to {{Id, Type, Data}, Rest} tuple and rest of data.
%% @end
%%--------------------------------------------------------------------
-spec decode( BinaryData :: binary()) -> {{Id :: integer(), Type :: atom(), Data :: term()}, Rest :: binary()}.
decode(<<TypeId, Id:16, Data/bytes>>) ->
    Type = map_type(TypeId),
    {Val, Rest} = decode(Type, Data),
    {{Id, Type, Val}, Rest}.

decode(bool, <<Val, Rest/bytes>>) ->
    {Val == 1, Rest};
decode(byte, <<Val, Rest/bytes>>) ->
    {Val, Rest};
decode(double, <<Val:64, Rest/bytes>>) ->
    {Val, Rest};
decode(i16, <<Val:16, Rest/bytes>>) ->
    {Val, Rest};
decode(i32, <<Val:32, Rest/bytes>>) ->
    {Val, Rest};
decode(i64, <<Val:64, Rest/bytes>>) ->
    {Val, Rest};
decode(string, <<ByteLen:32, BytesAndRest/bytes>>) ->
    <<Bytes:ByteLen/bytes, Rest/bytes>> = BytesAndRest,
    {Bytes, Rest};
decode(struct, Data) ->
    decode_struct(Data, []);
decode(map, <<KeyTypeId, ValTypeId, Size:32, KVPsAndRest/bytes>>) ->
    decode_map(
        map_type(KeyTypeId),
        map_type(ValTypeId),
        Size,
        KVPsAndRest,
        []
    );
%% Lists and Sets are encoded the same way
decode(set, Data) ->
    decode(list, Data);
decode(list, <<ElementTypeId, Size:32, ElementsAndRest/bytes>>) ->
    decode_list(
        map_type(ElementTypeId),
        Size,
        ElementsAndRest,
        []
    ).

%% Helpers

decode_struct(Data, Acc) ->
    case decode(Data) of
        {Val, <<0, Rest/bytes>>} ->
            {lists:reverse([Val | Acc]), Rest};
        {Val, Rest} ->
            decode_struct(Rest, [Val | Acc])
    end.

decode_map(KeyType, ValType, 0, Rest, Acc) ->
    {{{KeyType, ValType}, lists:reverse(Acc)}, Rest};
decode_map(KeyType, ValType, Size, KVPsAndRest, Acc) ->
    {Key, ValAndRest} = decode(KeyType, KVPsAndRest),
    {Val, Rest} =  decode(ValType, ValAndRest),
    decode_map(KeyType, ValType, Size-1, Rest, [{Key, Val} | Acc]).

decode_list(ElementType, 0, Rest, Acc) ->
    {{ElementType, lists:reverse(Acc)}, Rest};
decode_list(ElementType, Size, Elements, Acc) ->
    {Data, Rest} = decode(ElementType, Elements),
    decode_list(ElementType, Size-1, Rest, [Data | Acc]).

map_type(2)     -> bool;
map_type(3)     -> byte;
map_type(4)     -> double;
map_type(6)     -> i16;
map_type(8)     -> i32;
map_type(10)    -> i64;
map_type(11)    -> string;
map_type(12)    -> struct;
map_type(13)    -> map;
map_type(14)    -> set;
map_type(15)    -> list;
map_type(bool)  -> 2;
map_type(byte)  -> 3;
map_type(double)-> 4;
map_type(i16)   -> 6;
map_type(i32)   -> 8;
map_type(i64)   -> 10;
map_type(string)-> 11;
map_type(struct)-> 12;
map_type(map)   -> 13;
map_type(set)   -> 14;
map_type(list)  -> 15.
