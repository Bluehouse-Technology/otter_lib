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
%%% @doc
%%% This module implements a simple way of giving operational visibility
%%% of events in the system. It expects a Key and Data where the
%%% Key is used to increment a counter in a table and also to store the
%%% last received Data for that Key. The Data is preferred to be a list
%%% of `{K,V}' tuples, if it is any other format, it uses `[{data, Data}]'
%%% to store the last information.
%%%
%%% The ETS tables holding the counters/snapshots must be initialized
%%% before the feature can be used. The application using this module should
%%% call `otter_lib_snapshot_count:sup_init()' from a persistent process.
%%% The simplest is to call from the application supervisor init. The call
%%% only creaes the ETS tables that need an owner process to stay around.
%%% @end
%%%-------------------------------------------------------------------

-module(otter_lib_snapshot_count).
-export([
         delete_all_counters/0,
         delete_counter/1,
         get_snap/1,
         list_counts/0,
         snapshot/2,
         sup_init/0
        ]).


%%--------------------------------------------------------------------
%% @doc Increase a counter for the given Key and store the Data. If the data
%% is a property (key-value) list then it adds the timestamp  in
%% `{Year, Month, Day, Hour, Min, Sec, Us}' format to the list with key
%% `snap_timestamp`. If it is not a property list then it stores the
%% data in a property list with key `data' and adds the timestamp.
%% It returns the current counter value for the key.
%% @end
%%--------------------------------------------------------------------
-spec snapshot(Key :: term(), Data :: term()) -> integer().
snapshot(Key, [{_, _} |_ ] = Data) ->
    {_, _, Us} = os:timestamp(),
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:local_time(),
    ets:insert(
        otter_snapshot_store,
        {
            Key,
            [
                {snap_timestamp, {Year, Month, Day, Hour, Min, Sec, Us}}
                | Data
            ]
        }
    ),
    case catch ets:update_counter(otter_snapshot_count, Key, 1) of
        {'EXIT', {badarg, _}} ->
            ets:insert(otter_snapshot_count, {Key, 1});
        Cnt ->
            Cnt
    end;
snapshot(Key, Data) ->
    snapshot(Key, [{data, Data}]).


%%--------------------------------------------------------------------
%% @doc List all the counters with their values
%% @end
%%--------------------------------------------------------------------
-spec list_counts() -> [{Key :: term(), Counter :: integer()}].
list_counts() ->
    ets:tab2list(otter_snapshot_count).

%%--------------------------------------------------------------------
%% @doc Return the last stored data (snapshot) for a key
%% @end
%%--------------------------------------------------------------------
-spec get_snap(Key :: term()) -> term().
get_snap(Key) ->
    ets:lookup(otter_snapshot_store, Key).

%%--------------------------------------------------------------------
%% @doc Delete the counter and data (snapshot) for a key
%% @end
%%--------------------------------------------------------------------
-spec delete_counter(Key :: term()) -> ok.
delete_counter(Key) ->
    ets:delete(otter_snapshot_store, Key),
    ets:delete(otter_snapshot_count, Key),
    ok.

%%--------------------------------------------------------------------
%% @doc Delete all counters and data (snapshot)
%% @end
%%--------------------------------------------------------------------
-spec delete_all_counters() -> ok.
delete_all_counters() ->
    ets:delete_all_objects(otter_snapshot_store),
    ets:delete_all_objects(otter_snapshot_count),
    ok.

%%--------------------------------------------------------------------
%% @doc Initialize the ETS tables to store counters/snapshots. Should be
%% called from a persistent process/
%% @end
%%--------------------------------------------------------------------
-spec sup_init() -> term().
sup_init() ->
    [
     ets:new(Tab, [named_table, public, {Concurrency, true}]) ||
        {Tab, Concurrency} <- [
                               {otter_snapshot_count, write_concurrency},
                               {otter_snapshot_store, write_concurrency}
                              ]
    ].
