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
%%%
%%% @doc
%%% The main idea behind this filter that the processing of the spans can
%%% be modified runtime by changing the filter configuration. This way
%%% logging, counting or sending data to trace collectors can be modified
%%% on the running system based on the changing operational requirements.
%%%
%%% The span filter works with key value pair lists. This was the
%%% easiest to implement and reasonably fast.
%%%
%%% Filter rules are composed by a list of `{Conditions, Actions}' tuples.
%%% Processing a span means iterating through this list and when an item
%%% found where all `Conditions' evaluate to true then the `Actions' in that
%%% item are executed.
%%%
%%% `Conditions' operate on a copy of the tags of the span. It is a
%%% sequence of checks against the tags (e.g. key present, key value)
%%% where if any check in the sequence fails, the associated actions are
%%% not executed and the next `{Conditions, Actions}' item is evaluated.
%%%
%%% `Evaluation' of the rules happens in the process which invokes the span
%%% end statement (e.g. @link otter:finish/1.) i.e. it has impact on the
%%% request processing time. Therefore the actions that consume little
%%% time and resources with no external interfaces (e.g. counting in ets)
%%% can be done during the evaluation of the rules, but anything that has
%%% external interface or dependent on environment (e.g. logging and trace
%%% collecting) should be done asynchronously.
%%% @end
%%%-------------------------------------------------------------------

-module(otter_lib_filter).
-export([run/2, run/3]).

%%--------------------------------------------------------------------
%% @doc Takes a span and rules as input. The conditions of the rules are
%% evaluated and all matching actions are collected in a list which is
%% returned to the caller for execution.
%% @end
%% --------------------------------------------------------------------

-type condition() :: tuple().
-type action()    :: tuple()|atom().
-type rules()     :: [{[condition()], [action()]}].
-type tags()      :: [{term(), term()}].

-spec run(tags(), rules()) -> [action()].
run(Tags, Rules) ->
    run(Tags, Rules, continue).

-spec run(tags(), rules(), break|continue) -> [action()].
run(Tags, Rules, BreakOrContinue) ->
    rules(Rules, Tags, BreakOrContinue, []).

rules([{Conditions, Actions} | Rest], Tags, BreakOrContinue, CollectedActions) ->
    case check_conditions(Conditions, Tags) of
        false ->
            rules(Rest, Tags, BreakOrContinue, CollectedActions);
        true when BreakOrContinue == break ->
            Actions;
        true when BreakOrContinue == continue ->
            rules(Rest, Tags, BreakOrContinue, Actions ++ CollectedActions)
    end;
rules([], _Tags, _BreakOrContinue, CollectedActions) ->
    CollectedActions.

check_conditions([Condition | Rest], Tags) ->
    case check(Condition, Tags) of
        true ->
            check_conditions(Rest, Tags);
        false ->
            false
    end;
check_conditions([], _) ->
    true.

check({negate, Condition}, Tags) ->
    not check(Condition, Tags);
check({value, Key, Value}, Tags) ->
    case lists:keyfind(Key, 1, Tags) of
        {Key, Value} ->
            true;
        _ ->
            false
    end;
check({same, Key1, Key2}, Tags) ->
    case lists:keyfind(Key1, 1, Tags) of
        {Key1, Value} ->
            case lists:keyfind(Key2, 1, Tags) of
                {Key2, Value} ->
                    true;
                _ ->
                    false
            end;
        _ ->
            false
    end;
check({greater, Key, Value}, Tags) ->
    case lists:keyfind(Key, 1, Tags) of
        {Key, Value1} when Value1 > Value  ->
            true;
        _ ->
            false
    end;
check({less, Key, Value}, Tags) ->
    case lists:keyfind(Key, 1, Tags) of
        {Key, Value1} when Value1 < Value  ->
            true;
        _ ->
            false
    end;
check({between, Key, Value1, Value2}, Tags) ->
    case lists:keyfind(Key, 1, Tags) of
        {Key, Value} when Value > Value1 andalso Value < Value2  ->
            true;
        _ ->
            false
    end;
check({present, Key}, Tags) ->
    lists:keymember(Key, 1, Tags);
check({one_out_of, Nr}, _Tags) ->
    %% The rand:uniform/1 call is faster but it needs a process to
    %% keep state. Calling that from separate processes once will likely
    %% generate the same number for each call. crypto:rand_uniform/2 is
    %% convenient from this aspect.
    case crypto:rand_uniform(0, Nr) of
        1 -> true;
        _ -> false
    end;
check(_, _) ->
    false.
