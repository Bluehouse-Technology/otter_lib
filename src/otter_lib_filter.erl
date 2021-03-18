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
%%% Run a list of key-value pairs through a list of conditions and return
%%% the specified actions for the matching ones.
%%%
%%% Filter rules are composed by a list of `{Conditions, Actions}' tuples.
%%% Processing a span means iterating through this list and when an item
%%% found where all `Conditions' evaluate to true then the `Actions' in that
%%% item are collected and at the end of the evaluation returned to the
%%% caller for execution. i.e. the filter library can be applied in different
%%% environments.
%%%
%%% The evaluation of the rules can run in 2 modes. The `break' mode means
%%% that it stops at the first matching item and returns the `Actions'
%%% specified there. The `continue' mode runs through all `{Conditions, Actions}'
%%% items and collects all matching `Actions'.
%%%
%%% The library implements the following conditions :
%%%
%%% Check the presence of a Key
%%%
%%% `{present, Key}'
%%%
%%%%
%%% Check whether 2 Keys have the same value
%%%
%%% `{same, Key1, Key2}'
%%%
%%%
%%% The value of a Key/Value pair can be compared to a value
%%%
%%% `{value, Key, ValueToCompare}'
%%%
%%% example: check the name of the span
%%%
%%% `{value, otter_span_name, "radius request"}'
%%%
%%%
%%% Checking integer values
%%% Key/Value pairs with integer values can be checked with the following
%%% conditions.
%%%
%%% `{greater, Key, Integer}'
%%% `{less, Key, Integer}'
%%% `{between, Key, Integer1, Integer2}'
%%%
%%% example: check whether the span duration is greater than 5 seconds
%%%
%%% `{greater, otter_span_duration, 5000000}'
%%%
%%%
%%% Negate condition check
%%%
%%% `{negate, Condition}'
%%%
%%% example: Check if the value of the "final_result" tag is other than "ok"
%%%
%%% `{negate, {value, "final_result", "ok"}}'
%%%
%%% One out of
%%%
%%% This condition uses a random generated number and in the range of `0 < X =< Integer',
%%% and if the generated value is 1 it returns true.
%%%
%%% `{one_out_of, Integer}'
%%%
%%% example: Match 1 out of 1000 requests
%%% `{one_out_of, 1000}'
%%% @end
%%%-------------------------------------------------------------------

-module(otter_lib_filter).
-export([run/2, run/3]).


-type condition() :: tuple().
-type action()    :: tuple()|atom().
-type rules()     :: [{[condition()], [action()]}].
-type tags()      :: [{term(), term()}].

%%---------------------------------------------------------------------
%% @doc Run the key-value pair data (tags) through the rules with the default
%% `continue' mode.
%% @end
%%---------------------------------------------------------------------
-spec run(Tags :: tags(), Rules :: rules()) -> [action()].
run(Tags, Rules) ->
    run(Tags, Rules, continue).


%%---------------------------------------------------------------------
%% @doc Run the key-value pair data (tags) through the rules with the specified
%% mode.
%% @end
%%---------------------------------------------------------------------
-spec run(Tags :: tags(), Rules :: rules(), Mode :: break|continue) -> [action()].
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
    case rand:uniform(Nr) - 1 of
        1 -> true;
        _ -> false
    end;
check(_, _) ->
    false.
