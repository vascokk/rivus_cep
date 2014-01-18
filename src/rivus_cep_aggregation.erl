%%------------------------------------------------------------------------------
%% Copyright (c) 2013 Vasil Kolarov
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%------------------------------------------------------------------------------

-module(rivus_cep_aggregation).

-export([eval_resultset/3, get_group_key/1, new_state/0]).

-include_lib("eunit/include/eunit.hrl").
-include("rivus_cep.hrl").

eval_resultset(Stmt, [H|T], #res_eval_state{recno = RecNo, result = CurrentRes} = State) ->
    Key = get_group_key(H),
    {ResultRecord, NewState} = eval_result_record(Stmt, H, Key, State#res_eval_state{aggrno = 1}),
    NewRes = dict:store(Key, ResultRecord, CurrentRes),
    eval_resultset(Stmt, T, NewState#res_eval_state{recno=RecNo+1, result = NewRes});
eval_resultset(_, [], State) ->
    [Value || {_, Value} <- dict:to_list(State#res_eval_state.result)].

eval_result_record(Stmt, ResultRecord, Key, _State) ->
    Nodes = tuple_to_list(ResultRecord),
    %%?debugMsg(io_lib:format("Nodes: ~p~n",[Nodes])),
    {ResNodes, NewState} = lists:mapfoldl(fun(Node, State) -> eval_node(Stmt, Node, Key, State) end, _State, Nodes),
    {list_to_tuple(ResNodes), NewState}.

	    
eval_node(Stmt, Node, Key, State) ->
    case Node of
	Value when not is_tuple(Value) andalso not is_list(Value) -> {Value, State};
	{Value} when not is_tuple(Value) andalso not is_list(Value) -> {Value, State};
	{param, Value} -> {Value, State};
	{Op, LeftNode, RightNode} -> {LeftValue, NewState1} = eval_node(Stmt, LeftNode, Key, State),
				     {RightValue, NewState2} =  eval_node(Stmt, RightNode, Key, NewState1),
				     {eval_operation(Op, LeftValue, RightValue), NewState2};
	{Aggr, Param} -> {Value, NewState} = eval_node(Stmt, Param, Key, State),
			 eval_aggregation(Aggr, Stmt, Value, Key, NewState)
    end.

eval_aggregation(sum, Stmt, Value, Key, #res_eval_state{aggrno = AggrNo, aggr_nodes = AggrNodes} = State) ->
    NewAggrNodes = orddict:update_counter({Stmt, Key, AggrNo}, Value,  AggrNodes),
    {orddict:fetch({Stmt, Key, AggrNo}, NewAggrNodes),  State#res_eval_state{aggrno = AggrNo+1, aggr_nodes = NewAggrNodes}};
eval_aggregation(count, Stmt, _, Key, #res_eval_state{aggrno = AggrNo, aggr_nodes = Aggregations} = State) ->
    NewAggr = orddict:update_counter({Stmt, Key, AggrNo}, 1,  Aggregations),
    {orddict:fetch({Stmt, Key, AggrNo}, NewAggr),  State#res_eval_state{aggrno = AggrNo+1, aggr_nodes = NewAggr}};
eval_aggregation(min, Stmt, Value, Key, #res_eval_state{aggrno = AggrNo, aggr_nodes = AggrNodes} = State) ->
    NewAggrNodes = orddict:update({Stmt, Key, AggrNo}, fun(Old) -> case Old < Value of
								       true -> Old;
								       false -> Value
								   end
						       end, Value,  AggrNodes),
    {orddict:fetch({Stmt, Key, AggrNo}, NewAggrNodes),  State#res_eval_state{aggrno = AggrNo+1, aggr_nodes = NewAggrNodes}};
eval_aggregation(max, Stmt, Value, Key, #res_eval_state{aggrno = AggrNo, aggr_nodes = AggrNodes} = State) ->
    NewAggrNodes = orddict:update({Stmt, Key, AggrNo}, fun(Old) -> case Old > Value of
								       true -> Old;
								       false -> Value
								   end
						       end, Value,  AggrNodes),
    {orddict:fetch({Stmt, Key, AggrNo}, NewAggrNodes),  State#res_eval_state{aggrno = AggrNo+1, aggr_nodes = NewAggrNodes}}.

eval_operation(plus, Left, Right) ->	    
    Left + Right;
eval_operation(minus, Left, Right) ->	    
    Left - Right;
eval_operation(mult, Left, Right) ->	    
    Left * Right;
eval_operation('div', Left, Right) ->	    
    Left / Right.

get_group_key(T) when is_tuple(T)->
    L = tuple_to_list(T),
    Keys = lists:foldl(fun(Value, Acc) when not is_tuple(Value) andalso not is_list(Value) -> Acc ++ [Value];
			  ({Value}, Acc) when not is_tuple(Value) andalso not is_list(Value) -> Acc ++ [Value];
			  ({param, Value}, Acc) -> Acc ++ [Value];
			  ({Op, Left, Right}, Acc) -> Acc ++ eval_key(Op,Left,Right);
			  (_, Acc) -> Acc
		       end, [], L),
    list_to_tuple(Keys).

eval_key(Op, Left, Right) ->
    L = eval_key(Left),
    R = eval_key(Right),
    case L /= [] andalso R /= [] of 
	true -> case Op of
		    plus -> [L + R];
		    minus -> [L - R];
		    mult -> [L * R];
		    'div' -> [L/R]
		end;
	false ->[]
    end.

eval_key({param, Value}) ->
    Value;
eval_key({sum, _}) ->
    [];
eval_key({count, _}) ->
    [];
eval_key({min, _}) ->
    [];
eval_key({max, _}) ->
    [];
eval_key({avg, _}) ->
    [].

new_state() ->
    #res_eval_state{}.
