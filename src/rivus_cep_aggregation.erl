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

-export([eval_resultset/4, get_group_key/2, new_state/0, eval_resultset_fast_aggr/1]).

-include_lib("eunit/include/eunit.hrl").
-include("rivus_cep.hrl").

eval_resultset_fast_aggr(QueryState) ->
    Window = QueryState#query_state.window,
    Pid = QueryState#query_state.window_pid,
    [[AggrState]] = rivus_cep_window:get_aggr_state(Pid, local, Window),
    [Value || {_, Value} <- dict:to_list(AggrState#res_eval_state.result)].

eval_resultset(_, [], AggrState, #query_state{query_plan =QP}) when not QP#query_plan.fast_aggregations  ->
    [Value || {_, Value} <- dict:to_list(AggrState#res_eval_state.result)];
eval_resultset(_, [], AggrState, #query_state{query_plan =QP} = QueryState) when QP#query_plan.fast_aggregations ->
    Window = QueryState#query_state.window,
    Pid = QueryState#query_state.window_pid,
    rivus_cep_window:trim(Pid, Window),
    rivus_cep_window:update(Pid, Window, AggrState),
    [Value || {_, Value} <- dict:to_list(AggrState#res_eval_state.result)];
eval_resultset(Stmt, [H|T], #res_eval_state{recno = RecNo, result = CurrentRes} = AggrState, QueryState) ->
    SelectClause = QueryState#query_state.query_ast#query_ast.select,
    Key = get_group_key(H, SelectClause),
    {ResultRecord, NewAggrState} = eval_result_record(Stmt, H, Key, SelectClause, AggrState#res_eval_state{aggrno = 1}),
    NewRes = dict:store(Key, ResultRecord, CurrentRes),
    %%?debugMsg(io_lib:format("----->NewRes: ~p~n",[NewRes])),
    eval_resultset(Stmt, T, NewAggrState#res_eval_state{recno=RecNo+1, result = NewRes}, QueryState).


eval_result_record(Stmt, ResultRecord, Key, SelectClause, State) when is_tuple(ResultRecord)->
    eval_result_record(Stmt, [ResultRecord], Key, SelectClause, State);
eval_result_record(Stmt, ResultRecord, Key, SelectClause, State) -> 
    {ResNodes, NewState} = lists:mapfoldl(fun({EventName, EventParam}, Acc) when is_atom(EventName) andalso is_atom(EventParam) ->
						  Event = lists:keyfind(EventName, 1, ResultRecord),
						  ParamValue = (EventName):get_param_by_name(Event, EventParam),
						  {ParamValue, Acc};
					     ({Op, Left, Right}, Acc) -> eval_node(Stmt, {Op,Left,Right}, Key, ResultRecord, Acc);					  	 (Node, Acc) when is_tuple(Node) -> eval_node(Stmt, Node, Key, ResultRecord, Acc);
					     (Node, Acc) -> {Node, Acc}
					  end, State, SelectClause),		     
   
    {list_to_tuple(ResNodes), NewState}.

	    
eval_node(Stmt, Node, Key, ResultRecord, State) ->
    case Node of
	{EventName, EventParam} when is_atom(EventName) andalso is_atom(EventParam) ->
	    Event = lists:keyfind(EventName, 1, ResultRecord),
	    {(EventName):get_param_by_name(Event, EventParam), State};
	Value when not is_tuple(Value) andalso not is_list(Value) -> {Value, State};
	{Value} when not is_tuple(Value) andalso not is_list(Value) -> {Value, State};
	{Op, LeftNode, RightNode} -> {LeftValue, NewState1} = eval_node(Stmt, LeftNode, Key, ResultRecord, State),
				     {RightValue, NewState2} =  eval_node(Stmt, RightNode, Key, ResultRecord, NewState1),
				     {eval_op({Op, LeftValue, RightValue}, ResultRecord), NewState2}; %%!!!!!
	{Aggr, Param} when is_tuple(Param)-> {Value, NewState} = eval_node(Stmt, Param, Key, ResultRecord, State),
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

get_group_key(Event, SelectClause) when is_tuple(Event)->
    Keys = lists:foldl(fun({EventName, EventParam}, Acc) when is_atom(EventName) andalso is_atom(EventParam) -> 		    
			       ParamValue = (EventName):get_param_by_name(Event, EventParam),
			       Acc ++ [ParamValue];
			  ({Op, Left, Right}, Acc) -> Acc ++ lists:flatten([eval_op({Op, Left, Right}, Event)]);
			  (_, Acc) -> Acc 
		       end, [], SelectClause),		     
    list_to_tuple(Keys);
get_group_key(ResultRecord, SelectClause) when is_list(ResultRecord) ->
    Keys = lists:foldl(fun({EventName, EventParam}, Acc) when is_atom(EventName) andalso is_atom(EventParam) ->
			       Event = lists:keyfind(EventName, 1, ResultRecord),
			       ParamValue = (EventName):get_param_by_name(Event, EventParam),
			       Acc ++ [ParamValue];
			  ({Op, Left, Right}, Acc) -> Acc ++ lists:flatten([eval_op({Op, Left, Right}, ResultRecord)]);
			  (_, Acc) -> Acc 
		       end, [], SelectClause),		     
    list_to_tuple(Keys).


eval_op({EventName,ParamName}, ResultRecord) when is_atom(EventName) andalso is_atom(ParamName) andalso is_list(ResultRecord)->
    Event = lists:keyfind(EventName, 1, ResultRecord),
    EventName:get_param_by_name(Event, ParamName);
eval_op({EventName,ParamName}, Event) when is_atom(EventName) andalso is_atom(ParamName)->
    EventName:get_param_by_name(Event, ParamName);
eval_op({integer, Value}, _) ->
     Value;
eval_op({float, Value}, _) ->
    Value;
eval_op({atom, Value}, _) ->
    Value;
eval_op({_,T}, _) when is_tuple(T)-> %this is an aggregation operation
    [];
eval_op({Op,Left,Right}, ResultRecord) when Left=/=[] andalso Right=/=[]->
    L = eval_op(Left, ResultRecord),
    R = eval_op(Right, ResultRecord),
    case L=/=[] andalso R=/=[] of
	true ->  case Op of
		      plus -> L + R;
		      minus -> L - R;
		      mult -> L * R;
		      'div' -> L / R;
		      _ -> undefined_op
		 end;
	false -> []
    end;
eval_op({_,_,_}, _) ->
    [blah].

new_state() ->
    #res_eval_state{}.
