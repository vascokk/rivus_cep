-module(rivus_cep_aggregation).

-export([eval_resultset/3, get_group_key/1, new_state/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("../deps/folsom/include/folsom.hrl").
-include("rivus_cep.hrl").

eval_resultset(Stmt, [H|T], #res_eval_state{recno = RecNo, res_keys = ResKeys} = State) ->
    Key = get_group_key(H),
    ResultRecord = eval_result_record(Stmt, H, Key, State),
    NewResKeys = case dict:is_key(Key, ResKeys) of
		  false -> folsom_metrics:new_gauge(Key),
			   dict:store(Key, true, ResKeys);
		  true -> ResKeys
	      end,

    folsom_metrics_gauge:update(Key, ResultRecord),
    eval_resultset(Stmt, T, State#res_eval_state{recno=RecNo+1, res_keys = NewResKeys});
eval_resultset(Stmt, [], State) ->
    Res = [folsom_metrics_gauge:get_value(Key) || Key <- dict:fetch_keys(State#res_eval_state.res_keys)],
    [folsom_metrics:delete_metric(Key) || Key <- dict:fetch_keys(State#res_eval_state.res_keys)],
    [folsom_metrics:delete_metric(Key) || Key <- orddict:fetch_keys(State#res_eval_state.aggr_keys)],
    Res.



eval_result_record(Stmt, ResultRecord, Key, _State) ->
    Nodes = tuple_to_list(ResultRecord),
    %%?debugMsg(io_lib:format("Nodes: ~p~n",[Nodes])),
    {Res, _} = lists:mapfoldl(fun(Node, State) -> eval_node(Stmt, Node, Key, State) end, _State, Nodes),
    list_to_tuple(Res).

	    
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

eval_aggregation(sum, Stmt, Value, Key, #res_eval_state{aggrno = AggrNo, aggr_keys = Dict} = State) ->
    NewDict = case orddict:is_key({Stmt, Key, AggrNo}, Dict) of
		  false -> folsom_metrics:new_counter({Stmt, Key, AggrNo}),
			   orddict:store({Stmt, Key, AggrNo}, true, Dict);
		  true -> Dict
	      end,		 
    {folsom_metrics_counter:inc({Stmt, Key, AggrNo}, Value), State#res_eval_state{aggrno = AggrNo+1, aggr_keys = NewDict}};
eval_aggregation(count, Stmt, Value, Key, #res_eval_state{aggrno = AggrNo, aggr_keys = Dict} = State) ->
    NewDict = case orddict:is_key({Stmt,Key,AggrNo}, Dict) of
		  false -> folsom_metrics:new_counter({Stmt,Key, AggrNo}),
			   orddict:store({Stmt,Key,AggrNo}, true, Dict);
		  true -> Dict
	      end,		 
    {folsom_metrics_counter:inc({Stmt,Key, AggrNo}, 1), State#res_eval_state{aggrno = AggrNo+1, aggr_keys = NewDict}}.

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
