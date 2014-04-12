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

-module(rivus_cep_query).
-compile([{parse_transform, lager_transform}]).
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include("rivus_cep.hrl").

-export([init/1, process_event/2, get_result/1]).


init(QD) -> 
    [{QueryName}, {SelectClause}, FromClause, {WhereClause}, {WithinClause}, {Filters}] = QD#query_details.clauses,
    {QueryType, Events} = case FromClause of
                                   {pattern, {List}} -> {pattern, List};
                                   {List} -> {simple, List}
                               end,
   
    Ast = #query_ast{select=SelectClause, from=FromClause, where=WhereClause, within=WithinClause},

    case QD#query_details.event_window of
    	global -> [ gproc:reg({p, l, {Producer, Event, global }}) || Producer<-QD#query_details.producers, Event <- Events];
    	_ -> [ gproc:reg({p, l, {Producer, Event }}) || Producer<-QD#query_details.producers, Event <- Events]
    end,

    Plan = rivus_cep_query_planner:analyze(QD#query_details.clauses),

    lager:debug("~nStarting: ~p, PID: ~p, Query window: ~p, GlobalWinRegister: ~p ~n",
		[QueryName, self(), QD#query_details.event_window, QD#query_details.window_register]),

    {ok, #query_state{query_name = QueryName, 
		      query_type = QueryType,
		      window = QD#query_details.event_window,
		      fsm_window = QD#query_details.fsm_window,
		      win_register =  QD#query_details.window_register,
		      event_win_pid = QD#query_details.event_window_pid,
		      fsm_win_pid= QD#query_details.fsm_window_pid,
		      producers = QD#query_details.producers, 
		      subscribers = QD#query_details.subscribers,
		      events = Events,
		      query_ast = Ast,		
		      query_plan = Plan}}.

get_result(#query_state{events=Events, win_register=WinReg, query_ast = Ast, window = Window} = _State) when Window == global ->    
    PreResultSet = rivus_cep_window:get_pre_result(global, WinReg, Events),
 
    lager:debug("---> Pre-Result Set: ~p", [PreResultSet]),

    CartesianRes = lists:foldl(fun(Xs, A) -> [[X|Xs1] || X <- Xs, Xs1 <- A] end, [[]], PreResultSet),

    lager:debug("---> Result Set Cartesian: ~p", [CartesianRes]),

    WhereClause = Ast#query_ast.where,
    SelectClause = Ast#query_ast.select,
    
    FilteredRes = [ResRecord || ResRecord <- CartesianRes, where_eval(WhereClause, ResRecord) ],
    
    lager:debug("---> Filtered Result: ~p", [FilteredRes]),

    ResultSet = [list_to_tuple(build_select_clause(SelectClause, ResRecord,  [])) || ResRecord <-FilteredRes],

    lager:debug("---> ResultSet: ~p", [ResultSet]),
    
    case ResultSet of
    	[] -> nil;
    	_ -> FirstRec = hd(ResultSet),
	     Key = rivus_cep_aggregation:get_group_key(FirstRec),
	     %%naive check for aggregations in the select clause:
	     Result = case length(tuple_to_list(FirstRec)) == length(tuple_to_list(Key)) of
			  true -> ResultSet; %% no aggregations in the 'select' stmt
			  false -> rivus_cep_aggregation:eval_resultset(test_stmt, ResultSet, rivus_cep_aggregation:new_state())
		      end,
	     lager:debug("---> Result: ~p <-----", [Result]),
	     Result
    end;
get_result(#query_state{events=Events, window = Window, query_ast = Ast, event_win_pid = Pid} = _State) when Window /= global->
    
    PreResultSet = rivus_cep_window:get_pre_result(Pid, local, Window, Events),
 
    lager:debug("---> Pre-Result Set: ~p", [PreResultSet]),

    CartesianRes = lists:foldl(fun(Xs, A) -> [[X|Xs1] || X <- Xs, Xs1 <- A] end, [[]], PreResultSet),

    lager:debug("---> Result Set Cartesian: ~p", [CartesianRes]),

    WhereClause = Ast#query_ast.where,
    SelectClause = Ast#query_ast.select,
    
    FilteredRes = [ResRecord || ResRecord <- CartesianRes, where_eval(WhereClause, ResRecord) ],
    
    lager:debug("---> Filtered Result: ~p", [FilteredRes]),

    ResultSet = [list_to_tuple(build_select_clause(SelectClause, ResRecord,  [])) || ResRecord <-FilteredRes],

    lager:debug("---> ResultSet: ~p", [ResultSet]),
    
    case ResultSet of
    	[] -> nil;
    	_ -> FirstRec = hd(ResultSet),
	     Key = rivus_cep_aggregation:get_group_key(FirstRec),
	     %%naive check for aggregations in the select clause:
	     Result = case length(tuple_to_list(FirstRec)) == length(tuple_to_list(Key)) of
			  true -> ResultSet; %% no aggregations in the 'select' stmt
			  false -> rivus_cep_aggregation:eval_resultset(test_stmt, ResultSet, rivus_cep_aggregation:new_state())
		      end,
	     lager:debug("---> Result: ~p <-----", [Result]),
	     Result	     
    end.

%% evaluate predicates on a given edge to allow transition to the state "EventName"
eval_fsm_predicates(Fsm, EventName, Event, State) ->
    G = Fsm#fsm.fsm_graph,   
    Predicates = rivus_cep_query_planner:get_predicates_on_edge(G, Fsm#fsm.fsm_state, EventName),
    case Predicates of
	[] -> true; %% no predicates on this edge => can do transition
	_ -> SinglePredicate = rivus_cep_query_planner:to_single_predicate(Predicates),
	    Events = rivus_cep_query_planner:get_events_on_path(G, EventName),
	     eval_fsm_predicates(Fsm, Event, SinglePredicate, Events, State)    
    end.

eval_fsm_predicates(Fsm, Event, Predicates, Events, #query_state{event_win_pid = Pid} = _State) ->
    Window = Fsm#fsm.fsm_events,
  
    PreResultSet = rivus_cep_window:get_pre_result(Pid, local, Window, Events),
    
    lager:debug("---> Pre-Result Set: ~p", [PreResultSet++ [[Event]]]),

    CartesianRes = lists:foldl(fun(Xs, A) -> [[X|Xs1] || X <- Xs, Xs1 <- A] end, [[]], lists:filter(fun(L) -> L /=[] end, PreResultSet) ++ [[Event]]),

    lager:debug("---> Result Set Cartesian: ~p", [CartesianRes]),
    
    FilteredRes = [ResRecord || ResRecord <- CartesianRes, where_eval(Predicates, ResRecord) ],
    lager:debug("---> Filtered Result: ~p", [FilteredRes]),

    case FilteredRes of
	[] -> false;
	L when is_list(L) -> true;
	_ -> erlang:error({badres, "Non-list resultset returned"})
    end.
	    
eval_fsm_result(Fsm, EventName, #query_state{query_ast = Ast, event_win_pid = Pid} = _State) ->
    G = Fsm#fsm.fsm_graph,   
    Predicates = Ast#query_ast.where,
    Events =  Events = rivus_cep_query_planner:get_events_on_path(G, EventName),            
    Window = Fsm#fsm.fsm_events,
    
    PreResultSet = rivus_cep_window:get_pre_result(Pid, local, Window, Events),
    
    lager:debug("---> Pre-Result Set: ~p", [PreResultSet]),

    CartesianRes = lists:foldl(fun(Xs, A) -> [[X|Xs1] || X <- Xs, Xs1 <- A] end, [[]], PreResultSet),

    lager:debug("---> Result Set Cartesian: ~p", [CartesianRes]),

    SelectClause = Ast#query_ast.select,
    
    FilteredRes = [ResRecord || ResRecord <- CartesianRes, where_eval(Predicates, ResRecord) ],    
    lager:debug("---> Filtered Result: ~p", [FilteredRes]),

    ResultSet = [list_to_tuple(build_select_clause(SelectClause, ResRecord,  [])) || ResRecord <-FilteredRes],

    lager:debug("---> ResultSet: ~p", [ResultSet]),
    
    case ResultSet of
    	[] -> [];
    	_ -> FirstRec = hd(ResultSet),
	     Key = rivus_cep_aggregation:get_group_key(FirstRec),
	     %%naive check for aggregations in the select clause:
	     Result = case length(tuple_to_list(FirstRec)) == length(tuple_to_list(Key)) of
			  true -> ResultSet; %% no aggregations in the 'select' stmt
			  false -> rivus_cep_aggregation:eval_resultset(test_stmt, ResultSet, rivus_cep_aggregation:new_state())
		      end,
	     lager:debug("---> Result: ~p <-----", [Result]),
	     Result
    end.

where_eval({Op, Left, Right}, ResRecord) ->
    case Op of
	'and' -> where_eval(Left, ResRecord) andalso where_eval(Right, ResRecord);
	'or' -> where_eval(Left, ResRecord) orelse where_eval(Right, ResRecord);
	eq -> where_eval(Left, ResRecord) ==  where_eval(Right, ResRecord) ;
	gt -> where_eval(Left, ResRecord) > where_eval(Right, ResRecord);
	lt -> where_eval(Left, ResRecord) < where_eval(Right, ResRecord);
	lte -> where_eval(Left, ResRecord) =< where_eval(Right, ResRecord);
	gte -> where_eval(Left, ResRecord) >= where_eval(Right, ResRecord);
	ne -> where_eval(Left, ResRecord) /= where_eval(Right, ResRecord); 
	plus -> where_eval(Left, ResRecord) + where_eval(Right, ResRecord);
	minus -> where_eval(Left, ResRecord) - where_eval(Right, ResRecord);
	mult -> where_eval(Left, ResRecord) * where_eval(Right, ResRecord); 
	'div' -> where_eval(Left, ResRecord) / where_eval(Right, ResRecord)
    end;
where_eval({neg,Predicate}, ResRecord) ->
    {neg,where_eval(Predicate, ResRecord)};
where_eval({Type, Value}, ResRecord) ->
    case Type of
	integer -> Value;
	float -> Value;
	EventName -> Event = lists:keyfind(EventName,1,ResRecord),
		     (EventName):get_param_by_name(Event,Value)

    end.

select_eval({integer, Value}, _) ->
     Value;
select_eval({float, Value}, _) ->
    Value;
select_eval({EventName,ParamName}, ResRecord) when not is_tuple(EventName) andalso not is_tuple(ParamName)->
    Event = lists:keyfind(EventName,1,ResRecord),
    EventName:get_param_by_name(Event, ParamName);
select_eval({sum,SumTuple}, ResRecord) ->
    {sum,{select_eval(SumTuple, ResRecord)}}; 
select_eval({avg,AvgTuple}, ResRecord) ->
    {avg, {select_eval(AvgTuple, ResRecord)}}; 
select_eval({count,CountTuple}, ResRecord) ->
    {count,{select_eval(CountTuple, ResRecord)}}; 
select_eval({min,MinTuple}, ResRecord) ->
    {min,{select_eval(MinTuple, ResRecord)}};
select_eval({max,MaxTuple}, ResRecord) ->
    {max,{select_eval(MaxTuple, ResRecord)}};
select_eval({Op,Left,Right}, ResRecord) ->
     {Op,{select_eval(Left, ResRecord)},select_eval(Right, ResRecord)}.

build_select_clause([H|T], EventList, Acc) ->
     build_select_clause(T, EventList, Acc ++ [select_eval(H, EventList)]);
build_select_clause([], _, Acc) ->
     Acc.
       
is_initial_state(EventName, State) ->
    rivus_cep_query_planner:is_first(State#query_state.query_plan#query_plan.fsm, EventName) .

create_new_fsm(EventName, Event, #query_state{fsm_window = FsmWindow, event_win_pid = EvWinPid, fsm_win_pid = FsmWinPid} = State) ->
    NewFsm = #fsm{fsm_state = EventName,
		  fsm_graph = State#query_state.query_plan#query_plan.fsm,
		  fsm_events = rivus_cep_window:new(EvWinPid, slide, State#query_state.query_ast#query_ast.within)},
    rivus_cep_window:update(EvWinPid, NewFsm#fsm.fsm_events, Event),
    rivus_cep_window:update(FsmWinPid, FsmWindow, NewFsm),
    %%State.
    [].

%%  select all FSM's from the Reservoir/sliding window (NB: there are 2 windows - for events and for FSM's)
%%  for each FSM in the fsm_window:
%% 1 - check if the next state is "EventName" and edge predicates on the current path eval to true
%% 2 - if (1)==true update the event window, goto(3) else END
%% 3 - check if the new state is the last FSM state
%% 4 - if (3)==true evaluate WHERE predicates and generate resultset (evaluate SELECT clause)
%% 4.1 - remove the FSM (i.e. it reached the final state) else do nothing. END.
%% 5 - if (3)==false (not the last state) - update the FSM state to "EventName"

eval_fsm(EventName, Event, #query_state{event_win_pid = EvWinPid, fsm_win_pid = FsmWinPid} = State) ->    
    FsmWindow = State#query_state.fsm_window,
    Fsms = rivus_cep_window:get_fsms(FsmWinPid, FsmWindow),
    F = fun({FsmKey, Fsm}, Acc) ->	
		EventWindow = Fsm#fsm.fsm_events,		
		case rivus_cep_query_planner:is_next(Fsm#fsm.fsm_graph, Fsm#fsm.fsm_state, EventName) of
		    true -> case eval_fsm_predicates(Fsm, EventName, Event, State) of
				true -> rivus_cep_window:update(EvWinPid, EventWindow,Event), 
					Acc ++ [eval_fsm_state(EventName, FsmKey, Fsm, State)];
				false -> Acc
			    end;
		    false -> Acc				 
		end
	end, 
    lists:flatten(lists:foldl(F, [], Fsms)). 

eval_fsm_state(EventName, FsmKey, Fsm,  #query_state{fsm_win_pid = FsmWinPid} = State) ->
    FsmWindow = State#query_state.fsm_window,
    case rivus_cep_query_planner:is_last(Fsm#fsm.fsm_graph, EventName) of
	true -> Result = eval_fsm_result(Fsm#fsm{fsm_state = EventName}, EventName, State),		
		lager:debug("Delete FsmID: ~p~n",[FsmKey]),
		rivus_cep_window:delete_fsm(FsmWinPid, FsmWindow, FsmKey),
		Result;
	false -> rivus_cep_window:update_fsm(FsmWinPid, FsmWindow, FsmKey, Fsm#fsm{fsm_state = EventName}),
		 []
    end.
    

%%----------------------------------------------------------------------------------------------
%% gen_server functions
%%----------------------------------------------------------------------------------------------


process_event(Event, #query_state{query_type = QueryType, window = Window, event_win_pid=Pid} = State) when QueryType == simple->
    lager:debug("rivus_cep_query:process_event, query_type: simple,  Event: ~p",[Event]),

    case Window of
	global -> ok;
	_ -> rivus_cep_window:update(Pid, Window, Event)		
    end,
    get_result(State);
process_event(Event, #query_state{query_type = QueryType} = State) when QueryType == pattern ->
    EventName = element(1, Event),
    lager:debug("rivus_cep_query:process_event, query_type: pattern, EventName: ~p,  Event: ~p",[EventName,Event]),
    case is_initial_state(EventName, State) of
	false -> eval_fsm(EventName, Event, State);
	true -> create_new_fsm(EventName, Event, State),
		[]
    end.

    
