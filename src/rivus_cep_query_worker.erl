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

-module(rivus_cep_query_worker).
-behaviour(gen_server).
-compile([{parse_transform, lager_transform}]).
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include("rivus_cep.hrl").

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, code_change/3, terminate/2]).
-export([start_link/1, send_result/1]).


-record(state,{
	  query_name,
	  query_type,
	  producers,
	  subscribers,
	  events = [] ,
	  timeout = 60,
	  query_ast,
	  window,
	  fsm_window,
	  win_register,
	  query_plan = #query_plan{}
}).

-record(query_ast,{
	  select,
	  from,
	  where,
	  within
	 }).

-record(fsm,{
	  fsm_state,
	  fsm_graph,
	  fsm_events
}).



%%% API functions

start_link({QueryClauses, Producers, Subscribers, Options, EventWindow, FsmWindow, GlobalWinReg}) ->
    {QueryName} = hd(QueryClauses),
    gen_server:start_link( {local, QueryName}, ?MODULE, [QueryClauses, Producers, Subscribers,
							 Options, EventWindow, FsmWindow, GlobalWinReg], []).

init([QueryClauses, Producers, Subscribers, _Options, EventWindow, FsmWindow, GlobalWinReg]) ->
    [{QueryName}, {SelectClause}, FromClause, {WhereClause}, {WithinClause}] = QueryClauses,
    {QueryType, Events} = case FromClause of
                                   {pattern, {List}} -> {pattern, List};
                                   {List} -> {simple, List}
                               end,
   
    Ast = #query_ast{select=SelectClause, from=FromClause, where=WhereClause, within=WithinClause},

    case EventWindow of
    	global -> [ gproc:reg({p, l, {Producer, Event, global }}) || Producer<-Producers, Event <- Events];
    	_ -> [ gproc:reg({p, l, {Producer, Event }}) || Producer<-Producers, Event <- Events]
    end,

    Plan = rivus_cep_query_planner:analyze(QueryClauses),

    lager:debug("~nStarting: ~p, PID: ~p, Query window: ~p, GlobalWinRegister: ~p ~n",
		[QueryName, self(), EventWindow, GlobalWinReg]),

    {ok, #state{query_name = QueryName,
		query_type = QueryType,
		window = EventWindow,
		fsm_window = FsmWindow,
		win_register =  GlobalWinReg,
		producers = Producers,
		subscribers = Subscribers,
		events = Events,
		query_ast = Ast,		
		query_plan = Plan}}.

create_qh_ss(Event, WinReg) ->
    Window = dict:fetch(Event, WinReg),
    {Reservoir, Oldest} = rivus_cep_window:get_window(Window),
    MatchSpec = create_match_spec(Event, Oldest),
    create_qh(MatchSpec, Reservoir).

send_result(#state{events=Events, win_register=WinReg, query_ast = Ast, window = Window} = State) when Window == global ->    
    QueryHandlers = lists:map(fun(Event) ->  create_qh_ss(Event, WinReg) end, Events),
    
    PreResultSet = [qlc:e(QH) || QH <- QueryHandlers ],    
 
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
	     [gproc:send({p, l, {Subscriber, result_subscribers}}, Result) || Subscriber<-State#state.subscribers]
    end;
send_result(#state{events=Events, window = Window, query_ast = Ast} = State) when Window /= global->
    {Reservoir, Oldest} = rivus_cep_window:get_window(Window),
    MatchSpecs = [create_match_spec(Event, Oldest) || Event<- Events],
    QueryHandlers = [create_qh(MS, Reservoir) || MS <- MatchSpecs],

    PreResultSet = [qlc:e(QH) || QH <- QueryHandlers ],    
 
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
	     [gproc:send({p, l, {Subscriber, result_subscribers}}, Result) || Subscriber<-State#state.subscribers]
    end.

%% evaluate predicates on a given edge to allow transition to the state "EventName"
eval_fsm_predicates(Fsm, EventName, Event, State) ->
    G = Fsm#fsm.fsm_graph,   
    Predicates = rivus_cep_query_planner:get_predicates_on_edge(G, Fsm#fsm.fsm_state, EventName),
    case Predicates of
	[] -> true; %% no predicates on edge => can do transition
	_ -> Events = rivus_cep_query_planner:get_events_on_path(G, EventName),
	     eval_fsm_predicates(Fsm, Event, Predicates, Events, State)    
    end.

eval_fsm_predicates(Fsm, Event, Predicates, Events, _State) ->
    Window = Fsm#fsm.fsm_events,
    
    {Reservoir, Oldest} = rivus_cep_window:get_window(Window),
    
    MatchSpecs = [create_match_spec(E, Oldest) || E<- Events ],
    QueryHandlers = [create_qh(MS, Reservoir) || MS <- MatchSpecs],

    PreResultSet = [qlc:e(QH) || QH <- QueryHandlers ],    
 
    
    lager:debug("---> Pre-Result Set: ~p", [PreResultSet]),

    CartesianRes = lists:foldl(fun(Xs, A) -> [[X|Xs1] || X <- Xs, Xs1 <- A] end, [[]], PreResultSet ++ [Event]),

    lager:debug("---> Result Set Cartesian: ~p", [CartesianRes]),

    %SelectClause = Ast#query_ast.select,
    
    FilteredRes = [ResRecord || ResRecord <- CartesianRes, where_eval(Predicates, ResRecord) ],
    lager:debug("---> Filtered Result: ~p", [FilteredRes]),

    case FilteredRes of
	[] -> false;
	L when is_list(L) -> true;
	_ -> erlang:error({badres, "Non-list resultset returned"})
    end.
	    
eval_fsm_result(Fsm, EventName, #state{query_ast = Ast} = State) ->
    G = Fsm#fsm.fsm_graph,   
    Predicates = Ast#query_ast.where,
    Events =  Events = rivus_cep_query_planner:get_events_on_path(G, EventName),            
    Window = Fsm#fsm.fsm_events,
    
    {Reservoir, Oldest} = rivus_cep_window:get_window(Window),
    
    MatchSpecs = [create_match_spec(E, Oldest) || E<- Events],
    QueryHandlers = [create_qh(MS, Reservoir) || MS <- MatchSpecs],

    PreResultSet = [qlc:e(QH) || QH <- QueryHandlers ],    
 
    
    lager:debug("---> Pre-Result Set: ~p", [PreResultSet]),

    CartesianRes = lists:foldl(fun(Xs, A) -> [[X|Xs1] || X <- Xs, Xs1 <- A] end, [[]], PreResultSet),

    lager:debug("---> Result Set Cartesian: ~p", [CartesianRes]),

    SelectClause = Ast#query_ast.select,
    
    FilteredRes = [ResRecord || ResRecord <- CartesianRes, where_eval(Predicates, ResRecord) ],    
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
	     [gproc:send({p, l, {Subscriber, result_subscribers}}, Result) || Subscriber<-State#state.subscribers]
    end.

create_match_spec(Event, Oldest) ->
    ets:fun2ms(fun({ {Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==Event  -> Value end).
    
    
create_qh(MatchSpec, Reservoir) ->
     ets:table(Reservoir, [{traverse, {select, MatchSpec}}]).

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
    %% EventName == rivus_cep_query_planner:get_start_state(Ast#query_ast.from).
    rivus_cep_query_planner:is_first(State#state.query_plan#query_plan.fsm, EventName) .

create_new_fsm(EventName, Event, #state{fsm_window = FsmWindow} = State) ->
    NewFsm = #fsm{fsm_state = EventName,
		  fsm_graph = State#state.query_plan#query_plan.fsm,
		  fsm_events = rivus_cep_window:new(State#state.query_ast#query_ast.within)},
    rivus_cep_window:update(NewFsm#fsm.fsm_events, Event),
    rivus_cep_window:update(FsmWindow, NewFsm),
    State.

%%  select all FSM's from the Reservoir/sliding window (NB: there are 2 windows - for events and for FSM's)
%%  for each FSM in the fsm_window:
%% 1 - check if the next state is "EventName" and edge predicates on the current path eval to true
%% 2 - if (1)==true update the event window, goto(3) else END
%% 3 - check if the new state is the last FSM state
%% 4 - if (3)==true evaluate WHERE predicates and generate resultset (evaluate SELECT clause)
%% 5 - if the result from (4) != [] remove the FSM (i.e. it reached the final state) else do nothing
%% 6 - if the result from (4) != [] and EventName is the first state - create a new FSM and put it in the fsm_window
%% 7 - if (3)==false update the FSM state to "EventName"
eval_fsm(EventName, Event, State) ->    
    FsmWindow = State#state.fsm_window,
    Fsms = rivus_cep_window:get_fsms(FsmWindow),
    F = fun({FsmKey, Fsm}) ->	
		EventWindow = Fsm#fsm.fsm_events,		
		case rivus_cep_query_planner:is_next(Fsm#fsm.fsm_graph, Fsm#fsm.fsm_state, EventName) of
		    true -> case eval_fsm_predicates(Fsm, EventName, Event, State) of
				true -> rivus_cep_window:update(EventWindow,Event), 
					eval_fsm_state(EventName, FsmKey, Fsm, State);
				false -> nil
			    end;
		    false -> nil				 
		end
	end, 
    lists:foreach(F, Fsms),
    State.

eval_fsm_state(EventName, FsmKey, Fsm, State) ->
    FsmWindow = State#state.fsm_window,
    case rivus_cep_query_planner:is_last(Fsm#fsm.fsm_graph, EventName) of
	true -> eval_fsm_result(Fsm#fsm{fsm_state = EventName}, EventName, State),
		rivus_cep_window:delete_fsm(FsmWindow, FsmKey);
	false -> rivus_cep_window:update_fsm(FsmWindow, FsmKey, Fsm#fsm{fsm_state = EventName})
    end.
    

%%----------------------------------------------------------------------------------------------
%% gen_server functions
%%----------------------------------------------------------------------------------------------

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};	
handle_call(_Request, _From, State) ->
    Reply = {ok, notsupported} ,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({ _EventName , Event}, #state{query_type = QueryType, window = Window} = State) when QueryType == simple->
    lager:debug("handle_info, query_type: simple,  Event: ~p",[Event]),

    case Window of
	global -> ok;
	_ -> rivus_cep_window:update(Window, Event)		
    end,
    send_result(State),
    {noreply, State};
handle_info({ EventName , Event}, #state{query_type = QueryType} = State) when QueryType == pattern ->
    lager:debug("handle_info, query_type: pattern, EventName: ~p,  Event: ~p",[EventName,Event]),
    NewState = case is_initial_state(EventName, State) of
		   false -> eval_fsm(EventName, Event, State);
		   true -> create_new_fsm(EventName, Event, State)
	       end,
    {noreply, NewState}; 
handle_info(Info, State) ->
    lager:debug("Statement: ~p,  handle_info got event: ~p. Will do nothing ...",[ State#state.query_name ,Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    lager:debug("Worker stopped. Reason: ~p~n",[_Reason]),
    ok.

code_change(_OldVsn, _State, _Extra) ->
    {ok, _State}.
