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
	  fsm_states = dict:new(),
	  pattern = [],
	  producers,
	  subscribers,
	  events = [] ,
	  timeout = 60,
	  query_ast,
	  window,
	  win_register,
	  query_plan = #query_plan{}
}).

%%% API functions

start_link({QueryClauses, Producers, Subscribers, Options, QueryWindow, GlobalWinReg}) ->
    {QueryName} = hd(QueryClauses),
    gen_server:start_link( {local, QueryName}, ?MODULE, [QueryClauses, Producers, Subscribers,
							 Options, QueryWindow, GlobalWinReg], []).

init([QueryClauses, Producers, Subscribers, Options, QueryWindow, GlobalWinReg]) ->
    [{QueryName}, {SelectClause}, FromClause, {WhereClause}, {WithinClause}] = QueryClauses,
    {QueryType, Events} = case FromClause of
                                   {pattern, {Events}} -> {pattern, Events};
                                   {Events} -> {simple, Events}
                               end,
   
    Ast = #query_ast{select=SelectClause, from=FromClause, where=WhereClause, within=WithinClause},

    case QueryWindow of
    	global -> [ gproc:reg({p, l, {Producer, Event, global }}) || Producer<-Producers, Event <- Events];
    	_ -> [ gproc:reg({p, l, {Producer, Event }}) || Producer<-Producers, Event <- Events]
    end,
    
    Plan = rivus_cep_query_planner:analyze(QueryClauses),
    
    lager:debug("~nStarting: ~p, PID: ~p, Query window: ~p, GlobalWinRegister: ~p ~n",
		[QueryName, self(), QueryWindow, GlobalWinReg]),

    {ok, #state{query_name = QueryName,
		query_type = QueryType,
		window = QueryWindow,
		win_register =  GlobalWinReg,
		producers = Producers,
		subscribers = Subscribers,
		events = Events,
		query_ast = Ast,
		fsm_states = dict:new(),%%hd(Events),
		pattern = lists:zip(Events, lists:seq(1,length(Events))),
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

send_result(pattern, #state{fsm_states = FsmDict, query_ast = Ast} = State) ->
         
    Reservoirs = lists:map(fun({_, {_, Reservoir}}) -> dict:to_list(Reservoir) end, dict:to_list(FsmDict)),

    PreResultSet =  lists:map(fun({_, EventList}) -> EventList end, Reservoirs),

    
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
where_eval({Type, Value}, ResRecord) ->
    case Type of
	integer -> Value;
	float -> Value;
	EventName -> Event = lists:keyfind(EventName,1,ResRecord),
		     (EventName):get_param_by_name(Event,Value)

    end;
where_eval({neg,Predicate}, ResRecord) ->
    {neg,where_eval(Predicate, ResRecord)}.


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
       
build_pred_key_for_event(Event, JoinKeys) ->
    EventName = element(1,Event),
    {_, ParamList} = lists:keyfind(EventName, 1, JoinKeys),
    ParamValues = lists:foldl(fun(ParamName, Acc) ->
				      Acc ++ [EventName:get_param_by_name(Event, ParamName)]			    
			      end, [], ParamList),
    {EventName, list_to_tuple(ParamValues)}.

maybe_update_fsm_state(EventName, Event, PredicateKey, #state{fsm_states = FsmStatesDict, pattern = _Pattern} = State) ->
    case dict:is_key(PredicateKey, FsmStatesDict) of
	false ->  State#state{fsm_states = set_initial_fsm_state_for_predicate(PredicateKey, _Pattern, FsmStatesDict)};
	true -> {FsmStateName, Reservoir} = dict:fetch(PredicateKey, FsmStatesDict), 
		lager:debug("FSM States: ~p~n",[_Pattern]),
		{_, FsmStateNo} = lists:keyfind(FsmStateName, 1, _Pattern), 
		{_, EventStateNo} = lists:keyfind(EventName, 1, _Pattern),
		lager:debug("fsm state: ~p , event state: ~p~n",[FsmStateName, EventName]),
		lager:debug("fsm state no: ~p, event stateno: ~p~n",[FsmStateNo, EventStateNo]),
		case FsmStateNo == EventStateNo of
		    true -> set_next_state(EventName, Event, FsmStateNo, Reservoir, PredicateKey, State);      
		    false  -> State
		end
    end.

set_initial_fsm_state_for_predicate(PredicateKey, [{FsmStateName, _}|T], FsmStatesDict) ->
    lager:debug("Set initial state !!! ~n"),
    Reservoir = dict:new(),    
    dict:store(PredicateKey, {FsmStateName, Reservoir}, FsmStatesDict).
    
update_event_reservoir(PredicateKey, EventName, Event, FsmStatesDict, _Pattern, FsmStateNo) ->
     lager:debug("update_event_reservoir ---------------- ~n"),
    NewFsmStateName = element(1, lists:nth(FsmStateNo, _Pattern)),
    {_, Reservoir} = dict:fetch(PredicateKey, FsmStatesDict),
    NewRes = dict:append(EventName, Event, Reservoir),
    dict:store(PredicateKey, {NewFsmStateName, NewRes}, FsmStatesDict).
    

set_next_state(EventName, Event, FsmStateNo, Reservoir, PredicateKey, #state{fsm_states = FsmStatesDict,
									     pattern = _Pattern} = State) ->   
    Len = length(_Pattern),
    NewFsmStatesDict = case FsmStateNo of
			   Len -> TmpState = State#state{fsm_states =
							     update_event_reservoir(PredicateKey, EventName,
										    Event, FsmStatesDict, _Pattern,
										    FsmStateNo)},
				  send_result(pattern, State),
				  set_initial_fsm_state_for_predicate(PredicateKey, _Pattern, FsmStatesDict);
			   _ -> update_event_reservoir(PredicateKey, EventName, Event, FsmStatesDict,
						       _Pattern, FsmStateNo)
		       end,
    State#state{fsm_states = NewFsmStatesDict}.
    

new_window(#state{timeout = Timeout}) ->
    rivus_cep_window:new(Timeout).

is_initial_state(EventName, _Pattern) ->
    {_, EventStateNo} = lists:keyfind(EventName, 1, _Pattern),
    EventStateNo == 1.

spawn_new_fsm(EventName, Event, State) ->	    
    %% create new fsm process

    %% add the process to the query sliding window
    tbd.

notify_all_fsm(EventName, Event) ->
    %%get the current sliding window
    %%send the event to all the processes
    tbd.

%%----------------------------------------------------------------------------------------------
%% gen_server functions
%%----------------------------------------------------------------------------------------------

handle_call(stop, From, State) ->
    {stop, normal, ok, State};	
handle_call(Request, From, State) ->
    Reply = {ok, notsupported} ,
    {reply, Reply, State}.

handle_cast(Msg, State) ->
    {noreply, State}.

handle_info({ EventName , Event}, #state{query_type = QueryType, window = Window} = State) when QueryType == simple->
    lager:debug("handle_info, query_type: simple,  Event: ~p",[Event]),

    case Window of
	global -> ok;
	_ -> rivus_cep_window:update(Window, Event)		
    end,
    send_result(State),
    {noreply, State};
handle_info({ EventName , Event}, #state{query_type = QueryType} = State) when QueryType == pattern ->
    lager:debug("handle_info, query_type: pattern, EventName: ~p,  Event: ~p",[EventName,Event]),
    %% JoinKeys = (State#state.query_plan)#query_plan.join_keys,    
    %% PredicateKey = build_pred_key_for_event(Event, JoinKeys),
    %% lager:debug("Predicate key: ~p~n",[PredicateKey]),
    %% NewState = maybe_update_fsm_state(EventName, Event, PredicateKey, State),

    %% case Window of
    %% 	global -> ok;
    %% 	_ -> rivus_cep_window:update(Window, Event)		
    %% end,

    %check if it is state0 (initial state):
    % if it is - create new FSM process and store it into the window
    % if it is not - send the event to every process in the window for evaluation. Posssible optimisation: check which state the event equals to and get only           the FSMs in ("this state"-1)

    NewState = case is_initial_state(EventName, State#state.pattern) of
		   true -> spawn_new_fsm(EventName, Event, State);
		   false -> notify_all_fsm(EventName, Event)
	       end,

    send_result(State),    
    {noreply, NewState}; 
handle_info(Info, State) ->
    lager:debug("Statement: ~p,  handle_info got event: ~p. Will do nothing ...",[ State#state.query_name ,Info]),
    {noreply, State}.

terminate(Reason, State) ->
    lager:debug("Worker stopped. Reason: ~p~n",[Reason]),
    ok.

code_change(OldVsn, State, Extra) ->
    {ok, State}.
