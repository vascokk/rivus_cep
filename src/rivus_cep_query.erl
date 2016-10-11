%%------------------------------------------------------------------------------
%% Copyright (c) 2013-2014 Vasil Kolarov
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

-export([init/1, apply_filters/2, process_event/2, get_result_set/1, get_fast_aggr_result_set/1]).


init(QD) ->
  [{QueryName}, {SelectClause}, FromClause, {WhereClause}, WithinClause, {Filters}] = QD#query_details.clauses,
  {QueryType, Events} = case FromClause of
                          {pattern, {List}} -> {pattern, List};
                          {List} -> {simple, List}
                        end,

  {Within, WindowType} = case WithinClause of
                           {WD, WT} -> {WD, WT};
                           {WD} when is_integer(WD) -> {WD, sliding};
                           nil -> {nil, nil}
                         end,

  Ast = #query_ast{select = SelectClause, from = FromClause, where = WhereClause, within = Within},

  case QD#query_details.event_window of
    global -> [gproc:reg({p, l, {Producer, Event, global}}) || Producer <- QD#query_details.producers, Event <- Events];
    _ -> [gproc:reg({p, l, {Producer, Event}}) || Producer <- QD#query_details.producers, Event <- Events]
  end,

  Plan = rivus_cep_query_planner:analyze(QD#query_details.clauses),

  lager:debug("~nStarting: ~p, PID: ~p, Query window: ~p, GlobalWinRegister: ~p ~n",
    [QueryName, self(), QD#query_details.event_window, QD#query_details.window_register]),

  Window = QD#query_details.event_window,
  WinPid = QD#query_details.event_window_pid,

  case Plan#query_plan.fast_aggregations of
    true -> rivus_cep_window:update(WinPid, Window, rivus_cep_result_eval:new_state());
    false -> ok
  end,

  {ok, #query_state{query_name = QueryName,
    query_type = QueryType,
    window = Window,
    window_pid = WinPid,
    stream_filters = Filters,
    fsm_window = QD#query_details.fsm_window,
    global_window_register = QD#query_details.window_register,
    fsm_window_pid = QD#query_details.fsm_window_pid,
    producers = QD#query_details.producers,
    subscribers = QD#query_details.subscribers,
    events = Events,
    query_ast = Ast,
    query_plan = Plan,
    window_type = WindowType}}.

get_fast_aggr_result_set(Event, #query_state{window = AggWindow, window_pid = AggWinPid} = QueryState) ->
  [[AggState]] = rivus_cep_window:get_aggr_state(AggWinPid, local, AggWindow),
  rivus_cep_result_eval:eval_resultset(QueryState#query_state.query_name, [Event], AggState, QueryState).

get_fast_aggr_result_set(QueryState) ->
  rivus_cep_result_eval:eval_resultset_fast_aggr(QueryState).

get_result_set(WindowType, State) ->
  case WindowType of
    batch -> ok;
    _ -> get_result_set(State) %% TODO: should be 'sliding -> get_result(State)'
  end.

get_result_set(#query_state{events = Events, window = Window, query_ast = Ast} = QueryState) ->
  PreResultSet = case Window of
                   global -> GlobalWinReg = QueryState#query_state.global_window_register,
                     rivus_cep_window:get_pre_result(global, GlobalWinReg, Events);
                   _ -> rivus_cep_window:get_pre_result(QueryState#query_state.window_pid, local, Window, Events)
                 end,

  lager:debug("---> Pre-Result Set: ~p", [PreResultSet]),
  CartesianRes = lists:foldl(fun(Xs, A) -> [[X | Xs1] || X <- Xs, Xs1 <- A] end, [[]], PreResultSet),
  lager:debug("---> Result Set Cartesian: ~p", [CartesianRes]),
  WhereClause = Ast#query_ast.where,
  FilteredRes = [ResRecord || ResRecord <- CartesianRes, where_eval(WhereClause, ResRecord)],
  lager:debug("---> Result Set Filtered: ~p", [FilteredRes]),
  get_result_set(FilteredRes, QueryState#query_state.query_plan#query_plan.has_aggregations, QueryState).

get_result_set([], _, _) ->
  [];
get_result_set(ResultSet, false, QueryState) ->
  SelectClause = QueryState#query_state.query_ast#query_ast.select,
  [list_to_tuple(build_result_set(SelectClause, ResRecord, [])) || ResRecord <- ResultSet];
get_result_set(ResultSet, true, QueryState) ->
  rivus_cep_result_eval:eval_resultset(test_stmt, ResultSet, rivus_cep_result_eval:new_state(), QueryState).

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

eval_fsm_predicates(Fsm, Event, Predicates, Events, #query_state{window_pid = Pid}) ->
  Window = Fsm#fsm.fsm_events,

  PreResultSet = rivus_cep_window:get_pre_result(Pid, local, Window, Events),

  lager:debug("---> Pre-Result Set: ~p", [PreResultSet ++ [[Event]]]),

  CartesianRes = lists:foldl(fun(Xs, A) -> [[X | Xs1] || X <- Xs, Xs1 <- A] end, [[]], lists:filter(fun(L) ->
    L /= [] end, PreResultSet) ++ [[Event]]),

  lager:debug("---> Result Set Cartesian: ~p", [CartesianRes]),

  FilteredRes = [ResRecord || ResRecord <- CartesianRes, where_eval(Predicates, ResRecord)],
  lager:debug("---> Filtered Result: ~p", [FilteredRes]),

  case FilteredRes of
    [] -> false;
    L when is_list(L) -> true;
    _ -> erlang:error({badres, "Non-list resultset returned"})
  end.

eval_fsm_result(Fsm, EventName, #query_state{query_ast = Ast, window_pid = Pid} = QueryState) ->
  G = Fsm#fsm.fsm_graph,
  Predicates = Ast#query_ast.where,
  Events = Events = rivus_cep_query_planner:get_events_on_path(G, EventName),
  Window = Fsm#fsm.fsm_events,

  PreResultSet = rivus_cep_window:get_pre_result(Pid, local, Window, Events),

  lager:debug("---> Pre-Result Set: ~p", [PreResultSet]),

  CartesianRes = lists:foldl(fun(Xs, A) -> [[X | Xs1] || X <- Xs, Xs1 <- A] end, [[]], PreResultSet),

  lager:debug("---> Result Set Cartesian: ~p", [CartesianRes]),

  SelectClause = Ast#query_ast.select,

  FilteredRes = [ResRecord || ResRecord <- CartesianRes, where_eval(Predicates, ResRecord)],
  lager:debug("---> Filtered Result: ~p", [FilteredRes]),

  ResultSet = [list_to_tuple(build_result_set(SelectClause, ResRecord, [])) || ResRecord <- FilteredRes],

  lager:debug("---> ResultSet: ~p", [ResultSet]),

  case ResultSet of
    [] -> [];
    _ -> FirstRec = hd(ResultSet),
      Result = case QueryState#query_state.query_plan#query_plan.has_aggregations of
                 false -> ResultSet; %% no aggregations in the 'select' stmt
                 true ->
                   rivus_cep_result_eval:eval_resultset(test_stmt, ResultSet, rivus_cep_result_eval:new_state(), QueryState)
               end,
      lager:debug("---> Result: ~p <-----", [Result]),
      Result
  end.

where_eval({Op, Left, Right}, ResRecord) ->
  case Op of
    'and' -> where_eval(Left, ResRecord) andalso where_eval(Right, ResRecord);
    'or' -> where_eval(Left, ResRecord) orelse where_eval(Right, ResRecord);
    eq -> where_eval(Left, ResRecord) == where_eval(Right, ResRecord);
    gt -> where_eval(Left, ResRecord) > where_eval(Right, ResRecord);
    lt -> where_eval(Left, ResRecord) < where_eval(Right, ResRecord);
    lte -> where_eval(Left, ResRecord) =< where_eval(Right, ResRecord);
    gte -> where_eval(Left, ResRecord) >= where_eval(Right, ResRecord);
    neq -> where_eval(Left, ResRecord) /= where_eval(Right, ResRecord);
    plus -> where_eval(Left, ResRecord) + where_eval(Right, ResRecord);
    minus -> where_eval(Left, ResRecord) - where_eval(Right, ResRecord);
    mult -> where_eval(Left, ResRecord) * where_eval(Right, ResRecord);
    'div' -> where_eval(Left, ResRecord) / where_eval(Right, ResRecord)
  end;
where_eval({neg, Predicate}, ResRecord) ->
  {neg, where_eval(Predicate, ResRecord)};
where_eval({Type, Value}, ResRecord) ->
  case Type of
    integer -> Value;
    float -> Value;
    atom -> Value;
    EventName -> Event = lists:keyfind(EventName, 1, ResRecord),
      (EventName):get_param_by_name(Event, Value)

  end;
where_eval(nil, _ResRecord) ->
  true.

select_eval({integer, Value}, _) ->
  Value;
select_eval({float, Value}, _) ->
  Value;
select_eval({atom, Value}, _) ->
  Value;
select_eval({EventName, ParamName}, ResRecord) when not is_tuple(EventName) andalso not is_tuple(ParamName) ->
  Event = lists:keyfind(EventName, 1, ResRecord),
  EventName:get_param_by_name(Event, ParamName);
select_eval({sum, SumTuple}, ResRecord) ->
  {sum, {select_eval(SumTuple, ResRecord)}};
select_eval({avg, AvgTuple}, ResRecord) ->
  {avg, {select_eval(AvgTuple, ResRecord)}};
select_eval({count, CountTuple}, ResRecord) ->
  {count, {select_eval(CountTuple, ResRecord)}};
select_eval({min, MinTuple}, ResRecord) ->
  {min, {select_eval(MinTuple, ResRecord)}};
select_eval({max, MaxTuple}, ResRecord) ->
  {max, {select_eval(MaxTuple, ResRecord)}};
select_eval({Op, Left, Right}, ResRecord) ->
  case Op of
    plus -> select_eval(Left, ResRecord) + select_eval(Right, ResRecord);
    minus -> select_eval(Left, ResRecord) - select_eval(Right, ResRecord);
    mult -> select_eval(Left, ResRecord) * select_eval(Right, ResRecord);
    'div' -> select_eval(Left, ResRecord) / select_eval(Right, ResRecord);
    _ -> undefined_select_op
  end.

build_result_set([H | T], EventList, Acc) ->
  build_result_set(T, EventList, Acc ++ [select_eval(H, EventList)]);
build_result_set([], _, Acc) ->
  Acc.

is_initial_state(EventName, State) ->
  rivus_cep_query_planner:is_first(State#query_state.query_plan#query_plan.fsm, EventName).

create_new_fsm(EventName, Event, #query_state{fsm_window = FsmWindow, window_pid = EvWinPid, fsm_window_pid = FsmWinPid} = State) ->
  NewFsm = #fsm{fsm_state = EventName,
    fsm_graph = State#query_state.query_plan#query_plan.fsm,
    fsm_events = rivus_cep_window:new(EvWinPid, slide, State#query_state.query_ast#query_ast.within)},
  rivus_cep_window:update(EvWinPid, NewFsm#fsm.fsm_events, Event),
  rivus_cep_window:update(FsmWinPid, FsmWindow, NewFsm),
  [].

%% eval_fsm() evaluates the FSM state, using the following algorithm: 
%%  select all FSM's from the Reservoir/sliding window. (NB: there are 2 windows - one for events and one for FSM states)
%%  for each FSM in the fsm_window:
%% 1 - check if the next state is <EventName> and edge predicates on the current path evaluate to true
%% 2 - if (1)==true update the event window, goto(3) else END
%% 3 - check if the new state is the last FSM state
%% 4 - if (3)==true evaluate WHERE predicates and generate resultset (evaluate SELECT clause)
%% 4.1 - remove the FSM (i.e. it reached the final state) else do nothing. END.
%% 5 - if (3)==false (not the last state) - update the FSM state to <EventName>
eval_fsm(EventName, Event, #query_state{window_pid = EvWinPid, fsm_window_pid = FsmWinPid} = State) ->
  FsmWindow = State#query_state.fsm_window,
  Fsms = rivus_cep_window:get_fsms(FsmWinPid, FsmWindow),
  F = fun({FsmKey, Fsm}, Acc) ->
    EventWindow = Fsm#fsm.fsm_events,
    case rivus_cep_query_planner:is_next(Fsm#fsm.fsm_graph, Fsm#fsm.fsm_state, EventName) of
      true -> case eval_fsm_predicates(Fsm, EventName, Event, State) of
                true -> rivus_cep_window:update(EvWinPid, EventWindow, Event),
                  Acc ++ [eval_fsm_state(EventName, FsmKey, Fsm, State)];
                false -> Acc
              end;
      false -> Acc
    end
  end,
  lists:flatten(lists:foldl(F, [], Fsms)).

eval_fsm_state(EventName, FsmKey, Fsm, #query_state{fsm_window_pid = FsmWinPid} = State) ->
  FsmWindow = State#query_state.fsm_window,
  case rivus_cep_query_planner:is_last(Fsm#fsm.fsm_graph, EventName) of
    true -> Result = eval_fsm_result(Fsm#fsm{fsm_state = EventName}, EventName, State),
      lager:debug("Delete FsmID: ~p~n", [FsmKey]),
      rivus_cep_window:delete_fsm(FsmWinPid, FsmWindow, FsmKey),
      Result;
    false -> rivus_cep_window:update_fsm(FsmWinPid, FsmWindow, FsmKey, Fsm#fsm{fsm_state = EventName}),
      []
  end.

reset_fsm() ->
  todo.

update_event_window(Pid, Window, Event) ->
  case Window of
    global -> ok;
    _ -> rivus_cep_window:update(Pid, Window, Event)
  end.


process_event(Event, #query_state{query_type = QueryType, window = Window, window_pid = Pid, window_type = WindowType} = State) when QueryType == simple ->
  lager:debug("rivus_cep_query:process_event, query_type: simple,  Event: ~p", [Event]),
  case rivus_cep_query:apply_filters(Event, State#query_state.stream_filters) of
    true -> case State#query_state.query_plan#query_plan.fast_aggregations of
              true -> get_fast_aggr_result_set(Event, State);
              false -> update_event_window(Pid, Window, Event),
                get_result_set(WindowType, State)
            end;
    false -> []
  end;
process_event(Event, #query_state{query_type = QueryType} = State) when QueryType == pattern ->
  EventName = element(1, Event),
  lager:debug("rivus_cep_query:process_event, query_type: pattern, EventName: ~p,  Event: ~p", [EventName, Event]),
  fprof:trace(start, "/home/evaskol/work/rivus_cep/fprof2.log"),
  case rivus_cep_query:apply_filters(Event, State#query_state.stream_filters) of
    true -> case is_initial_state(EventName, State) of
              false -> eval_fsm(EventName, Event, State);
              true -> create_new_fsm(EventName, Event, State)
            end;
    false -> []
  end.

apply_filters(Event, FiltersDict) ->
  EventName = element(1, Event),
  Filters = case orddict:is_key(EventName, FiltersDict) of
              true -> orddict:fetch(EventName, FiltersDict);
              false -> []
            end,
  lists:foldl(fun(Filter, Acc) ->
    case eval_filter(Event, Filter) of
      true -> Acc;
      false -> false
    end
  end,
    true, Filters).

eval_filter(Event, {eq, Left, Right}) ->
  eval_filter(Event, Left) == eval_filter(Event, Right);
eval_filter(Event, {lt, Left, Right}) ->
  eval_filter(Event, Left) < eval_filter(Event, Right);
eval_filter(Event, {gt, Left, Right}) ->
  eval_filter(Event, Left) > eval_filter(Event, Right);
eval_filter(Event, {lte, Left, Right}) ->
  eval_filter(Event, Left) =< eval_filter(Event, Right);
eval_filter(Event, {gte, Left, Right}) ->
  eval_filter(Event, Left) >= eval_filter(Event, Right);
eval_filter(Event, {ne, Left, Right}) ->
  eval_filter(Event, Left) /= eval_filter(Event, Right);
eval_filter(_Event, {integer, Value}) ->
  Value;
eval_filter(_Event, {float, Value}) ->
  Value;
eval_filter(_Event, {string, Value}) ->
  Value;
eval_filter(_Event, {atom, Value}) ->
  Value;
eval_filter(Event, {EventName, ParamName}) ->
  EventName:get_param_by_name(Event, ParamName).

