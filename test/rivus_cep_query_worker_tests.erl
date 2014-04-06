-module(rivus_cep_query_worker_tests).

-compile([debug_info, export_all]).
-compile([{parse_transform, lager_transform}]).

-include_lib("eunit/include/eunit.hrl").
-include("rivus_cep.hrl").

query_worker_test_() ->
    {setup,
     fun () ->
	     folsom:start(),
	     lager:start(),
	     application:start(gproc),
	     lager:set_loglevel(lager_console_backend, debug),
	     application:set_env(rivus_cep, rivus_window_provider, rivus_cep_slide),
	     ok = application:start(rivus_cep)
     end,
     fun (_) ->
	     folsom:stop(),
	     application:stop(lager),
	     application:stop(gproc),
	     ok = application:stop(rivus_cep)
     end,

     [{"Test query without aggregations",
       fun query_1/0},
      {"Test an aggregation query",
       fun query_2/0},
      {"Test query on event sequence (event pattern matching)",
       fun pattern/0}
     ]
    }.

query_1() ->
    {ok,SubPid} = result_subscriber:start_link(),

    QueryStr = "define correlation1 as
                     select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
                     from event1 as ev1, event2 as ev2
                     where ev1.eventparam2 = ev2.eventparam2
                     within 60 seconds; ",
    Mod = application:get_env(rivus_cep, rivus_window_provider, rivus_cep_slide),
    {ok, Pid} = rivus_cep_window:start_link(Mod),        
    
    Window = rivus_cep_window:new(Pid, slide, 60),
    {ok, Tokens, Endline} = rivus_cep_scanner:string(QueryStr, 1),   
    {ok, QueryClauses} = rivus_cep_parser:parse(Tokens),    
    {ok, QueryPid} = rivus_cep_query_worker:start_link(#query_details{
							  clauses = QueryClauses,
							  producers = [test_query_1],
							  subscribers = [SubPid],
							  options = [],
							  event_window = Window,
							  event_window_pid = Pid,
							  fsm_window = nil,
							  window_register = nil}),
    
    Event1 = {event1, 10,b,c}, 
    Event2 = {event1, 15,bbb,c},
    Event3 = {event1, 20,b,c},
    Event4 = {event2, 30,b,cc,d},
    Event5 = {event2, 40,bb,cc,dd},
   
    gproc:send({p, l, {test_query_1, element(1, Event1)}}, Event1),
    gproc:send({p, l, {test_query_1, element(1, Event2)}}, Event2),
    gproc:send({p, l, {test_query_1, element(1, Event3)}}, Event3),
    gproc:send({p, l, {test_query_1, element(1, Event4)}}, Event4),
    gproc:send({p, l, {test_query_1, element(1, Event5)}}, Event5),

    timer:sleep(2000),
    
    {ok,Values} = gen_server:call(SubPid, get_result),
    %% ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    %%?assertEqual([{10,b,cc,b},{20,b,cc,b}], Values),
    ?assertEqual(2, length(Values)),
    ?assert(lists:any(fun(T) -> T == {10,b,cc,b} end, Values)),
    ?assert(lists:any(fun(T) -> T == {20,b,cc,b} end, Values)),
    
    gen_server:call(QueryPid,stop),
    gen_server:call(SubPid,stop).

query_2()->
    {ok, SubPid} = result_subscriber:start_link(),
    
    QueryStr = "define correlation2 as
                  select ev1.eventparam1, ev2.eventparam2, sum(ev2.eventparam3) 
                  from event1 as ev1, event2 as ev2
                   where ev1.eventparam2 = ev2.eventparam2
                    within 60 seconds; ",

    Mod = application:get_env(rivus_cep, rivus_window_provider, rivus_cep_slide),
    {ok, Pid} = rivus_cep_window:start_link(Mod),        
    
    Window = rivus_cep_window:new(Pid, slide, 60),

    {ok, Tokens, Endline} = rivus_cep_scanner:string(QueryStr, 1),   
    {ok, QueryClauses} = rivus_cep_parser:parse(Tokens),    
    {ok, QueryPid} = rivus_cep_query_worker:start_link(#query_details{
							  clauses = QueryClauses,
							  producers = [test_query_2],
							  subscribers = [SubPid],
							  options = [],
							  event_window = Window,
							  event_window_pid = Pid,
							  fsm_window = nil,
							  window_register = nil}),
    
    %% send some events
    Event1 = {event1, gr1,b,10}, 
    Event2 = {event1, gr2,bbb,20},
    Event3 = {event1, gr3,b,30},
    Event4 = {event2, gr1,b,40,d},
    Event5 = {event2, gr2,bb,50,dd},
    Event6 = {event2, gr3,b,40,d},

    gproc:send({p, l, {test_query_2, element(1, Event1)}}, Event1),
    gproc:send({p, l, {test_query_2, element(1, Event2)}}, Event2),
    gproc:send({p, l, {test_query_2, element(1, Event3)}}, Event3),
    gproc:send({p, l, {test_query_2, element(1, Event4)}}, Event4),
    gproc:send({p, l, {test_query_2, element(1, Event5)}}, Event5),
    gproc:send({p, l, {test_query_2, element(1, Event6)}}, Event6),

    timer:sleep(2000),
    {ok,Values} = gen_server:call(SubPid, get_result),
    %% ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    %%?assertEqual([{gr1,b,80},{gr3,b,80}], Values),
    ?assertEqual(2, length(Values)),
    ?assert(lists:any(fun(T) -> T == {gr1,b,80} end, Values)),
    ?assert(lists:any(fun(T) -> T == {gr3,b,80} end, Values)),
    
    gen_server:call(QueryPid,stop),
    gen_server:call(SubPid,stop).


pattern() ->
    {ok, SubPid} = result_subscriber:start_link(),
    
    QueryStr = "define pattern1 as
                      select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev2.eventparam4
                      from event1 as ev1 -> event2 as ev2
                      where ev1.eventparam2 = ev2.eventparam2
                      within 60 seconds; ",
    Mod = application:get_env(rivus_cep, rivus_window_provider, rivus_cep_slide),
    {ok, Pid1} = rivus_cep_window:start_link(Mod),        
    {ok, Pid2} = rivus_cep_window:start_link(Mod),        
    
    Window = rivus_cep_window:new(Pid1, slide, 60),
    FsmWindow = rivus_cep_window:new(Pid2, 60),

    {ok, Tokens, _Endline} = rivus_cep_scanner:string(QueryStr, 1),   
    {ok, QueryClauses} = rivus_cep_parser:parse(Tokens),    
    {ok, QueryPid} = rivus_cep_query_worker:start_link(#query_details{
							  clauses = QueryClauses,
							  producers = [test_pattern_1],
							  subscribers = [SubPid],
							  options = [],
							  event_window = Window,
							  event_window_pid = Pid1,
							  fsm_window = FsmWindow,
							  fsm_window_pid = Pid2,
							  window_register = nil}),

    Event1 = {event1, 10,b,10}, 
    Event2 = {event1, 15,bbb,20},
    Event3 = {event1, 20,b,10},
    Event4 = {event2, 30,b,100,20},
    Event5 = {event2, 40,bb,200,30},
   
    gproc:send({p, l, {test_pattern_1, element(1, Event1)}}, Event1),
    gproc:send({p, l, {test_pattern_1, element(1, Event2)}}, Event2),
    gproc:send({p, l, {test_pattern_1, element(1, Event3)}}, Event3),
    gproc:send({p, l, {test_pattern_1, element(1, Event4)}}, Event4),
    gproc:send({p, l, {test_pattern_1, element(1, Event5)}}, Event5),

    timer:sleep(2000),       
    
    {ok,Values} = gen_server:call(SubPid, get_result),
    ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    ?assertEqual([{10,b,100,20},{20,b,100,20}], Values),
    gen_server:call(QueryPid,stop),
    gen_server:call(SubPid,stop).
