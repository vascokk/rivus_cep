-module(rivus_cep_tests).

-compile([debug_info, export_all]).
-compile([{parse_transform, lager_transform}]).

-include_lib("eunit/include/eunit.hrl").


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
	     application:stop(rivus_cep)
     end,

     [{"Test query without aggregations",
       fun load_query_1/0},
      {"Test query with aggregation",
       fun load_query_2/0},
      {"Tes query on event sequence (event pattern matching)",
       fun load_pattern/0},
       {"Tes query without producers",
       fun load_query_no_producers/0}
     ]
    }.

shared_streams_test_() ->
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
	     application:stop(rivus_cep)
     end,

     [{"Test shared streams query without aggregations",
        fun shared_streams_1/0},
      {"Test shared streams query with aggregation",
        fun shared_streams_2/0}
     ]
    }.

load_query_1() ->
    {ok,Pid} = result_subscriber:start_link(),

    QueryStr = "define correlation1 as
                     select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
                     from event1 as ev1, event2 as ev2
                     where ev1.eventparam2 = ev2.eventparam2
                     within 60 seconds; ",
    
    {ok, QueryPid} = rivus_cep:load_query(QueryStr, [test_query_1], [Pid], []),
    
    Event1 = {event1, 10,b,c}, 
    Event2 = {event1, 15,bbb,c},
    Event3 = {event1, 20,b,c},
    Event4 = {event2, 30,b,cc,d},
    Event5 = {event2, 40,bb,cc,dd},

    rivus_cep:notify(test_query_1, Event1),
    rivus_cep:notify(test_query_1, Event2),
    rivus_cep:notify(test_query_1, Event3),
    rivus_cep:notify(test_query_1, Event4),
    rivus_cep:notify(test_query_1, Event5),

    timer:sleep(2000),
    
    {ok,Values} = gen_server:call(Pid, get_result),
    %% ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    %%?assertEqual([{10,b,cc,b},{20,b,cc,b}], Values),
    ?assertEqual(2, length(Values)),
    ?assert(lists:any(fun(T) -> T == {10,b,cc,b} end, Values)),
    ?assert(lists:any(fun(T) -> T == {20,b,cc,b} end, Values)),
    
    gen_server:call(QueryPid,stop),
    gen_server:call(Pid,stop).
    
load_query_2() ->
    {ok, Pid} = result_subscriber:start_link(),
    
    QueryStr = "define correlation2 as
                  select ev1.eventparam1, ev2.eventparam2, sum(ev2.eventparam3) 
                  from event1 as ev1, event2 as ev2
                   where ev1.eventparam2 = ev2.eventparam2
                    within 60 seconds; ",
    
    {ok, QueryPid} = rivus_cep:load_query(QueryStr, [test_query_2], [Pid], []),
    
    %% send some events
    Event1 = {event1, gr1,b,10}, 
    Event2 = {event1, gr2,bbb,20},
    Event3 = {event1, gr3,b,30},
    Event4 = {event2, gr1,b,40,d},
    Event5 = {event2, gr2,bb,50,dd},
    Event6 = {event2, gr3,b,40,d},

    rivus_cep:notify(test_query_2, Event1),
    rivus_cep:notify(test_query_2, Event2),
    rivus_cep:notify(test_query_2, Event3),
    rivus_cep:notify(test_query_2, Event4),
    rivus_cep:notify(test_query_2, Event5),
    rivus_cep:notify(test_query_2, Event6),

    timer:sleep(2000),
    {ok,Values} = gen_server:call(Pid, get_result),
    %% ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    %%?assertEqual([{gr1,b,80},{gr3,b,80}], Values),
    ?assertEqual(2, length(Values)),
    ?assert(lists:any(fun(T) -> T == {gr1,b,80} end, Values)),
    ?assert(lists:any(fun(T) -> T == {gr3,b,80} end, Values)),

    gen_server:call(QueryPid,stop),
    gen_server:call(Pid,stop).


load_pattern() ->    
    {ok, Pid} = result_subscriber:start_link(),
    
    QueryStr = "define pattern1 as
                      select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev2.eventparam4
                      from event1 as ev1 -> event2 as ev2
                      where ev1.eventparam2 = ev2.eventparam2
                      within 60 seconds; ",
    
    {ok, QueryPid} = rivus_cep:load_query(QueryStr, [test_pattern_1], [Pid], []),
    
    Event1 = {event1, 10,b,10}, 
    Event2 = {event1, 15,bbb,20},
    Event3 = {event1, 20,b,10},
    Event4 = {event2, 30,b,100,20},
    Event5 = {event2, 40,bb,200,30},
  
    rivus_cep:notify(test_pattern_1, Event1),
    rivus_cep:notify(test_pattern_1, Event2),
    rivus_cep:notify(test_pattern_1, Event3),
    rivus_cep:notify(test_pattern_1, Event4),
    rivus_cep:notify(test_pattern_1, Event5),
    
    timer:sleep(2000),       
    
    {ok,Values} = gen_server:call(Pid, get_result),
    %% ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    ?assertEqual([{20,b,100,20}], Values),
    gen_server:call(QueryPid,stop),
    gen_server:call(Pid,stop).


load_query_no_producers() ->
    {ok,Pid} = result_subscriber:start_link(),

    QueryStr = "define noproducers_query as
                     select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
                     from event1 as ev1, event2 as ev2
                     where ev1.eventparam2 = ev2.eventparam2
                     within 60 seconds; ",
    
    {ok, QueryPid} = rivus_cep:load_query(QueryStr, [any], [Pid], []),
    
    Event1 = {event1, 10,b,c}, 
    Event2 = {event1, 15,bbb,c},
    Event3 = {event1, 20,b,c},
    Event4 = {event2, 30,b,cc,d},
    Event5 = {event2, 40,bb,cc,dd},

    rivus_cep:notify(Event1),
    rivus_cep:notify(Event2),
    rivus_cep:notify(Event3),
    rivus_cep:notify(Event4),
    rivus_cep:notify(Event5),

    timer:sleep(2000),
    
    {ok,Values} = gen_server:call(Pid, get_result),
    %% ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    %%?assertEqual([{10,b,cc,b},{20,b,cc,b}], Values),
    ?assertEqual(2, length(Values)),
    ?assert(lists:any(fun(T) -> T == {10,b,cc,b} end, Values)),
    ?assert(lists:any(fun(T) -> T == {20,b,cc,b} end, Values)),
    
    gen_server:call(QueryPid,stop),
    gen_server:call(Pid,stop).

shared_streams_1() ->
    {ok,Pid} = result_subscriber:start_link(),

    QueryStr = "define sharedstreams1 as
                     select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
                     from event1 as ev1, event2 as ev2
                     where ev1.eventparam2 = ev2.eventparam2
                     within 60 seconds; ",
    
    {ok, QueryPid} = rivus_cep:load_query(QueryStr, [test_sharedstreams_1], [Pid], [{shared_streams,true}]),
    
    Event1 = {event1, 10,b,10}, 
    Event2 = {event1, 15,bbb,20},
    Event3 = {event1, 20,b,30},
    Event4 = {event2, 30,b,50,d},
    Event5 = {event2, 40,bb,100,dd},

    rivus_cep:notify(test_sharedstreams_1, Event1),
    rivus_cep:notify(test_sharedstreams_1, Event2),
    rivus_cep:notify(test_sharedstreams_1, Event3),
    rivus_cep:notify(test_sharedstreams_1, Event4),
    rivus_cep:notify(test_sharedstreams_1, Event5),

    timer:sleep(2000),
    
    {ok,Values} = gen_server:call(Pid, get_result),
    %% ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    %%?assertEqual([{10,b,50,b},{20,b,50,b}], Values),
    ?assertEqual(2, length(Values)),
    ?assert(lists:any(fun(T) -> T == {10,b,50,b} end, Values)),
    ?assert(lists:any(fun(T) -> T == {20,b,50,b} end, Values)),
    
    gen_server:call(QueryPid,stop),
    gen_server:call(Pid,stop).

shared_streams_2() ->
    {ok, Pid} = result_subscriber:start_link(),
    
    QueryStr = "define sharedstreams2 as
                  select ev1.eventparam1, ev2.eventparam2, sum(ev2.eventparam3) 
                  from event1 as ev1, event2 as ev2
                   where ev1.eventparam2 = ev2.eventparam2
                    within 60 seconds; ",
    
    {ok, QueryPid} = rivus_cep:load_query(QueryStr, [test_sharedstreams_2], [Pid], [{shared_streams,true}]),
    
    %% send some events
    Event1 = {event1, gr1,b,10}, 
    Event2 = {event1, gr2,bbb,20},
    Event3 = {event1, gr3,b,30},
    Event4 = {event2, gr1,b,40,d},
    Event5 = {event2, gr2,bb,50,dd},
    Event6 = {event2, gr3,b,40,d},

    rivus_cep:notify(test_sharedstreams_2, Event1),
    rivus_cep:notify(test_sharedstreams_2, Event2),
    rivus_cep:notify(test_sharedstreams_2, Event3),
    rivus_cep:notify(test_sharedstreams_2, Event4),
    rivus_cep:notify(test_sharedstreams_2, Event5),
    rivus_cep:notify(test_sharedstreams_2, Event6),

    timer:sleep(2000),
    {ok,Values} = gen_server:call(Pid, get_result),
    %%result will includes the events generated in the previous test 
    %%?assertEqual([{20,b,130},{10,b,130},{gr1,b,130},{gr3,b,130}], Values),
    ?assertEqual(4, length(Values)),
    ?assert(lists:any(fun(T) -> T == {20,b,130} end, Values)),
    ?assert(lists:any(fun(T) -> T == {10,b,130} end, Values)),
    ?assert(lists:any(fun(T) -> T == {gr1,b,130} end, Values)),
    ?assert(lists:any(fun(T) -> T == {gr3,b,130} end, Values)),
	    
    gen_server:call(QueryPid,stop),
    gen_server:call(Pid,stop).
