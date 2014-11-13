-module(rivus_cep_server_tests).

-compile([debug_info, export_all]).
-compile([{parse_transform, lager_transform}]).

-include_lib("eunit/include/eunit.hrl").


query_worker_test_() ->
    {setup,
        fun() ->
            folsom:start(),
            lager:start(),
            application:start(gproc),
            lager:set_loglevel(lager_console_backend, debug),
            application:set_env(rivus_cep, rivus_window_provider, rivus_cep_window_ets),
            application:set_env(rivus_cep, rivus_tcp_serv, {"127.0.0.1", 5775}),
            ok = application:start(rivus_cep)
        end,
        fun(_) ->
            folsom:stop(),
            application:stop(lager),
            application:stop(gproc),
            application:stop(rivus_cep)
        end,
        [
            {"Test query via TCP connection",
                timeout, 60 * 60,
                 fun load_query_tcp/0},
            {"Test event module load via TCP connection",
                timeout, 60 * 60,
                fun load_event_tcp/0},
            {"Test query and event module load via TCP connection",
                timeout, 60 * 60,
               fun load_query_and_event/0}
        ]
    }.


load_query_tcp() ->

    {ok, Pid} = result_subscriber:start_link(),
    {ok, {Host, Port}} = application:get_env(rivus_cep, rivus_tcp_serv),
    {ok, Socket} = gen_tcp:connect(Host, Port, [{active, false}, {nodelay, true}, {packet, 4}, binary]),

    QueryStr = "define correlation1 as
                     select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
                     from event1 as ev1, event2 as ev2
                     where ev1.eventparam2 = ev2.eventparam2
                     within 60 seconds; ",

    ?assertEqual(ok, gen_tcp:send(Socket, term_to_binary({load_query, {QueryStr, [test_query_1], [Pid], []}}))),
    Result = gen_tcp:recv(Socket, 0),
    ?assertMatch({ok, _}, Result),
    QueryPid = binary_to_term(element(2, Result)),

    Event1 = {event1, 10, b, c},
    Event2 = {event1, 15, bbb, c},
    Event3 = {event1, 20, b, c},
    Event4 = {event2, 30, b, cc, d},
    Event5 = {event2, 40, bb, cc, dd},

    gen_tcp:send(Socket, term_to_binary({event, test_query_1, Event1})),
    gen_tcp:send(Socket, term_to_binary({event, test_query_1, Event2})),
    gen_tcp:send(Socket, term_to_binary({event, test_query_1, Event3})),
    gen_tcp:send(Socket, term_to_binary({event, test_query_1, Event4})),
    gen_tcp:send(Socket, term_to_binary({event, test_query_1, Event5})),

    timer:sleep(3000),

    {ok, Values} = gen_server:call(Pid, get_result),
    ?debugMsg(io_lib:format("Values: ~p~n", [Values])),
    %%?assertEqual([{10, b, cc, b}, {20, b, cc, b}], Values),
    ?assertEqual(2, length(Values)),
    ?assert(lists:any(fun(T) -> T == {10, b, cc, b} end, Values)),
    ?assert(lists:any(fun(T) -> T == {20, b, cc, b} end, Values)),

    gen_server:call(QueryPid, stop),
    gen_server:call(Pid, stop).

load_event_tcp() ->
    EventDefStr = "define event11 as (attr1, attr2, attr3, attr4);",
    {ok, {Host, Port}} = application:get_env(rivus_cep, rivus_tcp_serv),
    {ok, Socket} = gen_tcp:connect(Host, Port, [{active, false}, {nodelay, true}, {packet, 4}, binary]),
    ?assertEqual(ok, gen_tcp:send(Socket, term_to_binary({load_query, {EventDefStr, [], [], []}}))),
    Result = gen_tcp:recv(Socket, 0),
    ?assertMatch({ok, _}, Result),
    QueryPid = binary_to_term(element(2, Result)).



load_query_and_event() ->
    EventDefStr = "define event11 as (attr1, attr2, attr3, attr4);",
    Query = "define correlation1 as
                                select sum(ev1.attr1)
                                from event11 as ev1
                                within 60 seconds; ",
    {ok, {Host, Port}} = application:get_env(rivus_cep, rivus_tcp_serv),
    {ok, Socket} = gen_tcp:connect(Host, Port, [{active, false}, {nodelay, true}, {packet, 4}, binary]),
    ok = gen_tcp:send(Socket, term_to_binary({load_query, {EventDefStr, [benchmark_event], [], []}})),
    ?assertMatch({ok, _}, gen_tcp:recv(Socket, 0)),


    ok = gen_tcp:send(Socket, term_to_binary({load_query, {Query, [benchmark_test], [], []}})),

    ?assertMatch({ok, _}, gen_tcp:recv(Socket, 0)).

%% {ok, {Host, Port}} = application:get_env(rivus_cep, rivus_tcp_serv).
%% {ok, Socket} = gen_tcp:connect(Host, Port, [{active, false}, {nodelay, true}, {packet, 4}, binary]).
%%
%%
%% Event1 = {event11, 10,b,c}.
%% Event2 = {event11, 15,bbb,c}.
%% Event3 = {event11, 20,b,c}.
%% Event4 = {event11, 30,b,cc,d}.
%% Event5 = {event11, 40,bb,cc,dd}.
%%
%% rivus_cep:notify(test1, Event1).
%% rivus_cep:notify(test1, Event2).
%% rivus_cep:notify(test1, Event3).
%% rivus_cep:notify(test1, Event4).
%% rivus_cep:notify(test1, Event5).
%%
%%
%% {ok,Pid} = result_subscriber:start_link().
%% EventDefStr = "define event11 as (attr1, attr2, attr3, attr4);".
%% QueryStr = "define correlation1 as
%%                      select sum(ev1.attr1)
%%                      from event11 as ev1
%%                      within 60 seconds; ".
%% ok = rivus_cep:execute(EventDefStr).
%% {ok, QueryPid, _} = rivus_cep:execute(QueryStr, [test1], [Pid], []).
%%
%%
%%
%% gen_tcp:send(Socket, term_to_binary({event, test1, Event1})).
%% gen_tcp:send(Socket, term_to_binary({event, test1, Event2})).
%% gen_tcp:send(Socket, term_to_binary({event, test1, Event3})).
%% gen_tcp:send(Socket, term_to_binary({event, test1, Event4})).
%% gen_tcp:send(Socket, term_to_binary({event, test1, Event5})).
