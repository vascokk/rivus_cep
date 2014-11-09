-module(basho_bench_driver_rivus).
-compile([{parse_transform, lager_transform}]).

-export([new/1, run/4]).

-include("basho_bench.hrl").

-record(state, {
    host,
    port,
    socket,
    client_num
}).

new(Id) ->
    {Host, Port, Socket} = connect(),
    deploy_queries(Socket, Id),

    {ok, #state{
        host   = Host,
        port   = Port,
        socket = Socket,
        client_num = Id}}.
%% new(_Id) ->
%%     {Host, Port, Socket} = connect(),
%%
%%     {ok, #state{
%%         host   = Host,
%%         port   = Port,
%%         socket = Socket}}.

connect() ->
    {Host, Port} = basho_bench_config:get(rivus_cep_tcp_serv),
    {ok, Socket} = gen_tcp:connect(Host, Port, [{active, false}, {nodelay, true}, {packet, 4}, binary]),
    {Host, Port, Socket}.

deploy_queries(Socket, Id) ->
    Queries = basho_bench_config:get(rivus_cep_queries, []),
    lists:foreach(fun({Type, Query}) ->
        Q = re:replace(Query,"\\$",integer_to_list(Id),[global, {return,list}]),
        lager:info("Deploying query type: ~p, client: ~p, stmt: ~p", [Type, Id, Q]),
        execute(Type, Q, Socket) end, Queries).

execute(query, Query, Socket) ->
    ok = gen_tcp:send(Socket, term_to_binary({load_query, {Query, [benchmark_test], [], []}}));
execute(event, Query, Socket) ->
    ok = gen_tcp:send(Socket, term_to_binary({load_query, {Query, [benchmark_event], [], []}})).

send_event(Event, #state{socket = Socket} = State) ->
    case gen_tcp:send(Socket, term_to_binary({event, benchmark_test, Event})) of
        ok -> {ok, State};
        Error -> Error
    end.

create_event(_KeyGen, ValueGen) ->
    ValueGen.

run(notify, KeyGen, ValueGen, #state{socket = Socket} = State) ->
    Event = create_event(KeyGen, ValueGen),
    % Send message
    case send_event(Event, State) of
        {error, E} ->
            {error, E, State};
        {ok, State} -> case gen_tcp:recv(Socket,0) of
                              {ok, _} -> {ok, State};
                           {error, E} -> {error, E, State}
                       end
    end.
