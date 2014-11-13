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

-module(rivus_cep).
-behaviour(gen_server).
-compile([{parse_transform, lager_transform}]).

%% API
-export([start_link/0,
    start_link/1,
    execute/1,
    execute/2,
    execute/4,
    execute/5,
    load_query/4,
    notify/1,
    notify/2,
    notify/3,
    notify_sync/1,
    notify_sync/2,
    notify_sync/3,
    get_query_details/4]).

%% gen_server API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-include("rivus_cep.hrl").
-include_lib("folsom/include/folsom.hrl").

-record(state, {query_sup,
    clock_sup,
    win_register = dict:new()}
).

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------
start_link() ->
    SrvId = erlang:phash2({node(), now()}),
    gen_server:start_link({local, list_to_atom(integer_to_list(SrvId))}, ?MODULE, [], []).

start_link(standalone) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

load_query(QueryStr, Producers, Subscribers, Options) ->
    gen_server:call(?SERVER, {load_query, [QueryStr, Producers, Subscribers, Options]}).

execute(QueryStr) ->
    gen_server:call(?SERVER, {execute, [QueryStr, [], [], []]}).

execute(Pid, QueryStr) ->
    gen_server:call(Pid, {execute, [QueryStr, [], [], []]}).

execute(QueryStr, Producers, Subscribers, Options) ->
    gen_server:call(?SERVER, {execute, [QueryStr, Producers, Subscribers, Options]}).

execute(Pid, QueryStr, Producers, Subscribers, Options) ->
    gen_server:call(Pid, {execute, [QueryStr, Producers, Subscribers, Options]}).

get_query_details(QueryStr, Producers, Subscribers, Options) ->
    gen_server:call(?SERVER, {get_query_details, [QueryStr, Producers, Subscribers, Options]}).

notify(Event) ->
    notify(any, Event).

notify(Pid, Event) when is_pid(Pid) ->
    notify(Pid, any, Event);
notify(Producer, Event) ->
    gen_server:cast(?SERVER, {notify, Producer, Event}).

notify(Pid, Producer, Event) ->
    gen_server:cast(Pid, {notify, Producer, Event}).



notify_sync(Event) ->
    notify_sync(any, Event).

notify_sync(Pid, Event) when is_pid(Pid) ->
    notify_sync(Pid, any, Event);
notify_sync(Producer, Event) ->
    gen_server:call(?SERVER, {notify, Producer, Event}).

notify_sync(Pid, Producer, Event) ->
    gen_server:call(Pid, {notify, Producer, Event}).


init([]) ->
    {ok, QuerySup} = rivus_cep_app_srv:get_query_sup(),
    {ok, ClockSup} = rivus_cep_app_srv:get_clock_sup(),
    {ok, #state{query_sup = QuerySup, clock_sup = ClockSup}}.


%%--------------------------------------------------------------------
%% gen_server functions
%%--------------------------------------------------------------------
handle_cast({notify, Producer, Event}, #state{win_register = WinReg} = State) ->
    EventName = element(1, Event),
    gproc:send({p, l, {Producer, EventName}}, Event),
    case dict:is_key(EventName, WinReg) of
        true -> Window = dict:fetch(EventName, WinReg),
            lager:debug("Updating global window: ~p~n", [Window]),
            rivus_cep_window:update(Window, Event),
            gproc:send({p, l, {Producer, EventName, global}}, Event);
        false -> ok
    end,
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.


handle_call({load_query, [QueryStr, _Producers, Subscribers, Options]}, _From, State) ->

    WinReg = State#state.win_register,
    QuerySup = State#state.query_sup,

    QueryDetails = get_query_details([QueryStr, _Producers, Subscribers, Options], WinReg),

    lager:debug("Query sup, Args: ~p~n", [QueryDetails]),

    {ok, Pid} = supervisor:start_child(QuerySup, [QueryDetails]),
    {reply, {ok, Pid, QueryDetails}, State#state{win_register = QueryDetails#query_details.window_register}};
handle_call({execute, [QueryStr, _Producers, Subscribers, Options]}, _From, State) ->

    WinReg = State#state.win_register,
    QuerySup = State#state.query_sup,

    QueryDetails = get_query_details([QueryStr, _Producers, Subscribers, Options], WinReg),

    case QueryDetails of
        {module, _} -> {reply, ok, State};
        _ -> lager:info("Query sup, Args: ~p~n", [QueryDetails]),
            {ok, Pid} = supervisor:start_child(QuerySup, [QueryDetails]),
            {reply, {ok, Pid, QueryDetails}, State#state{win_register = QueryDetails#query_details.window_register}}
    end;
handle_call({get_query_details, [QueryStr, _Producers, Subscribers, Options]}, _From, #state{win_register = WinReg} = State) ->
    QueryDetails = get_query_details([QueryStr, _Producers, Subscribers, Options], WinReg),
    {reply, {ok, QueryDetails}, State#state{win_register = QueryDetails#query_details.window_register}};
handle_call({notify, Producer, Event}, _From, #state{win_register = WinReg} = State) ->
    EventName = element(1, Event),
    gproc:send({p, l, {Producer, EventName}}, Event),
    case dict:is_key(EventName, WinReg) of
        true -> Window = dict:fetch(EventName, WinReg),
            rivus_cep_window:update(Window, Event),
            gproc:send({p, l, {Producer, EventName, global}}, Event);
        false -> ok
    end,
    {reply, ok, State};
handle_call(_Msg, _From, State) ->
    {reply, not_handled, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
get_query_details([QueryStr, _Producers, Subscribers, Options], WinReg) ->
    QueryClauses = parse_query(QueryStr),

    case QueryClauses of
        {event, EventDef} -> rivus_cep_event_creator:load_event_mod(EventDef);
        _ -> Producers = case _Producers of
                             [] -> [any];
                             _ -> _Producers
                         end,

            {{EventWindow, EvWinPid}, {FsmWindow, FsmWinPid}, NewWinReg} = register_windows(QueryClauses, Options, WinReg),

            #query_details{
                clauses          = QueryClauses,
                producers        = Producers,
                subscribers      = Subscribers,
                options          = Options,
                event_window     = EventWindow,
                fsm_window       = FsmWindow,
                window_register  = NewWinReg,
                event_window_pid = EvWinPid,
                fsm_window_pid   = FsmWinPid
            }
    end.

parse_query(QueryStr) ->
    {ok, Tokens, _} = rivus_cep_scanner:string(QueryStr, 1),
    {ok, QueryClauses} = rivus_cep_parser:parse(Tokens),
    QueryClauses.

register_windows([_StmtName, _SelectClause, FromClause, _WhereClause, WithinClause, {_Filters}], Options, WinReg) ->
    Mod = application:get_env(rivus_cep, rivus_window_provider, rivus_cep_window_ets), %%TODO: to be passed as parameter to the func
    %%{ok, EvWinPid} = rivus_cep_window:start_link(Mod),

    {QueryType, Events} = case FromClause of
                              {pattern, {List}} -> {pattern, List};
                              {List} -> {simple, List}
                          end,
    SharedStreams = proplists:get_value(shared_streams, Options, false),
    case {QueryType, SharedStreams} of
        {pattern, _} -> {ok, EvWinPid} = rivus_cep_window:start_link(Mod),
            {ok, FsmWinPid} = rivus_cep_window:start_link(Mod),
            {{register_local_window(WithinClause, EvWinPid), EvWinPid},
                {register_local_window(WithinClause, FsmWinPid), FsmWinPid},
                WinReg};
        {simple, true} -> {{global, nil}, {nil, nil}, register_global_windows(Events, WithinClause, WinReg)};
        _ -> {ok, EvWinPid} = rivus_cep_window:start_link(Mod),
            {{register_local_window(WithinClause, EvWinPid), EvWinPid}, {nil, nil}, WinReg}
    end.

register_local_window({Within, _WindowsType}, Pid) ->
    rivus_cep_window:new(Pid, slide, Within).

register_global_windows(Events, WithinClause, WinReg) ->
    NewWinReg = lists:foldl(fun(Event, Register) -> case dict:is_key(Event, Register) of
                                                        true -> maybe_update_window_size(Event, Register, WithinClause);
                                                        false -> create_new_global_window(Event, Register, WithinClause)
                                                    end
    end, WinReg, Events),
    lager:debug("Windows Register: ~p~n", [NewWinReg]),
    NewWinReg.

maybe_update_window_size(Event, WinReg, {Within, _WindowsType}) ->
    Window = dict:fetch(Event, WinReg),
    Size = Window#slide.window,
    case Size < Within of
        true -> NewWindow = rivus_cep_window:resize(Window, Within),
            dict:store(Event, NewWindow, WinReg);
        false -> WinReg
    end.


create_new_global_window(Event, WinReg, {Within, _WindowType}) ->
    %% Mod = application:get_env(rivus_cep, rivus_window_provider, rivus_cep_window_ets), %%TODO: to be passed as parameter to the func
    %% {ok, EvWinPid} = rivus_cep_window:start_link(Mod),
    Window = rivus_cep_window:new(slide, Within),
    dict:store(Event, Window, WinReg).
