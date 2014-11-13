%%%-------------------------------------------------------------------
%%% @author vasco
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Nov 2014 10:08 PM
%%%-------------------------------------------------------------------
-module(rivus_cep_app_srv).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

%% API
-export([start_link/1,
    get_query_sup/0,
    get_clock_sup/0,
    get_cep_sup/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    app_srv_sup,
    query_sup,
    clock_sup,
    cep_sup
}).

%%%===================================================================
%%% API
%%%===================================================================
get_query_sup() ->
    gen_server:call(?SERVER, get_query_sup).

get_clock_sup() ->
    gen_server:call(?SERVER, get_clock_sup).

get_cep_sup() ->
    gen_server:call(?SERVER, get_cep_sup).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
start_link(Sup) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Sup], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Sup]) ->
    QuerySupSpec = {query_worker_sup,
        {rivus_cep_query_worker_sup, start_link, []},
        permanent,
        10000,
        supervisor,
        [rivus_cep_query_worker_sup]},
    BatchClockSupSpec = {batch_clock_sup,
        {rivus_cep_clock_sup, start_link, []},
        permanent,
        10000,
        supervisor,
        [rivus_cep_clock_sup]},
    CepSupSpeck = {cep_worker_sup,
        {rivus_cep_sup, start_link, []},
        permanent,
        10000,
        supervisor,
        [rivus_cep_sup]},
    self() ! {start_query_supervisor, Sup, QuerySupSpec},
    self() ! {start_batch_clock_supervisor, Sup, BatchClockSupSpec},
    self() ! {start_cep_worker_supervisor, Sup, CepSupSpeck},
    lager:info("--- Rivus CEP server started"),
    {ok, #state{app_srv_sup = Sup}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
handle_call(get_query_sup, _From, #state{query_sup = Sup} = State) ->
    {reply, {ok, Sup}, State};
handle_call(get_clock_sup, _From, #state{clock_sup = Sup} = State) ->
    {reply, {ok, Sup}, State};
handle_call(get_cep_sup, _From, #state{cep_sup = Sup} = State) ->
    {reply, {ok, Sup}, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({start_query_supervisor, Sup, QuerySupSpec}, State) ->
    lager:info("----> Start Query Sup, Args: ~p,   ~p~n", [Sup, QuerySupSpec]),
    {ok, Pid} = supervisor:start_child(Sup, QuerySupSpec),
    link(Pid),
    {noreply, State#state{query_sup = Pid}};
handle_info({start_batch_clock_supervisor, Sup, BatchClockSupSpec}, State) ->
    {ok, Pid} = supervisor:start_child(Sup, BatchClockSupSpec),
    link(Pid),
    {noreply, State#state{clock_sup = Pid}};
handle_info({start_cep_worker_supervisor, Sup, CepSupSpeck}, State) ->
    {ok, Pid} = supervisor:start_child(Sup, CepSupSpeck),
    link(Pid),
    {noreply, State#state{cep_sup = Pid}};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
