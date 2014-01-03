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

-export([start_link/1, load_query/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {query_sup}).

start_link(Supervisor) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Supervisor], []).

init([Supervisor]) ->
    QuerySupSpec = {query_worker_sup,
		    {rivus_cep_query_worker_sup, start_link, []},
		    permanent,
		    10000,
		    supervisor,
		    [rivus_cep_query_worker_sup]},
    self() ! {start_query_supervisor, Supervisor, QuerySupSpec},
    lager:info("--- Rivus CEP server started"),
    {ok, #state{}}.


%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------
load_query(QueryName, QueryStr, Producers, Subscribers) ->
    gen_server:call(?SERVER, {load_query, [QueryName, QueryStr, Producers, Subscribers]}).


%%--------------------------------------------------------------------
%% gen_server functions
%%--------------------------------------------------------------------
handle_call({load_query, Args}, From, #state{query_sup=QuerySup} = State) ->
    %% Res = rivus_cep_compiler:load_query(Query, Producers, Subscribers),

    {ok, Pid} = supervisor:start_child(QuerySup, [Args]),
    {reply, {ok,Pid}, State};
handle_call(Msg, From, State) ->
    {reply, not_handled, State}.

handle_cast(_Msg, State) -> 
    {noreply, State}.

handle_info({start_query_supervisor, Supervisor, QuerySupSpec}, State) ->
    {ok, Pid} = supervisor:start_child(Supervisor, QuerySupSpec),
    link(Pid),
    {noreply, State#state{query_sup=Pid}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
