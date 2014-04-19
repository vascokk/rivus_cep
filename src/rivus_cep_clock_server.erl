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

-module(rivus_cep_clock_server).
-behaviour(gen_server).
-compile([{parse_transform, lager_transform}]).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2,
	 stop/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {query_pid, interval}).
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(QueryPid, Interval) ->
    gen_server:start_link(?MODULE, [QueryPid, Interval], []).


stop(Pid) ->
    gen_server:cast(Pid, stop).
    
%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([QueryPid, Interval]) ->
    lager:debug(" Clock Server STARTED ......~n",[]),
    {ok, #state{query_pid = QueryPid, interval = Interval}, Interval}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State=#state{query_pid = QueryPid, interval = Interval}) ->
    %%gen_server:cast(QueryPid, generate_result),
    lager:debug(" Clock Server TIMEOUT ......~n",[]),
    rivus_cep_query_worker:generate_result(QueryPid),     
    {noreply, State, Interval};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
