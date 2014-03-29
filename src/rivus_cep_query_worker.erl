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
-export([start_link/1]).


%%% API functions

start_link(QueryDetails) ->
    {QueryName} = hd(QueryDetails#query_details.clauses),
    gen_server:start_link( {local, QueryName}, ?MODULE, [QueryDetails], []).

init([QueryDetails]) ->
    {ok, State} = rivus_cep_query:init([QueryDetails]), 
    {ok, State}.



%%----------------------------------------------------------------------------------------------
%% gen_server functions
%%----------------------------------------------------------------------------------------------
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};	
handle_call(_Request, _From, State) ->
    Reply = {ok, notsupported} ,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Event, #query_state{query_type = QueryType} = State) when QueryType == simple->
    lager:debug("handle_info, query_type: simple,  Event: ~p",[Event]),

    Result = rivus_cep_query:process_event(Event, State),
    case Result of
    	nil -> nil;
    	_ -> [gproc:send({p, l, {Subscriber, result_subscribers}}, Result) || Subscriber<-State#query_state.subscribers]
    end,
    {noreply, State};
handle_info(Event, #query_state{query_type = QueryType} = State) when QueryType == pattern ->
    Result = rivus_cep_query:process_event(Event, State),
    case Result of
    	[] -> [];
    	_ -> [gproc:send({p, l, {Subscriber, result_subscribers}}, Result) || Subscriber<-State#query_state.subscribers]
    end,
    {noreply, State}; 
handle_info(Info, State) ->
    lager:debug("Statement: ~p,  handle_info got event: ~p. Will do nothing ...",[ State#query_state.query_name ,Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    lager:debug("Worker stopped. Reason: ~p~n",[_Reason]),
    ok.

code_change(_OldVsn, _State, _Extra) ->
    {ok, _State}.
