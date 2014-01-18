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

-export([start_link/1, load_query/4, notify/1, notify/2, notify_sync/1, notify_sync/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-include("rivus_cep.hrl").

-record(state, {query_sup,
	        win_register = dict:new()}).

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------
start_link(Supervisor) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Supervisor], []).

load_query(QueryStr, Producers, Subscribers, Options) ->
    gen_server:call(?SERVER, {load_query, [QueryStr, Producers, Subscribers, Options]}).

notify(Event) ->
    notify(any, Event).

notify(Producer, Event) ->
    gen_server:cast(?SERVER, {notify, Producer, Event}).

notify_sync(Event) ->
    notify_sync(any, Event).

notify_sync(Producer, Event) ->
    gen_server:call(?SERVER, {notify, Producer, Event}).
    

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
%% gen_server functions
%%--------------------------------------------------------------------

handle_cast({notify, Producer, Event}, #state{win_register = WinReg} = State) ->
    EventName = element(1, Event),
    gproc:send({p, l, {Producer, EventName}}, {EventName, Event}),
    case dict:is_key(EventName, WinReg) of
	true -> Window = dict:fetch(EventName, WinReg),
		lager:debug("Updating global window: ~p~n",[Window]),
		rivus_cep_window:update(Window, Event),
		gproc:send({p, l, {Producer, EventName, global}}, {EventName, Event});
	false -> ok
    end,        
    {noreply, State};
handle_cast(_Msg, State) -> 
    {noreply, State}.


handle_call({load_query, [QueryStr, _Producers, Subscribers, Options]}, From, #state{query_sup=QuerySup, win_register = WinReg} = State) ->
    QueryClauses = parse_query(QueryStr),
    Producers = case _Producers of
	[] -> [any];
	_ -> _Producers
    end,
    {QueryWindow, NewWinReg} = register_windows(QueryClauses, Producers, Options, WinReg),
    QueryModArgs = {QueryClauses, Producers, Subscribers, Options, QueryWindow, NewWinReg},
    
    lager:debug("Query sup, Args: ~p~n",[QueryModArgs]),
    
    {ok, Pid} = supervisor:start_child(QuerySup, [QueryModArgs]),
    {reply, {ok,Pid}, State#state{win_register=NewWinReg}};
handle_call({notify, Producer, Event}, From, #state{win_register = WinReg} = State) ->
    EventName = element(1, Event),
    gproc:send({p, l, {Producer, EventName}}, {EventName, Event}),
    case dict:is_key(EventName, WinReg) of
	true -> Window = dict:fetch(EventName, WinReg),
		rivus_cep_window:update(Window, Event),
		gproc:send({p, l, {Producer, EventName, global}}, {eventName, Event});
	false -> ok
    end,    
    {reply, ok, State};
handle_call(Msg, From, State) ->
    {reply, not_handled, State}.

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

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
parse_query(QueryStr) ->    
    {ok, Tokens, Endline} = rivus_cep_scanner:string(QueryStr, 1),   
    {ok, QueryClauses} = rivus_cep_parser:parse(Tokens),
    QueryClauses.

register_windows([StmtName, {SelectClause}, FromClause, {WhereClause}, {WithinClause}], Producers, Options, WinReg) ->
    {QueryType, Events} = case FromClause of
			      {pattern, {Events}} -> {pattern, Events};
			      {Events} -> {simple, Events}
			  end,    
    SharedStreams = proplists:get_value(shared_streams, Options, false),
    QueryWindow = case {QueryType, SharedStreams} of
		      {pattern,_} -> register_local_window(WithinClause, WinReg);
		      {simple, true} -> register_global_windows(Events, Producers, WithinClause, WinReg);
		      _ -> register_local_window(WithinClause, WinReg)
		  end.
    
register_local_window(WithinClause, WinReg) ->
    {rivus_cep_window:new(WithinClause), WinReg}.

register_global_windows(Events, Producers, WithinClause, WinReg) ->
    NewWinReg = lists:foldl(fun(Event, Register) -> case dict:is_key(Event, Register) of
							true -> maybe_update_window_size(Event, Register, WithinClause);
							false -> create_new_global_window(Event, Register, WithinClause)
						    end
			    end, WinReg, Events),
    lager:debug("Windows Register: ~p~n",[NewWinReg]),
    {global, NewWinReg}.

maybe_update_window_size(Event, WinReg, WithinClause) ->
    Window = dict:fetch(Event, WinReg),
    Size = Window#slide.size,
    case Size < WithinClause of
	true -> NewWindow = rivus_cep_window:resize(Window, WithinClause),
		dict:store(Event, NewWindow, WinReg);
	false -> WinReg
    end.
	     
    
create_new_global_window(Event, WinReg, WithinClause) ->
    Window = rivus_cep_window:new(WithinClause),
    dict:store(Event, Window, WinReg).
