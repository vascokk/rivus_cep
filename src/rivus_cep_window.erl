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

-module(rivus_cep_window).
-behaviour(gen_server).

-compile([{parse_transform, lager_transform}]).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include("rivus_cep.hrl").
-include_lib("folsom/include/folsom.hrl").

-export([new/1,
         new/2,
         update/2,
	 resize/2,
         get_values/1,
	 get_window/1,
	 update_fsm/3,
	 delete_fsm/2,
	 get_fsms/1,
	 start_link/0,
	 get_pre_result/3]).

-record(state,{provider}).

-define(SERVER, ?MODULE).


%% gen_server API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    lager:info("--- rivus_cep: Window server started"),
    Mod = application:get_env(rivus_cep, rivus_window_provider, rivus_cep_slide),
    {ok, #state{provider = Mod}}.

new(Size) ->
    gen_server:call(?SERVER, {new, Size}).

new(Size, slide) ->
    gen_server:call(?SERVER, {new, Size}).

update(Window, Value) ->
    gen_server:call(?SERVER, {update, Window, Value}).

get_values(Window) ->
    gen_server:call(?SERVER, {get_value, Window}).

resize(Window, NewSize) ->
    gen_server:call(?SERVER, {resize, Window, NewSize}).

get_window(Window) ->
    gen_server:call(?SERVER, {get_window, Window}).

update_fsm(Window, Key, Value) ->
    gen_server:call(?SERVER, {update_fsm, Window, Key, Value}).

get_fsms(Window) ->
    gen_server:call(?SERVER, {get_fsm, Window}).

delete_fsm(Window, Key) ->
    gen_server:call(?SERVER, {delete_fsm, Window, Key}).

get_pre_result(local, Window, Events) ->
    gen_server:call(?SERVER, {get_result, local, Window, Events});
get_pre_result(global, WinReg, Events) ->
    gen_server:call(?SERVER, {get_result, global, WinReg, Events}).
    

handle_cast(_Msg, State) -> 
    {noreply, State}.


handle_call({new, Size}, _From, #state{provider=Mod} = State) ->
    Res = Mod:new(Size),
    {reply, Res, State};
handle_call({new, slide, Size}, _From, #state{provider=Mod} = State) ->
    Res = Mod:new(Size),
    {reply, Res, State};
handle_call({update, Window, Value}, _From, #state{provider=Mod} = State) ->
    lager:debug("~nUpdate window:~p, Value: ~p~n",[Window, Value]),
    Res = Mod:update(Window, Value),
    {reply, Res, State};
handle_call({get_value, Window}, _From, #state{provider=Mod} = State) ->
    Res = Mod:get_values(Window),
    {reply, Res, State};
handle_call({resize, Window, NewSize}, _From, #state{provider=Mod} = State) ->
    Res = Mod:resize(Window, NewSize),
    {reply, Res, State};
handle_call({get_window, Window}, _From, #state{provider=Mod} = State) ->
    Res = Mod:get_window(Window),
    {reply, Res, State};
handle_call({update_fsm, Window, Key, Value}, _From, #state{provider=Mod} = State) ->
    Res = Mod:update_fsm(Window, Key, Value),
    {reply, Res, State};
handle_call({get_fsm, Window}, _From, #state{provider=Mod} = State) ->
    Res = Mod:get_fsms(Window),
    {reply, Res, State};
handle_call({delete_fsm, Window, Key}, _From, #state{provider=Mod} = State) ->
    Res = Mod:delete_fsm(Window, Key),
    {reply, Res, State};
handle_call({get_result, local, Window, Events}, _From, #state{provider=Mod} = State) ->
    %% {Reservoir, Oldest} =  Mod:get_window(Window),
    %% MatchSpecs = [create_match_spec(Event, Oldest) || Event<- Events],
    %% QueryHandlers = [create_qh(MS, Reservoir) || MS <- MatchSpecs],
    %% Res = [qlc:e(QH) || QH <- QueryHandlers ],
    Res = Mod:get_result(local, Window, Events),
    {reply, Res, State};
handle_call({get_result, global, WinReg, Events}, _From, #state{provider=Mod} = State) ->
    %% QueryHandlers = lists:map(fun(Event) ->  create_qh_shared_window(Event, WinReg, Mod) end, Events),    
    %% Res = [qlc:e(QH) || QH <- QueryHandlers ],
    Res = Mod:get_result(global, WinReg, Events),
    {reply, Res, State}.
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% create_qh_shared_window(Event, WinReg, Mod) ->
%%     Window = dict:fetch(Event, WinReg),
%%     {Reservoir, Oldest} = Mod:get_window(Window),
%%     MatchSpec = create_match_spec(Event, Oldest),
%%     create_qh(MatchSpec, Reservoir).

%% create_match_spec(Event, Oldest) ->
%%     ets:fun2ms(fun({ {Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==Event  -> Value end).
    
%% create_qh(MatchSpec, Reservoir) ->
%%      ets:table(Reservoir, [{traverse, {select, MatchSpec}}]).
