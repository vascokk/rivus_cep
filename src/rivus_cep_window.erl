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
%%-include_lib("folsom/include/folsom.hrl").

-export([new/1,
	 new/2,
         new/3,
	 update/2,
         update/3,
	 resize/2,
	 resize/3,
	 get_values/1,
         get_values/2,
	 get_window/1,
	 get_window/2,
	 update_fsm/4,
	 delete_fsm/3,
	 get_fsms/2,
	 start_link/1,
	 start_link/2,
	 get_pre_result/3,
	 get_pre_result/4]).

-record(state,{provider, window, size}).

-define(SERVER, ?MODULE).


%% gen_server API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(WinModule) ->
    %%gen_server:start_link({local, ?SERVER}, ?MODULE, [WinModule], []).
    gen_server:start_link(?MODULE, [WinModule], []).

%%start server for global windows
start_link(global, WinModule) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [WinModule], []).


init([WinModule]) ->
    lager:info("--- rivus_cep: Window server started"),
    %%Mod = application:get_env(rivus_cep, rivus_window_provider, rivus_cep_slide),
    {ok, #state{provider = WinModule}}.

new(Size) ->
    gen_server:call(?SERVER, {new, Size}).

new(slide, Size) ->
    gen_server:call(?SERVER, {new, slide, Size});
new(Pid, Size) when Pid /= slide->
    gen_server:call(Pid, {new, Size}).

new(Pid, slide, Size) ->
    gen_server:call(Pid, {new, slide, Size}).

update(Window, Value) ->
    gen_server:call(?SERVER, {update, Window, Value}).

update(Pid, Window, Value) ->
    gen_server:call(Pid, {update, Window, Value}).


get_values(Window) ->
    gen_server:call(?SERVER, {get_value, Window}).

get_values(Pid, Window) ->
    gen_server:call(Pid, {get_value, Window}).

resize(Window, NewSize) ->
    gen_server:call(?SERVER, {resize, Window, NewSize}).

resize(Pid, Window, NewSize) ->
    gen_server:call(Pid, {resize, Window, NewSize}).

get_window(Window) ->
    gen_server:call(?SERVER, {get_window, Window}).

get_window(Pid, Window) ->
    gen_server:call(Pid, {get_window, Window}).

update_fsm(Pid, Window, Key, Value) ->
    gen_server:call(Pid, {update_fsm, Window, Key, Value}).

get_fsms(Pid, Window) ->
    gen_server:call(Pid, {get_fsm, Window}).

delete_fsm(Pid, Window, Key) ->
    gen_server:call(Pid, {delete_fsm, Window, Key}).

get_pre_result(Pid, local, Window, Events) ->
    gen_server:call(Pid, {get_result, local, Window, Events}).

get_pre_result(global, WinReg, Events) ->
    gen_server:call(?SERVER, {get_result, global, WinReg, Events}).
    
handle_cast(_Msg, State) -> 
    {noreply, State}.

handle_call({new, Size}, _From, #state{provider=Mod} = State) ->
    Res = Mod:new(Size),
    {reply, Res, State#state{window=Res, size=Size}};
handle_call({new, slide, Size}, _From, #state{provider=Mod} = State) ->
    Res = Mod:new(Size),
    {reply, Res, State#state{window=Res, size=Size}, timeout(Size)};
handle_call({update, Window, Value}, _From, #state{provider=Mod} = State) ->
    lager:debug("~nUpdate window:~p, Value: ~p~n",[Window, Value]),
    Res = Mod:update(Window, Value),
    {reply, Res, State};
handle_call({get_value, Window}, _From, #state{provider=Mod} = State) ->
    Res = Mod:get_values(Window),
    {reply, Res, State};
handle_call({resize, Window, NewSize}, _From, #state{provider=Mod} = State) ->
    Res = Mod:resize(Window, NewSize),
    {reply, Res, State#state{window=Res, size=NewSize},timeout(NewSize)};
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
    Res = Mod:get_result(local, Window, Events),
    {reply, Res, State};
handle_call({get_result, global, WinReg, Events}, _From, #state{provider=Mod} = State) ->
    Res = Mod:get_result(global, WinReg, Events),
    {reply, Res, State}.

handle_info(timeout, State=#state{window = Window, provider=Mod, size=Size}) ->
    Mod:trim(Window),
    {noreply, State, timeout(Size)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

timeout(Window) ->
    timer:seconds(Window) div 2.
