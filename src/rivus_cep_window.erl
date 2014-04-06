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

-record(state,{provider, window, size, mod_details}).

-define(SERVER, ?MODULE).


%% gen_server API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(WinModule) ->
    gen_server:start_link(?MODULE, [WinModule], []).

start_link(WinModule, Args) when is_atom(Args) andalso Args == global-> %%start server for global windows
    gen_server:start_link({local, ?SERVER}, ?MODULE, [WinModule], []);
start_link(WinModule, Args) when is_list(Args) ->
    gen_server:start_link(?MODULE, [WinModule, Args], []).



init([WinModule]) ->
    lager:info("--- rivus_cep: Window server started. Provider:~p~n",[WinModule]),
    {ok, MD} = WinModule:initialize([]),
    {ok, #state{provider = WinModule, mod_details = MD}};
init([WinModule, Args]) ->
    lager:info("--- rivus_cep: Window server started. Provider:~p, Args:~p~n",[WinModule,Args]),
    {ok, MD} = WinModule:initialize(Args),
    {ok, #state{provider = WinModule, mod_details = MD}}.

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

handle_call({new, Size}, _From, #state{provider=Mod, mod_details = MD} = State) ->
    Res = Mod:new(Size, MD),
    {reply, Res, State#state{window=Res, size=Size}};
handle_call({new, slide, Size}, _From, #state{provider=Mod, mod_details = MD} = State) ->
    Res = Mod:new(Size, MD),
    {reply, Res, State#state{window=Res, size=Size, mod_details = MD}, timeout(Size)};
handle_call({update, Window, Value}, _From, #state{provider=Mod, mod_details = MD} = State) ->
    lager:debug("~nUpdate window:~p, Value: ~p~n",[Window, Value]),
    Res = Mod:update(Window, Value, MD),
    {reply, Res, State};
handle_call({get_value, Window}, _From, #state{provider=Mod, mod_details = MD} = State) ->
    Res = Mod:get_values(Window, MD),
    {reply, Res, State};
handle_call({resize, Window, NewSize}, _From, #state{provider=Mod, mod_details = MD} = State) ->
    Res = Mod:resize(Window, NewSize, MD),
    {reply, Res, State#state{window=Res, size=NewSize},timeout(NewSize)};
handle_call({get_window, Window}, _From, #state{provider=Mod, mod_details = MD} = State) ->
    Res = Mod:get_window(Window, MD),
    {reply, Res, State};
handle_call({update_fsm, Window, Key, Value}, _From, #state{provider=Mod, mod_details = MD} = State) ->
    Res = Mod:update_fsm(Window, Key, Value, MD),
    {reply, Res, State};
handle_call({get_fsm, Window}, _From, #state{provider=Mod, mod_details = MD} = State) ->
    Res = Mod:get_fsms(Window, MD),
    {reply, Res, State};
handle_call({delete_fsm, Window, Key}, _From, #state{provider=Mod, mod_details = MD} = State) ->
    Res = Mod:delete_fsm(Window, Key, MD),
    {reply, Res, State};
handle_call({get_result, local, Window, Events}, _From, #state{provider=Mod, mod_details = MD} = State) ->
    Res = Mod:get_result(local, Window, Events, MD),
    {reply, Res, State};
handle_call({get_result, global, WinReg, Events}, _From, #state{provider=Mod, mod_details = MD} = State) ->
    Res = Mod:get_result(global, WinReg, Events, MD),
    {reply, Res, State}.

handle_info(timeout, State=#state{window = Window, provider=Mod, mod_details = MD, size=Size}) ->
    Mod:trim(Window, MD),
    {noreply, State, timeout(Size)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

timeout(Window) ->
    timer:seconds(Window) div 2.
