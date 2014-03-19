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
-include_lib("../deps/folsom/include/folsom.hrl").

-export([new/1,
         new/2,
         update/2,
	 resize/2,
         get_values/1,
	 get_window/1,
	 update_fsm/3,
	 delete_fsm/2,
	 get_fsms/1,
	 start_link/0]).

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

update(Sample, Value) ->
    gen_server:call(?SERVER, {update, Sample, Value}).

get_values(Sample) ->
    gen_server:call(?SERVER, {get_value, Sample}).

resize(Sample, NewSize) ->
    gen_server:call(?SERVER, {resize, Sample, NewSize}).

get_window(Sample) ->
    gen_server:call(?SERVER, {get_window, Sample}).

update_fsm(Sample, Key, Value) ->
    gen_server:call(?SERVER, {update_fsm, Sample, Key, Value}).

get_fsms(Sample) ->
    gen_server:call(?SERVER, {get_fsm, Sample}).

delete_fsm(Sample, Key) ->
    gen_server:call(?SERVER, {delete_fsm, Sample, Key}).

handle_cast(_Msg, State) -> 
    {noreply, State}.


handle_call({new, Size}, _From, #state{provider=Mod} = State) ->
    Res = Mod:new(Size),
    {reply, Res, State};
handle_call({new, slide, Size}, _From, #state{provider=Mod} = State) ->
    Res = Mod:new(Size),
    {reply, Res, State};
handle_call({update, Sample, Value}, _From, #state{provider=Mod} = State) ->
    lager:debug("~nUpdate window:~p, Value: ~p~n",[Sample, Value]),
    Res = Mod:update(Sample, Value),
    {reply, Res, State};
handle_call({get_value, Sample}, _From, #state{provider=Mod} = State) ->
    Res = Mod:get_values(Sample),
    {reply, Res, State};
handle_call({resize, Sample, NewSize}, _From, #state{provider=Mod} = State) ->
    Res = Mod:resize(Sample, NewSize),
    {reply, Res, State};
handle_call({get_window, Sample}, _From, #state{provider=Mod} = State) ->
    Res = Mod:get_window(Sample),
    {reply, Res, State};
handle_call({update_fsm, Sample, Key, Value}, _From, #state{provider=Mod} = State) ->
    Res = Mod:update_fsm(Sample, Key, Value),
    {reply, Res, State};
handle_call({get_fsm, Sample}, _From, #state{provider=Mod} = State) ->
    Res = Mod:get_fsms(Sample),
    {reply, Res, State};
handle_call({delete_fsm, Sample, Key}, _From, #state{provider=Mod} = State) ->
    Res = Mod:delete_fsm(Sample, Key),
    {reply, Res, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
