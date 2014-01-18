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

-module(rivus_cep_slide_server).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/3,
	 resize/2,
	 stop/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {window_mod,  reservoir, size}).
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(WindowMod, Reservoir, Size) ->
    gen_server:start_link(?MODULE, [WindowMod, Reservoir, Size], []).

resize(Pid, NewSize) ->
    gen_server:call(Pid, {resize, NewSize}).

stop(Pid) ->
    gen_server:cast(Pid, stop).
    
%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([WindowMod, Reservoir, Size]) ->
    {ok, #state{window_mod = WindowMod, reservoir = Reservoir, size = Size}, timeout(Size)}.

handle_call({resize, NewSize}, _From, State) ->
    NewState = State#state{size=NewSize},
    Reply = ok,
    {reply, Reply, NewState, timeout(NewSize)};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State=#state{window_mod = WindowMod, reservoir = Reservoir, size = Size}) ->
    WindowMod:trim(Reservoir,Size),
    {noreply, State, timeout(Size)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
timeout(Window) ->
    timer:seconds(Window) div 2.
