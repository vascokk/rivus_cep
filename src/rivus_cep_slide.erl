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

-module(rivus_cep_slide).

-compile([{parse_transform, lager_transform}]).

-export([new/2,
	 resize/3,
	 trim/2,
	 update/3,
	 get_values/2,
	 get_fsms/2,
	 update_fsm/4,
	 delete_fsm/3,
	 get_window/2,
	 get_result/4,
	 get_aggr_state/3,
	 initialize/1]).

-include("rivus_cep.hrl").
-include_lib("folsom/include/folsom.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(WIDTH, 16).

initialize([]) ->
    {ok, []}.

new(Size, _MD) ->
    folsom_sample_slide:new(Size).

resize(Window, NewSize, _MD) ->
    folsom_sample_slide:resize(Window, NewSize).

get_window(Window, _MD) ->
    Size = Window#slide.window,
    Reservoir = Window#slide.reservoir,    
    Oldest = rivus_cep_utils:timestamp() - Size,
    {Reservoir, Oldest}.

update(Window, Value, _MD) ->
    folsom_sample_slide:update(Window, Value).

update_fsm(#slide{reservoir = Reservoir} = Window, Key, Value, _MD) ->
    ets:update_element(Reservoir, Key, {1, Value}),
    Window.

get_values(Window, _MD) ->
    folsom_sample_slide:get_values(Window).

get_fsms(#slide{reservoir = Reservoir, window = Size}, _MD) ->
    Oldest = rivus_cep_utils:timestamp() - Size,
    ets:select(Reservoir, [{{{'$1','$2'},'$3'},[{'>=', '$1', Oldest}],['$_']}]).

delete_fsm(#slide{reservoir = Reservoir}, {Ts,Rnd}, _MD) ->    
    ets:select_delete(Reservoir, [{{{'$1','$2'},'$3'},[{'andalso',{'==', '$1', Ts}, {'==', '$2',Rnd}}],['true']}]).

get_result(local, Window, Events, _MD) ->
    {Reservoir, Oldest} =  get_window(Window, []),
    MatchSpecs = [create_match_spec(Event, Oldest) || Event<- Events],
    QueryHandlers = [create_qh(MS, Reservoir) || MS <- MatchSpecs],
    [qlc:e(QH) || QH <- QueryHandlers ];
get_result(global, WinReg, Events, _MD) ->
    QueryHandlers = lists:map(fun(Event) ->  create_qh_shared_window(Event, WinReg) end, Events),    
    [qlc:e(QH) || QH <- QueryHandlers ].

get_aggr_state(local, Window, _MD) ->
    {Reservoir, Oldest} =  get_window(Window, []),
    MatchSpecs = [ets:fun2ms(fun({ {Time,'_'},Value}) when Time >= Oldest -> Value end)],
    QueryHandlers = [create_qh(MS, Reservoir) || MS <- MatchSpecs],
    [qlc:e(QH) || QH <- QueryHandlers ].

trim(Window, _MD) ->
    Reservoir = Window#slide.reservoir,
    ets:delete_all_objects(Reservoir).

trim(Window) ->
    Size = Window#slide.window,
    Reservoir = Window#slide.reservoir,
    Oldest = rivus_cep_utils:timestamp() - Size,
    ets:select_delete(Reservoir, [{{{'$1','_'},'_'},[{'<', '$1', Oldest}],['true']}]).   
    
create_qh_shared_window(Event, WinReg) ->
    Window = dict:fetch(Event, WinReg),
    {Reservoir, Oldest} = get_window(Window, []),
    MatchSpec = create_match_spec(Event, Oldest),
    create_qh(MatchSpec, Reservoir).

create_match_spec(Event, Oldest) ->
    ets:fun2ms(fun({ {Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==Event  -> Value end).
    
create_qh(MatchSpec, Reservoir) ->
     ets:table(Reservoir, [{traverse, {select, MatchSpec}}]).
