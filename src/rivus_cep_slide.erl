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

-export([new/1,
	 resize/2,
	 trim/1,
	 trim/2,
	 update/2,
	 get_values/1,
	 get_fsms/1,
	 update_fsm/3,
	 delete_fsm/2,
	 get_window/1]).

-include("rivus_cep.hrl").
-include_lib("../deps/folsom/include/folsom.hrl").

-define(WIDTH, 16).



new(Size) ->
    folsom_sample_slide:new(Size).

resize(Window, NewSize) ->
    folsom_sample_slide:resize(Window, NewSize).

get_window(Sample) ->
    Size = Sample#slide.window,
    Reservoir = Sample#slide.reservoir,    
    Oldest = rivus_cep_utils:timestamp() - Size,
    {Reservoir, Oldest}.

update(Window, Value) ->
    folsom_sample_slide:update(Window, Value).

update_fsm(#slide{reservoir = Reservoir} = Window, Key, Value) ->
    ets:update_element(Reservoir, Key, {1, Value}),
    Window.

get_values(Window) ->
    folsom_sample_slide:get_values(Window).

get_fsms(#slide{reservoir = Reservoir, window = Size}) ->
    Oldest = rivus_cep_utils:timestamp() - Size,
    ets:select(Reservoir, [{{{'$1','$2'},'$3'},[{'>=', '$1', Oldest}],['$_']}]).

delete_fsm(#slide{reservoir = Reservoir}, {Ts,Rnd}) ->    
    ets:select_delete(Reservoir, [{{{'$1','$2'},'$3'},[{'andalso',{'==', '$1', Ts}, {'==', '$2',Rnd}}],['true']}]).

trim(#slide{reservoir = Reservoir, window = Size}) ->
    Oldest = rivus_cep_utils:timestamp() - Size,
    ets:select_delete(Reservoir, [{{{'$1','_'},'_'},[{'<', '$1', Oldest}],['true']}]).

trim(Reservoir, Size) when is_integer(Size) ->
    Oldest = rivus_cep_utils:timestamp() - Size,
    ets:select_delete(Reservoir, [{{{'$1','_'},'_'},[{'<', '$1', Oldest}],['true']}]);
trim(#slide{reservoir = Reservoir, window = Size}, Fun) when is_function(Fun) ->
    Oldest = rivus_cep_utils:timestamp() - Size,
    Match = ets:select(Reservoir, [{{{'$1','_'},'_'},[{'<', '$1', Oldest}],['true']}]),
    lists:foreach(fun(X) -> Fun(X) end, Match),
    ets:select_delete(Reservoir, [{{{'$1','_'},'_'},[{'<', '$1', Oldest}],['true']}]).


