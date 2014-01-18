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

-compile([{parse_transform, lager_transform}]).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include("rivus_cep.hrl").

-export([new/1,
         new/2,
         update/2,         
         get_values/1,
	 select/2,
	 get_window/1
         ]).

new(Size) ->
    rivus_cep_slide:new(Size).

new(Size, slide) ->
    rivus_cep_slide:new(Size).


update(Sample, Value) ->
    lager:debug("~nUpdate window:~p, Value: ~p~n",[Sample, Value]),
    rivus_cep_slide:update(Sample, Value).

% pulls the sample out of the record obtained from ets
get_values(Sample) ->
    rivus_cep_slide:get_values(Sample).

resize(Sample, NewSize) ->
    rivus_cep_slide:resize(Sample, NewSize).

%%just for testing
select(Sample, "blah") ->
    Size = Sample#slide.size,
    Reservoir = Sample#slide.reservoir,    
    Oldest = rivus_cep_utils:timestamp() - Size,
    ets:select(Reservoir,   ets:fun2ms(fun({{Time,'_'},Value}) when Time >= Oldest andalso element(2,Value) == a -> Value end)).

get_window(Sample) ->
    Size = Sample#slide.size,
    Reservoir = Sample#slide.reservoir,    
    Oldest = rivus_cep_utils:timestamp() - Size,
    {Reservoir, Oldest}.

