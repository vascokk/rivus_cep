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
-include_lib("../deps/folsom/include/folsom.hrl").

-export([new/1,
         new/2,
         new/3,
         new/4,
         update/2,
         get_value/1,
         get_values/1,
	 select/2,
	 get_window/1
         ]).

new(Name) ->
    folsom_metrics_histogram:new(Name, slide).

new(Name, slide) ->
    folsom_metrics_histogram:new(Name, slide);
new(Name, slide_uniform) ->
    folsom_metrics_histogram:new(Name, slide_uniform);
new(Name, SampleType) ->
    folsom_metrics_histogram:new(Name, SampleType).

new(Name, SampleType, SampleSize) ->
    folsom_metrics_histogram:new(Name, SampleType, SampleSize).

new(Name, SampleType, SampleSize, Alpha) ->
    folsom_metrics_histogram:new(Name, SampleType, SampleSize, Alpha).

update(Name, Value) ->
    lager:debug("~nUpdate window:~p~n",[Name]),
    folsom_metrics_histogram:update(Name, Value).

% gets the histogram record from ets
get_value(Name) ->
    folsom_metrics_histogram:get_value(Name).

% pulls the sample out of the record obtained from ets
get_values(Name) ->
    folsom_metrics_histogram:get_values(Name).

%%just for testing
select(Name, "blah") ->
    Window = folsom_metrics_histogram:get_value(Name),
    Sample = Window#histogram.sample,
    Size = Sample#slide.window,
    Reservoir = Sample#slide.reservoir,    
    Oldest = folsom_sample_slide:moment() - Size,
    ets:select(Reservoir,   ets:fun2ms(fun({{Time,'_'},Value}) when Time >= Oldest andalso element(2,Value) == a -> Value end)).

get_window(Name) ->
    Window = folsom_metrics_histogram:get_value(Name),
    Sample = Window#histogram.sample,
    Size = Sample#slide.window,
    Reservoir = Sample#slide.reservoir,    
    Oldest = folsom_sample_slide:moment() - Size,
    {Reservoir, Oldest}.

