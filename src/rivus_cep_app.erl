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

-module(rivus_cep_app).

-behaviour(application).

%% Application callbacks
-export([start/0, stop/0, start/2, stop/1]).

start() ->
    start(normal, []).

stop() ->
    stop([]).

start(_StartType, _StartArgs) ->
    case rivus_cep_sup:start_link() of
	{ok, Pid} ->
	    {ok, _} = rivus_cep_window:start_link(),
	    {ok, Pid};
	Error ->
	    Error
    end.
 
stop(_State) ->
    ok.
