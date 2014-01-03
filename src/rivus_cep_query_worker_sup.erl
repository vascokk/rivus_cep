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

-module(rivus_cep_query_worker_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

-compile([{parse_transform, lager_transform}]).

start_link() ->
    supervisor:start_link(?MODULE, []).
 
init(Args) ->
    lager:debug("query_worker_sup, Args:  ~p~n",[Args]),
    MaxRestart = 5,
    MaxTime = 3600,
    {ok, {{simple_one_for_one, MaxRestart, MaxTime},
	  [{query_worker,
	    {rivus_cep_query_worker, start_link, []},
	    transient, brutal_kill, worker, [rivus_cep_query_worker]}]}}.
