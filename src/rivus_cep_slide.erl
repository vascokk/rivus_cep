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
	 delete/1]).

-include("rivus_cep.hrl").

-define(WIDTH, 16).



new(Size) ->
    Window = #slide{size = Size},
    Pid = rivus_cep_slide_server_sup:start_slide_server(?MODULE, Window#slide.reservoir, Window#slide.size),
    Window#slide{server = Pid}.

resize(Window, NewSize) ->
    rivus_cep_slide_server:resize(Window#slide.server, NewSize),
    Window#slide{size = NewSize}.    

update(#slide{reservoir = Reservoir} = Window, Value) ->
    Now = rivus_cep_utils:timestamp(),
    X = erlang:system_info(scheduler_id),
    Rnd = X band (?WIDTH-1),
    ets:insert(Reservoir, {{Now, Rnd}, Value}),
    Window.

update_fsm(#slide{reservoir = Reservoir} = Window, Key, Value) ->
    ets:update_element(Reservoir, Key, {1, Value}),
    Window.

get_values(#slide{reservoir = Reservoir, size = Size}) ->
    Oldest = rivus_cep_utils:timestamp() - Size,
    ets:select(Reservoir, [{{{'$1','_'},'$2'},[{'>=', '$1', Oldest}],['$2']}]).

get_fsms(#slide{reservoir = Reservoir, size = Size}) ->
    Oldest = rivus_cep_utils:timestamp() - Size,
    ets:select(Reservoir, [{{{'$1','$2'},'$3'},[{'>=', '$1', Oldest}],['$_']}]).


delete_fsm(#slide{reservoir = Reservoir, size = Size}, {Ts,Rnd}) ->
    Oldest = rivus_cep_utils:timestamp() - Size,
    ets:select_delete(Reservoir, [{{{'$1','$2'},'$3'},[{'andalso',{'==', '$1', Ts}, {'==', '$2',Rnd}}],['true']}]).

delete(#slide{reservoir = Reservoir, server = Pid}) ->
    rivus_cep_slide_server:stop(Pid),
    ets:delete(Reservoir).

trim(#slide{reservoir = Reservoir, size = Size}) ->
    Oldest = rivus_cep_utils:timestamp() - Size,
    ets:select_delete(Reservoir, [{{{'$1','_'},'_'},[{'<', '$1', Oldest}],['true']}]).

trim(Reservoir, Size) when is_integer(Size) ->
    Oldest = rivus_cep_utils:timestamp() - Size,
    ets:select_delete(Reservoir, [{{{'$1','_'},'_'},[{'<', '$1', Oldest}],['true']}]);
trim(#slide{reservoir = Reservoir, size = Size}, Fun) when is_function(Fun) ->
    Oldest = rivus_cep_utils:timestamp() - Size,
    Match = ets:select(Reservoir, [{{{'$1','_'},'_'},[{'<', '$1', Oldest}],['true']}]),
    lists:foreach(fun(X) -> Fun(X) end, Match),
    ets:select_delete(Reservoir, [{{{'$1','_'},'_'},[{'<', '$1', Oldest}],['true']}]).


