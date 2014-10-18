%%------------------------------------------------------------------------------
%% Copyright (c) 2014 Vasil Kolarov
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
-module(rivus_cep_event_creator).
-compile([{parse_transform, lager_transform}]).
-include_lib("eunit/include/eunit.hrl").

%% API
-export([load_event_mod/1]).

load_event_mod({EventName, Attributes}) when is_atom(EventName) andalso is_list(Attributes) ->
  %%{event10, [attr1, attr2, attr3]}
  Module = erl_syntax:attribute(erl_syntax:atom(module),[erl_syntax:atom(EventName)]),
  ModuleForm =  erl_syntax:revert(Module),

  Export = erl_syntax:attribute(erl_syntax:atom(export),[erl_syntax:list([erl_syntax:arity_qualifier(erl_syntax:atom(get_param_by_name),erl_syntax:integer(2))])]),
  ExportForm = erl_syntax:revert(Export),

  %%
  %%function: get_param_by_name(Event, ParamName)
  %%
  EventVar = erl_syntax:variable("Event"),
  ParamNameVar = erl_syntax:variable("ParamName"),

  %%Case = case_expr(Argument::syntaxTree(), Clauses::[syntaxTree()]) -> syntaxTree()
  Argument = ParamNameVar,

  {_, ClausesList} = lists:foldl(fun(Attr, {Idx, L}) ->
                              {Idx+1, L ++ [create_case_clause(EventVar, Attr, Idx)]}
                            end, {2, [create_case_clause(EventVar, name, 1)]}, Attributes),

  CaseExpr = erl_syntax:case_expr(Argument, ClausesList),

  FuncClause =  erl_syntax:clause([EventVar,ParamNameVar],[],[CaseExpr]),
  Function =  erl_syntax:function(erl_syntax:atom(get_param_by_name),[FuncClause]),
  FunctionForm = erl_syntax:revert(Function),

  {ok, Mod, Bin, _} = compile:forms([ModuleForm, ExportForm, FunctionForm], [return]),
  code:load_binary(Mod, [], Bin).


create_case_clause(EventVar, Attribute, Idx) ->
  %%clause(Patterns::[syntaxTree()], Guard::guard(), Body::[syntaxTree()]) -> syntaxTree()
  ClauseBody = erl_syntax:application(none, erl_syntax:atom(element), [erl_syntax:integer(Idx), EventVar]),
  erl_syntax:clause([erl_syntax:atom(Attribute)],[],[ClauseBody]).