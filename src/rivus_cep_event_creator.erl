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

load_event_mod({EventName, ParameterNames}) when is_atom(EventName) andalso is_list(ParameterNames) ->
  %%{event10, [attr1, attr2, attr3]}
  Module = erl_syntax:attribute(erl_syntax:atom(module),[erl_syntax:atom(EventName)]),
  ModuleForm =  erl_syntax:revert(Module),

  ExpFunctionList = erl_syntax:list([erl_syntax:arity_qualifier(erl_syntax:atom(get_param_by_name),erl_syntax:integer(2)),
                                  erl_syntax:arity_qualifier(erl_syntax:atom(get_param_names),erl_syntax:integer(0))]),
  Export = erl_syntax:attribute(erl_syntax:atom(export),[ExpFunctionList]),
  ExportForm = erl_syntax:revert(Export),

  GetParamFuncForm = create_get_param_by_name(ParameterNames),
  GetAttrFuncForm = create_get_param_names(ParameterNames),

  {ok, Mod, Bin, _} = compile:forms([ModuleForm, ExportForm, GetParamFuncForm, GetAttrFuncForm], [return]),
  code:load_binary(Mod, [], Bin).

create_get_param_by_name(ParameterNames) ->
  EventVar = erl_syntax:variable("Event"),
  ParamNameVar = erl_syntax:variable("ParamName"),

  %%Case = case_expr(Argument::syntaxTree(), Clauses::[syntaxTree()]) -> syntaxTree()
  {_, ClausesList} = lists:foldl(fun(Attr, {Idx, L}) ->
                                      {Idx + 1, L ++ [create_case_clause(EventVar, Attr, Idx)]}
                                 end,
                                {2, [create_case_clause(EventVar, name, 1)]},
                                 ParameterNames),
  CaseExpr = erl_syntax:case_expr(ParamNameVar, ClausesList),
  FuncClause = erl_syntax:clause([EventVar, ParamNameVar], [], [CaseExpr]),
  Function = erl_syntax:function(erl_syntax:atom(get_param_by_name), [FuncClause]),
  FunctionForm = erl_syntax:revert(Function),
  FunctionForm.

create_get_param_names(ParameterNames) ->
  Body = erl_syntax:list([erl_syntax:atom(ParamName) || ParamName <- ParameterNames]),
  FuncClause = erl_syntax:clause([], [], [Body]),
  Function = erl_syntax:function(erl_syntax:atom(get_param_names), [FuncClause]),
  FunctionForm = erl_syntax:revert(Function),
  FunctionForm.

create_case_clause(EventVar, ParameterNames, Idx) ->
  %%clause(Patterns::[syntaxTree()], Guard::guard(), Body::[syntaxTree()]) -> syntaxTree()
  ClauseBody = erl_syntax:application(none, erl_syntax:atom(element), [erl_syntax:integer(Idx), EventVar]),
  erl_syntax:clause([erl_syntax:atom(ParameterNames)],[],[ClauseBody]).