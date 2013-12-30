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

-module(rivus_cep_compiler).

-export([load_query/2, compile/2, scan_parse/4]).

compile (ScannerFile, ParserFile) ->
    {ok, ScanerModStr} = leex:file(ScannerFile),
    {ok, ParserModStr} = yecc:file(ParserFile).
    
scan_parse(Cont, Str, StartLoc, Acc) ->
	case erl_scan:tokens(Cont, Str, StartLoc) of 
		{done, {ok,Tokens, EndLoc}, LeftOverChars} -> 
			{ok,Form} = erl_parse:parse_form(Tokens),
			scan_parse([], LeftOverChars, EndLoc, [Form|Acc]);
		_ -> lists:reverse(Acc)
	end.


load_query(QueryStr, SubscriberPid)->
    compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
    {ok, Tokens, Endline} = rivus_cep_scanner:string(QueryStr, 1),    
    StmtClauses = rivus_cep_parser:parse(Tokens),    
    {ok, [StmtName, SelectClause, FromClause, WhereClause, WithinClause]} = StmtClauses,
    {TemplateFile, _FromClause} = case FromClause of
				   {pattern, Events} -> {"../priv/pattern_stmt_template.dtl", Events};
				   _ -> {"../priv/stmt_template.dtl", FromClause}
			       end,
    
    Stmt = rivus_cep_stmt_builder:build_rs_stmt(StmtName, SelectClause, _FromClause, WhereClause),
    
    erlydtl:compile(TemplateFile, stmt_template,[{custom_filters_modules,[erlydtl_custom_filters]}]),
    {ok, Templ} = stmt_template:render([
					{stmtName, list_to_binary(atom_to_list(element(1,StmtName)))},
					{timeout, element(1,WithinClause)},
					{resultsetStmt, list_to_binary(Stmt)},
					{eventList, element(1, _FromClause)}
				       ]),

    Source = binary_to_list(iolist_to_binary(Templ)),
    Forms = erl_syntax:revert(rivus_cep_compiler:scan_parse([], Source, 0, [])),
    Mod = compile:forms(Forms, [return]),
    {CompileRes, ModName, Bin, _} = Mod,
    code:load_binary(ModName, atom_to_list(ModName), Bin),
    pattern2:start_link(SubscriberPid).
