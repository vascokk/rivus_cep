%% ; -*- mode: Erlang;-*-

Header "%% Copyright (c)" "%% @Vasil Kolarov".

Nonterminals declaration select_clause from_clause pattern where_clause within_clause name_clause name_params select_element event events alias event_param expression predicate predicates operand uminus function. 

Terminals define as select from where within seconds and or not if foreach '(' ')' '+' '-' '*' '/' '<' '>' '=' '<=' '>=' '<>' ',' '->' atom integer float index of '.' var string char count sum min max avg.

Rootsymbol declaration.

Endsymbol semicolon.

Nonassoc 100 '='.
Nonassoc 200 '>'.
Nonassoc 300 '<'.
Nonassoc 400 '>='.
Nonassoc 500 '<='.
Nonassoc 600 '<>'.
Left 700 '+'.
Left 800 '-'.
Left 900 'or'.
Left 1000 '*'.
Left 1100 '/'.
Left 1200 'and'.
Unary 1300 uminus.

uminus -> '-' expression.

declaration -> define 
		   name_clause  as
		   select
		   select_clause
		   from_clause
		   where_clause
		   within_clause : get_ast({'$2','$5','$6','$7','$8'}).

declaration -> define 
		   name_clause  as
		   select
		   select_clause
		   from_clause
		   where_clause : get_ast({'$2','$5','$6','$7',nil}).

declaration -> define 
		   name_clause  as
		   select
		   select_clause
		   from_clause
		   within_clause : get_ast({'$2','$5','$6',nil,'$7'}).

declaration -> define 
		   name_clause  as
		   select
		   select_clause
		   from_clause: get_ast({'$2','$5','$6',nil,nil}).

name_clause -> atom: value_of('$1').

select_clause -> expression: ['$1'].
select_clause -> expression ',' select_clause: flatten(['$1', '$3']). 

event_param ->  atom: {nil, value_of('$1')}.
event_param -> alias '.' atom: {'$1', value_of('$3')}.

from_clause -> from events: '$2'.
from_clause -> from pattern: {pattern, '$2'}.

pattern -> event '->' event: {'$1','$3'}.
pattern -> event '->' pattern: {'$1', '$3'}.

event -> atom: {nil, value_of('$1')}. 
event -> atom as alias: {'$3',value_of('$1')}.
events -> event: ['$1'].
events -> event ',' events: flatten(['$1','$3']).

alias -> atom: value_of('$1').

where_clause -> where predicates: '$2'.

expression -> expression '+' expression: {'plus','$1','$3'}.
expression -> expression '-' expression: {'minus','$1','$3'}.
expression -> expression '*' expression: {'mult','$1','$3'}.
expression -> expression '/' expression: {'div','$1','$3'}.
expression ->  '(' expression ')': '$2'.
expression ->  function '(' expression ')': {type_of('$1'), '$3'}.
expression -> event_param: '$1'.
expression -> integer: {integer,value_of('$1')}.
expression -> float: {float, value_of('$1')}.

predicates -> predicates 'or' predicates:  {'or','$1','$3'}.
predicates -> predicates 'and'  predicates:  {'and','$1','$3'}.
predicates -> predicate: '$1'.
predicate -> '(' predicate ')' : '$2'.
predicate -> expression '=' expression: {'eq','$1','$3'}.
predicate -> expression '<' expression: {'lt','$1','$3'}.
predicate -> expression '>' expression: {'gt','$1','$3'}.
predicate -> expression '<=' expression: {'lte','$1','$3'}.
predicate -> expression '>=' expression: {'gte','$1','$3'}.
predicate -> expression '<>' expression: {'neq','$1','$3'}.

within_clause -> within integer seconds: value_of('$2').

function -> sum: '$1'.
function -> count: '$1'.
function -> min: '$1'.
function -> max: '$1'.
function -> avg: '$1'.

Erlang code.

-import(erl_syntax, []).
-include_lib("eunit/include/eunit.hrl").

value_of(Token) -> element(3, Token).
line_of(Token) -> element(2, Token).
type_of(Token) -> element(1, Token).
flatten(L) -> lists:flatten(L).
    
get_ast({Name, SelectClause, _FromClause, WhereClause, WithinClause}) ->
    %% ?debugMsg(io_lib:format("SelectClause: ~p~n",[SelectClause])),
    %% ?debugMsg(io_lib:format("FromClause: ~p~n",[_FromClause])),
    %% ?debugMsg(io_lib:format("WhereClause: ~p~n",[WhereClause])),
    {IsPattern, FromClause} = case _FromClause of
				  {pattern, Events} -> {true, tuple_to_list(Events)};
				  _ -> {false, _FromClause}
			      end,
    %% ?debugMsg(io_lib:format("new FromClause: ~p~n",[FromClause])),
    Select = rivus_cep_parser_utils:replace_select_aliases(SelectClause, FromClause),
    Where = rivus_cep_parser_utils:replace_where_aliases(WhereClause, FromClause),
    From = case IsPattern of
	       true -> {pattern, {rivus_cep_parser_utils:remove_from_aliases(FromClause)}};
	       false -> {rivus_cep_parser_utils:remove_from_aliases(FromClause)}
	   end,
    [{Name}, {Select}, From, {Where}, {WithinClause}].

