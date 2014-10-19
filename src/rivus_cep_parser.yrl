%% ; -*- mode: Erlang;-*-

Header "%% Copyright (c)" "%% @Vasil Kolarov".

Nonterminals declaration event_declaration query_declaration select_clause select_elements from_clause pattern where_clause within_clause filter filter_predicates filter_predicate name_clause name_params event events alias event_param expression predicate predicates operand uminus function literal window_type event_name event_attribute event_attributes.

Terminals define as select from where within seconds and or not if foreach '(' ')' '+' '-' '*' '/' '<' '>' '=' '<=' '>=' '<>' ',' '->' atom integer float index of '.' var string char count sum min max avg sliding batch tumbling.

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
Unary 1400 'not'.

uminus -> '-' expression.

declaration -> event_declaration : {event, '$1'}.
declaration -> query_declaration : '$1'.

event_declaration -> define name_clause as '(' event_attributes ')' : {'$2', '$5'}.

event_attributes -> event_attribute: '$1'.
event_attributes -> event_attribute ',' event_attributes: flatten(['$1', '$3']).

event_attribute -> atom: value_of('$1').


query_declaration -> define
		   name_clause  as
		   select_clause
		   from_clause
		   where_clause
		   within_clause : get_ast({'$2','$4','$5','$6','$7'}).

query_declaration -> define
		   name_clause  as
		   select_clause
		   from_clause
		   where_clause : get_ast({'$2','$4','$5','$6',nil}).

query_declaration -> define
		   name_clause  as
		   select_clause
		   from_clause
		   within_clause : get_ast({'$2','$4','$5',nil,'$6'}).

query_declaration -> define
		   name_clause  as
		   select_clause
		   from_clause: get_ast({'$2','$4','$5',nil,nil}).

name_clause -> atom: value_of('$1').

select_clause -> select select_elements: flatten(['$2']).

select_elements -> expression: '$1'.
select_elements -> expression ',' select_elements: flatten(['$1', '$3']).

event_param ->  atom: {nil, value_of('$1')}.
event_param -> alias '.' atom: {'$1', value_of('$3')}.

from_clause -> from events: '$2'.
from_clause -> from pattern: {pattern, '$2'}.

pattern -> event '->' event: ['$1','$3'].
pattern -> event '->' pattern: ['$1', '$3'].

event -> atom: {nil, value_of('$1')}.
event -> atom filter: {nil, value_of('$1'), '$2'}.
event -> atom as alias: {'$3',value_of('$1')}.
event -> atom filter as alias: {'$4',value_of('$1'), '$2'}.
events -> event: ['$1'].
events -> event ',' events: flatten(['$1','$3']).

filter -> '(' filter_predicates ')': '$2'.
filter_predicates -> filter_predicate: ['$1'].
filter_predicates -> filter_predicate ',' filter_predicates: flatten(['$1','$3']).

filter_predicate -> event_param '=' literal: {'eq', '$1', '$3'}. 
filter_predicate -> event_param '>' literal: {'gt', '$1', '$3'}. 
filter_predicate -> event_param '<' literal: {'lt', '$1', '$3'}. 
filter_predicate -> event_param '<=' literal: {'lte', '$1', '$3'}. 
filter_predicate -> event_param '>=' literal: {'gte', '$1', '$3'}. 
filter_predicate -> event_param '<>' literal: {'neq', '$1', '$3'}. 

literal -> integer: {integer, value_of('$1')}.
literal -> float: {float, value_of('$1')}.
literal -> string: {string, value_of('$1')}.
literal -> char: {char, value_of('$1')}.    
literal -> atom: {atom, value_of('$1')}.

alias -> atom: value_of('$1').

where_clause -> where predicates: '$2'.

expression -> event_param: '$1'.
expression -> expression '+' expression: {'plus','$1','$3'}.
expression -> expression '-' expression: {'minus','$1','$3'}.
expression -> expression '*' expression: {'mult','$1','$3'}.
expression -> expression '/' expression: {'div','$1','$3'}.
expression ->  '(' expression ')': '$2'.
expression ->  function '(' expression ')': {type_of('$1'), '$3'}.
expression -> integer: {integer,value_of('$1')}.
expression -> float: {float, value_of('$1')}.


predicates -> predicates 'or' predicates:  {'or','$1','$3'}.
predicates -> predicates 'and'  predicates:  {'and','$1','$3'}.
predicates -> 'not' predicates: {neg, '$2'}.
predicates -> predicate: '$1'.
predicate -> '(' predicate ')' : '$2'.
predicate -> expression '=' expression: {'eq','$1','$3'}.
predicate -> expression '<' expression: {'lt','$1','$3'}.
predicate -> expression '>' expression: {'gt','$1','$3'}.
predicate -> expression '<=' expression: {'lte','$1','$3'}.
predicate -> expression '>=' expression: {'gte','$1','$3'}.
predicate -> expression '<>' expression: {'neq','$1','$3'}.

within_clause -> within integer seconds: value_of('$2').
within_clause -> within integer seconds window_type: {value_of('$2'), '$4'}.

window_type -> sliding : sliding.
window_type -> batch : batch.
%% TODO window_type -> tumbling : tumbling.

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
  %%?debugMsg(io_lib:format("SelectClause: ~p~n",[SelectClause])),
  %%?debugMsg(io_lib:format("_FromClause: ~p~n",[_FromClause])),
  %%?debugMsg(io_lib:format("Within clause: ~p~n",[WithinClause])),

  {IsPattern, FromClause, Filters} = case _FromClause of
                                       {pattern, Events} -> {true, get_events(Events), get_filters(Events)};
                                       Events when is_list(Events) -> {false, get_events(Events), get_filters(Events)}
                                     end,
  %%?debugMsg(io_lib:format("FromClause: ~p~n",[FromClause])),
  %%?debugMsg(io_lib:format("Filters: ~p~n",[Filters])),

  Select = replace_select_aliases(SelectClause, FromClause),
  Where = replace_where_aliases(WhereClause, FromClause),
  From = case IsPattern of
           true -> {pattern, {remove_from_aliases(FromClause, [])}};
           false -> {remove_from_aliases(FromClause, [])}
         end,
  FiltersDict = orddict:from_list(Filters),
  Within = case WithinClause of
             {D, W} -> {D, W};
             D when is_integer(D) -> {D, sliding};
             nil -> {nil}
           end,
  [{Name}, {Select}, From, {Where}, Within, {FiltersDict}].


get_events(Events) ->
  get_events(Events, []).

get_events([L | T], Acc) when is_list(L) ->
  Acc2 = get_events(L, []),
  get_events(T, Acc ++ [get_events(L, [])]);
get_events([{Alias, Event} | T], Acc) ->
  get_events(T, Acc ++ [{Alias, Event}]);
get_events([{Alias, Event, _} | T], Acc) ->
  get_events(T, Acc ++ [{Alias, Event}]);
get_events([], Acc) ->
  Acc.


get_filters(Events) ->
  get_filters(Events, []).

get_filters([L | T], Acc) when is_list(L) ->
  get_filters(T, Acc ++ get_filters(L, []));
get_filters([{_, _} | T], Acc) ->
  get_filters(T, Acc);
get_filters([{Alias, Event, FilterPred} | T], Acc) ->
  get_filters(T, Acc ++ [{Event, replace_filter_aliases(Event, FilterPred)}]);
get_filters([], Acc) ->
  Acc.

replace_filter_aliases(Event, Predicates) ->
  lists:map(fun({Op, Left, Right}) ->
    {Op,
      replace_filter_alias(Event, Left),
      replace_filter_alias(Event, Right)}
  end, Predicates).

replace_filter_alias(Event, {nil, Param}) ->
  {Event, Param};
replace_filter_alias(_Event, {atom, Param}) ->
  {atom, Param};
replace_filter_alias(_Event, {integer, Param}) ->
  {integer, Param};
replace_filter_alias(_Event, {float, Param}) ->
  {float, Param};
replace_filter_alias(_Event, {string, Param}) ->
  {string, Param};
replace_filter_alias(Event, {_Alias, Param}) ->
  {Event, Param};
replace_filter_alias(_Event, Predicate) ->
  Predicate.


replace_select_aliases({nil, ParamName}, FromTuples) ->
    case length(FromTuples) == 1 of
        true -> {element(2, hd(FromTuples)), ParamName};
        _ -> erlang:error({error, missing_event_qualifier})
    end;
replace_select_aliases({Alias, ParamName}, FromTuples) ->
    {_, Event} = lists:keyfind(Alias, 1, FromTuples),
    [Event, ParamName];
replace_select_aliases(Tuples, FromTuples) when is_list(Tuples) ->
    lists:map(fun({E1, E2, E3}) when is_tuple(E2) andalso is_tuple(E3) ->
        list_to_tuple(replace_select_aliases([E1, E2, E3], FromTuples));
                 ({E1, E2}) when is_tuple(E2) ->
                     {E1, list_to_tuple(replace_select_aliases(E2, FromTuples))};
                 ({E1, E2}) ->
                     case E1 of
                         integer -> {integer, E2};
                         float -> {float, E2};
                         atom -> {atom, E2};
                         nil -> case find_event_name(FromTuples, E2) of
                                    ubiquitous_event_param -> erlang:error({error, ubiquitous_event_param});
                                    EventName when is_atom(EventName) -> {EventName, E2}
                                end;
                         _ -> {_, Event} = lists:keyfind(E1, 1, lists:flatten(FromTuples)),
                             {Event, E2}
                     end;
                 ({EventParam}) ->
                     case length(FromTuples) of
                         1 -> {element(1, hd(FromTuples)), EventParam};
                         _ -> erlang:error({error, missing_event_qualifier})
                     end;
                 (nil) -> erlang:error({error, missing_event_qualifier});
                 (Atom) when is_atom(Atom) -> Atom
    end,
        Tuples).


replace_where_aliases(nil, _) ->
  nil;
replace_where_aliases({Op, Left, Right}, FromTuples) ->
  {Op, replace_where_aliases(Left, FromTuples), replace_where_aliases(Right, FromTuples)};
replace_where_aliases({E1, E2}, FromTuples) ->
  case E1 of
    integer -> {integer, E2};
    float -> {float, E2};
    atom -> {atom, E2};
    nil -> case find_event_name(FromTuples, E2) of
                 ubiquitous_event_param -> erlang:error({error, ubiquitous_event_param});
                 EventName when is_atom(EventName) -> {EventName, E2}
           end;
    _ -> {_, Event} = lists:keyfind(E1, 1, lists:flatten(FromTuples)),
                      {Event, E2}
<<<<<<< HEAD
=======
  %% TODO: the case when there is no event aliases
>>>>>>> 3c3553a16b4fe35bade90d95d3c18f8cae796924
  end.

remove_from_aliases([H | T], Acc) when is_tuple(H) ->
  remove_from_aliases(T, Acc ++ [element(2, H)]);
remove_from_aliases([H | T], Acc) when is_list(H) ->
  remove_from_aliases(T, Acc ++ [list_to_tuple(remove_from_aliases(H, []))]);
remove_from_aliases([], Acc) ->
  Acc.

find_event_name(FromTuples, ParamName) ->
    ParamOccur = lists:foldl(fun({_, EventName}, Acc) ->
        Params = EventName:get_param_names(),
        case lists:member(ParamName, Params) of
            true -> Acc + 1;
            false -> Acc
        end
    end,
        0,
        FromTuples),
    [{_, Event}] = case ParamOccur > 1 of
                true -> ubiquitous_event_param;
                false -> lists:filter(fun({_, EventName}) ->
                                          lists:member(ParamName, EventName:get_param_names())
                                      end,
                                      FromTuples)
              end,
    Event.