%% ; -*- mode: Erlang;-*-
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
-module(rivus_cep_query_planner).

-include("rivus_cep.hrl").

-export([analyze/1, get_join_keys/2,
	 to_cnf/1,
	 predicates_to_list/1,
	 get_predicate_variables/1,
	 sort_predicates/1,
	 pattern_to_graph/1,
	 get_first_event_from_pattern/1]).


analyze( [{QueryName}, {SelectClause}, FromClause, {WhereClause}, {WithinClause}]) ->
    Events = case FromClause of
		 {pattern, {List}} -> List;
		 {List} -> List
	     end,
    JoinKeys = get_join_keys(Events, WhereClause),
    #query_plan{join_keys = JoinKeys}.


sort_predicates(WhereClause) ->
    PL = predicates_to_list(to_cnf(WhereClause)),
    VL = get_predicate_variables(lists:flatten(PL)).

get_predicate_variables(PL) ->
    [{get_predicate_variables(Predicate, ordsets:new()), Predicate} || Predicate <- PL].

get_predicate_variables({_, Left, Right}, Acc) ->
    NewAcc = get_predicate_variables(Left, Acc),
    get_predicate_variables(Right, NewAcc);
get_predicate_variables({neg,Predicate}, Acc) ->
    {neg,get_predicate_variables(Predicate, Acc)};
get_predicate_variables({EventName, Value}, Acc) ->
    ordsets:add_element({EventName, Value}, Acc);
get_predicate_variables(_, Acc) ->
    Acc.


predicates_to_list({'and', P, Q}) ->
    predicates_to_list(P) ++ predicates_to_list(Q);
predicates_to_list(Predicate) ->
    [Predicate].

    
to_cnf(WhereClause) ->
    P = move_not_inwards(WhereClause),
    distribute_or_over_and(P).

move_not_inwards({neg,{neg,Predicate}}) ->
    move_not_inwards(Predicate);
move_not_inwards({neg, {'or', P, Q}}) ->
    {'and', move_not_inwards({neg, P}), move_not_inwards({neg, Q})};
move_not_inwards({neg, {'and', P, Q}}) ->
    {'or', move_not_inwards({neg, P}), move_not_inwards({neg, Q}) };    
move_not_inwards(Predicate) ->
    Predicate.
    

distribute_or_over_and({'or', P, {'and', Q, R}}) ->
    {'and', distribute_or_over_and({'or', P, Q}), distribute_or_over_and({'or', P, R})};
distribute_or_over_and(Predicate) ->
    Predicate.

pattern_to_graph(Pattern) ->
    pattern_to_graph(Pattern, digraph:new()).

pattern_to_graph([First,Second|T], G) when is_atom(First), is_atom(Second) ->
    FV = digraph:add_vertex(G, First),
    SV = digraph:add_vertex(G, Second),
    digraph:add_edge(G, FV, SV),
    pattern_to_graph([Second|T], G);
pattern_to_graph([First,Second|T], G) when is_atom(First), is_tuple(Second) -> 
    FV = digraph:add_vertex(G, First),
    lists:foreach(fun(V) when is_atom(V) ->
			  SV = digraph:add_vertex(G, V),
			  digraph:add_edge(G, FV, SV);
		     (T) when is_tuple(T) ->
			  L = tuple_to_list(T),
			  SV = digraph:add_vertex(G, hd(L)),
			  digraph:add_edge(G, FV, SV),
			  pattern_to_graph(L, G)
		  end, tuple_to_list(Second)),
    pattern_to_graph([Second|T], G);
pattern_to_graph([First,Second|_], _) when is_list(First), is_list(Second)->
    erlang:error(unsupported_pattern);
pattern_to_graph([_|[]], G) ->
    G.

get_first_event_from_pattern(Pattern) ->
    false.

get_join_keys(Events, Predicate) ->
    Keys = [{Event, ordsets:to_list(get_join_keys(Predicate, Event, ordsets:new()))} || Event <- Events],
    lists:foldl(fun({Event, Params}, Acc) -> orddict:store(Event, Params, Acc) end, orddict:new(), Keys).

get_join_keys({_, Left, Right}, Event, Acc) ->
    NewAcc = get_join_keys(Left, Event, Acc),
    get_join_keys(Right, Event, NewAcc);
get_join_keys({neg,Predicate}, Event, Acc) ->
    {neg, get_join_keys(Predicate, Event, Acc)};
get_join_keys({EventName, Value}, Event, Acc) ->
    case EventName of
    	Event -> ordsets:add_element(Value, Acc);
    	_ -> Acc
    end.
