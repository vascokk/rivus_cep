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
-include_lib("eunit/include/eunit.hrl").

-export([analyze/1, get_join_keys/2,
	 to_cnf/1,
	 predicates_to_list/1,
	 get_predicate_variables/1,
	 sort_predicates/1,
	 pattern_to_graph/2,
	 get_start_state/1,
	 set_predicates_on_edge/4,
	 get_predicates_on_edge/3]).


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
distribute_or_over_and({'or', {'and', Q, R}, P}) ->
    {'and', distribute_or_over_and({'or', P, Q}), distribute_or_over_and({'or', P, R})};
distribute_or_over_and(Predicate) ->
    Predicate.

pattern_to_graph(PredVars, Pattern) ->
    G = digraph:new(),
    %%Start = digraph:add_vertex(G,  get_start_state(Pattern)),
    pattern_to_graph(start, PredVars, Pattern, G).


pattern_to_graph(start, PredVars, [First,Second|T], G) when is_atom(First), is_atom(Second) ->
    FV = digraph:add_vertex(G, First),
    Start = FV,
    SV = digraph:add_vertex(G, Second),

    E = digraph:add_edge(G, FV, SV, []),
    {NewPredVars, Labels} = set_predicates_on_edge(Start, SV, PredVars, G),
    digraph:add_edge(G, E, FV, SV, Labels),
    pattern_to_graph(Start, NewPredVars, [Second|T], G);    
pattern_to_graph(Start, PredVars, [First,Second|T], G) when is_atom(First), is_atom(Second) ->
    FV = digraph:add_vertex(G, First),
    SV = digraph:add_vertex(G, Second),

    E = digraph:add_edge(G, FV, SV, []),
    {NewPredVars, Labels} = set_predicates_on_edge(Start, SV, PredVars, G),
    digraph:add_edge(G, E, FV, SV, Labels),
    pattern_to_graph(Start, NewPredVars, [Second|T], G);
pattern_to_graph(Start, PredVars, [First,Second|T], G) when is_atom(First), is_tuple(Second) -> 
    FV = digraph:add_vertex(G, First),
    NewPV = lists:foldl(fun(V, PV) when is_atom(V) ->
				SV = digraph:add_vertex(G, V),

				E = digraph:add_edge(G, FV, SV, []),
				{NewPredVars, Labels} = set_predicates_on_edge(Start, SV, PV, G),
				digraph:add_edge(G, E, FV, SV, Labels),
				NewPredVars;
			   
			        %%digraph:add_edge(G, FV, SV);
			   (T, PV) when is_tuple(T) ->
				L = tuple_to_list(T),
				SV = digraph:add_vertex(G, hd(L)),

				E = digraph:add_edge(G, FV, SV, []),
				{NewPredVars, Labels} = set_predicates_on_edge(Start, SV, PV, G),
				digraph:add_edge(G, E, FV, SV, Labels),
				
				%%digraph:add_edge(G, FV, SV),
				pattern_to_graph(Start, NewPredVars, L, G),
				NewPredVars
			end, PredVars, tuple_to_list(Second)),
    pattern_to_graph(Start, NewPV, [Second|T], G);
pattern_to_graph(Start, PredVars, [First,Second|_], _) when is_list(First), is_list(Second)->
    erlang:error(unsupported_pattern);
pattern_to_graph(Start, PredVars, [_|[]], G) ->
    G.

set_predicates_on_edge(Start, Second, PredVars, G) ->
    CurrentPath = ordsets:from_list(digraph:get_path(G, Start, Second)),
    {NewPredVars,Labels} = lists:foldl(fun({Vars, Predicate}, {PVSet, Acc}) ->
					       case check_path(CurrentPath, Vars) of
						   true -> {remove_predicate({Vars,Predicate}, PVSet),
							     Acc ++ [Predicate]};
						   false -> {PVSet, Acc}
					       end
				       end, {ordsets:from_list(PredVars), []}, PredVars),
    {ordsets:to_list(NewPredVars), Labels}.


%% check if all the events in the current predicate (Vars) are part of the current path
check_path(CurrentPath, Vars) ->
    lists:all(fun({Event,_}) -> ordsets:is_element(Event, CurrentPath) end, Vars).

%% current predicate will be assigned to edge. remove it from the list ov available predicates
remove_predicate(Key, PVSet) ->
  ordsets:del_element(Key, PVSet).

get_predicates_on_edge(G, V1, V2) ->
    [Edge] = lists:filter(fun(E) ->
				   {E_tmp, V1_tmp, V2_tmp, _ } = digraph:edge(G,E),
				   V2_tmp == V2
			   end, digraph:out_edges(G,V1)),
    {_,_,_,Label} = digraph:edge(G, Edge),
    Label.

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

get_start_state(Pattern) when is_atom(hd(Pattern)) ->
    hd(Pattern);
get_start_state(_) ->
    erlang:error(badpattern).

