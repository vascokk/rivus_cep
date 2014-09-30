%% ; -*- mode: Erlang;-*-
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
-module(rivus_cep_query_planner).

-include("rivus_cep.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([analyze/1, get_join_keys/1,
	 to_cnf/1,
	 to_single_predicate/1,
	 predicates_to_list/1,
	 get_predicate_variables/1,
	 sort_predicates/1,
	 pattern_to_graph/2,
	 get_start_state/1,
	 set_predicates_on_edge/4,
	 get_predicates_on_edge/3,
	 get_events_on_path/2,
	 is_first/2,
	 is_next/3,
	 is_last/2]).


analyze( [{_QueryName}, {SelectClause}, FromClause, {WhereClause}, {_Within, _WindowType}, {_Filters}]) ->
    case FromClause of
	{pattern, {Pattern}} -> CNF =  to_cnf(WhereClause),    
				PL =  predicates_to_list(CNF),
				PV =  get_predicate_variables(PL),
				G =  pattern_to_graph(PV, Pattern),				
				#query_plan{fsm = G};
	{_EventsList} -> #query_plan{join_keys = get_join_keys(WhereClause), has_aggregations = is_aggregation_query(SelectClause)}
    end.

sort_predicates(WhereClause) ->
    PL = predicates_to_list(to_cnf(WhereClause)),
    get_predicate_variables(lists:flatten(PL)).

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

to_single_predicate(Predicates) ->
    lists:foldl(fun([],Acc) -> Acc;
		   (P, Acc) when P /= []-> {'and', Acc, P}
		end,
		hd(Predicates), tl(Predicates)).

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
    {G,_} = pattern_to_graph(start, PredVars, Pattern, digraph:new()),
    G.

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
			   			       
			   (Tup, PV) when is_tuple(Tup) ->
				L = tuple_to_list(Tup),
				SV = digraph:add_vertex(G, hd(L)),

				E = digraph:add_edge(G, FV, SV, []),
				{NewPredVars, Labels} = set_predicates_on_edge(Start, SV, PV, G),
				digraph:add_edge(G, E, FV, SV, Labels),
								
				{_, NewPredVars2} = pattern_to_graph(Start, NewPredVars, L, G),
				NewPredVars2
			end, PredVars, tuple_to_list(Second)),
    pattern_to_graph(Start, NewPV, [Second|T], G);
pattern_to_graph(_Start, _PredVars, [First,Second|_], _) when is_list(First), is_list(Second)->
    erlang:error(unsupported_pattern);
pattern_to_graph(_Start, [], _, G) ->
	{G, []};
pattern_to_graph(_Start, PredVars, [_|[]], _G) ->
    erlang:error({unsupported_pattern, "Cannot assign some of the predicates.", PredVars}).


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
				   {_E_tmp, _V1_tmp, V2_tmp, _ } = digraph:edge(G,E),
				   V2_tmp == V2
			   end, digraph:out_edges(G,V1)),
    {_,_,_,Label} = digraph:edge(G, Edge),
    Label.


get_join_keys(Predicate) ->
    {L, R, Dict} = get_join_keys(start, Predicate, {false, false, orddict:new()}),
    case {L,R} of
	{true,true} -> Set = lists:foldl(fun({Name, _}, Acc) -> sets:add_element(Name, Acc) end,
					  sets:new(),
					  orddict:to_list(Dict)),
		       case length(sets:to_list(Set)) < 2 of
			   true -> [];
			   false -> Dict
		       end;
	_ -> []
    end.

get_join_keys(_Side, {_, Left, Right}, {L, R, Acc}) ->
    {L1, R1, NewAcc1} = get_join_keys(left, Left, {L, R, Acc}),
    {L2, R2, NewAcc2} = get_join_keys(right, Right, {L1, R1, Acc}),
    List = orddict:to_list(NewAcc1) ++ orddict:to_list(NewAcc2),
    NewAcc = lists:foldl(fun({Name, Val}, FAcc) ->
				 case orddict:is_key(Name, FAcc) of
				     true -> orddict:update(Name, fun(Old) ->
									  sets:to_list(sets:union(sets:from_list(Val),sets:from_list(Old)))
								  end, hd(Val), FAcc);
				     false -> orddict:store(Name, lists:flatten(Val), FAcc)
				 end
			 end,
			 orddict:new(),
			 lists:flatten(List)
			),
    {L2, R2, NewAcc};
get_join_keys(Side, {neg, Predicate}, {L, R, Acc}) ->
    get_join_keys(Side, Predicate, {L, R, Acc});
get_join_keys(_, {integer, _Value}, {L, R, Acc}) ->
    {L,R, Acc};
get_join_keys(_, {float, _Value}, {L, R, Acc}) ->
    {L,R, Acc};
get_join_keys(_, {string, _Value}, {L, R, Acc}) ->
    {L,R, Acc};
get_join_keys(Side, {EventName, Value}, {L,R, Acc}) ->
    List = ordsets:to_list(ordsets:add_element({EventName, Value}, ordsets:from_list(orddict:to_list(Acc)))),
   
    NewAcc = lists:foldl(fun({Name, Val}, FAcc) ->
				 case orddict:is_key(Name, FAcc) of
				     true -> orddict:append(Name, Val, FAcc);
				     false -> orddict:store(Name, [Val], FAcc)
				 end
			 end,
			 orddict:new(),
			 lists:flatten(List)
			),
    case Side of
	left -> {true, R, NewAcc};
	right -> {L, true, NewAcc}
    end;
get_join_keys(_, nil, _) ->
    {false, false, []}.


%% {[{event1,eventparam1},
%% 	{sum,{event2,eventparam2}},
%% 	{minus,{plus,{plus,{plus,{event1,eventparam1},
%% 			    {mult,{event2,eventparam2},{integer,5}}},
%% 		      {integer,6}},
%% 		{event2,eventparam4}},
%% 	 {event1,eventparam1}},
%% 	{count,{event2,eventparam3}}]}
is_aggregation_query(sum) ->
    true;
is_aggregation_query(count) ->
    true;
is_aggregation_query(min) ->
    tru;
is_aggregation_query(max) ->
    true;
is_aggregation_query({L,R}) ->
    is_aggregation_query(L) or is_aggregation_query(R);    
is_aggregation_query({Op,L,R}) ->
    is_aggregation_query(Op) or is_aggregation_query(L) or is_aggregation_query(R);    
is_aggregation_query([H|[]]) ->
    is_aggregation_query(H);	    
is_aggregation_query([H|L]) ->
    is_aggregation_query(H) or is_aggregation_query(L);
is_aggregation_query(_) ->
    false.

  

is_first(G, StateName) ->
    hd(digraph_utils:topsort(G)) == StateName.

is_next(G, CurrentState, StateName) ->
   case digraph:get_path(G, CurrentState, StateName) of
       false -> false;
           _ -> true
   end.

is_last(G, StateName) ->
    lists:last(digraph_utils:topsort(G)) == StateName.


get_start_state(Pattern) when is_list(Pattern) andalso is_atom(hd(Pattern)) ->
    hd(Pattern);
get_start_state(G) when is_tuple(G) ->
    case digraph_utils:topsort(G) of
	false -> erlang:error({badpattern, "Unsupported pattern."});
	L when is_list(L) -> hd(L)
    end;
get_start_state(_) ->
    erlang:error({badpattern, "Unsupported pattern."}).

get_events_on_path(G, EventName) ->
    digraph:get_path(G,get_start_state(G), EventName).
