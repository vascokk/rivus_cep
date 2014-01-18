-module(rivus_cep_query_planner_tests).
-compiler([export_all]).
-include_lib("eunit/include/eunit.hrl").


to_cnf_1_test() ->
    Predicate = {neg,{'or', p,q}},
    ?assertEqual({'and',{neg,p}, {neg,q}}, rivus_cep_query_planner:to_cnf(Predicate)).

to_cnf_2_test() ->
    Predicate = {neg,{'or',
		      {neg,{'or',p1,q1}},
		      {neg, {'and',p2,q2}}}},
    ?assertEqual({'and',{'or',p1,q1},{'and',p2,q2}}, rivus_cep_query_planner:to_cnf(Predicate)).

to_cnf_3_test() ->
    Predicate = {'or', p, {'and', q, r}},
    ?assertEqual({'and', {'or', p, q}, {'or', p, r}}, rivus_cep_query_planner:to_cnf(Predicate)).

to_cnf_4_test() ->
    Predicate = {neg,{'or', p, {'and', q, r}}},
    ?assertEqual({'and',{neg,p},{'or',{neg,q},{neg,r}}}, rivus_cep_query_planner:to_cnf(Predicate)).

predicates_to_list_1_test()->
    Predicate = {'and', p, q},
    CNF =  rivus_cep_query_planner:to_cnf(Predicate),
    ?assertEqual([p,q], rivus_cep_query_planner:predicates_to_list(CNF)).

predicates_to_list_2_test()->
    Predicate = {'and', p, {'and', q, r}},
    CNF =  rivus_cep_query_planner:to_cnf(Predicate),
    ?assertEqual([p, q, r], rivus_cep_query_planner:predicates_to_list(CNF)).

predicates_to_list_3_test()->
    Predicate = {'and', {'or', p1,p2}, {'and', q, r}},
    CNF =  rivus_cep_query_planner:to_cnf(Predicate),
    ?assertEqual([{'or',p1,p2}, q, r], rivus_cep_query_planner:predicates_to_list(CNF)).


get_predicate_variables_test() ->
     Predicate = {'or',{gt,{plus,{mult,{event1,eventparam1},{event2,eventparam2}},{integer,4}},
			      {event2,eventparam4}},
			{'and',{eq,{event1,eventparam1},{event2,eventparam2}},
			 {gt,{event1,eventparam1},{event2,eventparam2}}}},
    CNF =  rivus_cep_query_planner:to_cnf(Predicate),
    PL =  rivus_cep_query_planner:predicates_to_list(CNF),
    ?assertEqual([{'or',{gt,{plus,{mult,{event1,eventparam1},{event2,eventparam2}},{integer,4}},
			 {event2,eventparam4}},
		   {eq,{event1,eventparam1},{event2,eventparam2}}},
		  {'or',{gt,{plus,{mult,{event1,eventparam1},{event2,eventparam2}},{integer,4}},
			 {event2,eventparam4}},
		   {gt,{event1,eventparam1},{event2,eventparam2}}}]
		 , PL),
    %% [ {[variables], predicate}, {[variables],predicate}, .....  ]
    ?assertEqual([{[{event1,eventparam1},
		   {event2,eventparam2},
		   {event2,eventparam4},
		   {integer,4}],
		   {'or',{gt,{plus,{mult,{event1,eventparam1},{event2,eventparam2}},{integer,4}},
			 {event2,eventparam4}},
		   {eq,{event1,eventparam1},{event2,eventparam2}}}
		  },
		  {[{event1,eventparam1},
		   {event2,eventparam2},
		   {event2,eventparam4},
		   {integer,4}],
		   {'or',{gt,{plus,{mult,{event1,eventparam1},{event2,eventparam2}},{integer,4}},
			 {event2,eventparam4}},
		   {gt,{event1,eventparam1},{event2,eventparam2}}}
		  }],
		 rivus_cep_query_planner:get_predicate_variables(PL)).

pattern_to_graph_test() ->
    Pattern = [a,b,[{c, [c,d]},{x, [x,d]}]],
    G = rivus_cep_query_planner:pattern_to_graph(Pattern),
    ?assertEqual([x,c], digraph_utils:loop_vertices(G)),
    ?assertEqual([a,b,c], digraph:get_path(G, a, c)),
    ?assertEqual([a,b,x], digraph:get_path(G, a, x)),
    ?assertEqual([c,d], digraph_utils:reachable_neighbours([c],G)),
    ?assertEqual([x,d], digraph_utils:reachable_neighbours([x],G)).
    
get_join_keys_test() ->
    Predicate =  {eq,{event1,eventparam1},{event2, eventparam1}},
    Events = [event1, event2],
    ?assertEqual([{event1,[eventparam1]}, {event2, [eventparam1]}], orddict:to_list(rivus_cep_query_planner:get_join_keys(Events, Predicate))).

get_join_keys_2_test() ->
    Predicate =  {'and',{eq,{event1,eventparam1},{event2,eventparam2}},
			{gt,{event1,eventparam1},{event2,eventparam2}}},
    Events = [event1, event2],
    ?assertEqual([{event1,[eventparam1]}, {event2, [eventparam2]}], orddict:to_list(rivus_cep_query_planner:get_join_keys(Events, Predicate))).

get_join_keys_3_test() ->
    Predicate = {'or',{gt,{plus,{mult,{event1,eventparam1},{event2,eventparam2}},{integer,4}},
			      {event2,eventparam4}},
			{'and',{eq,{event1,eventparam1},{event2,eventparam2}},
			 {gt,{event1,eventparam1},{event2,eventparam2}}}},
    Events = [event1, event2],
    ?assertEqual([{event1,[eventparam1]}, {event2, [eventparam2,eventparam4]}], orddict:to_list(rivus_cep_query_planner:get_join_keys(Events, Predicate))).
    
    
    
