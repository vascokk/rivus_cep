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
    %% Transform Where Clause in Conjunctive Normal Form
    CNF =  rivus_cep_query_planner:to_cnf(Predicate),

    %% Get the list of predicates (list of conjuncts)
    PL =  rivus_cep_query_planner:predicates_to_list(CNF),
    ?assertEqual([{'or',{gt,{plus,{mult,{event1,eventparam1},{event2,eventparam2}},{integer,4}},
			 {event2,eventparam4}},
		   {eq,{event1,eventparam1},{event2,eventparam2}}},
		  {'or',{gt,{plus,{mult,{event1,eventparam1},{event2,eventparam2}},{integer,4}},
			 {event2,eventparam4}},
		   {gt,{event1,eventparam1},{event2,eventparam2}}}]
		 , PL),
    %% find the variables in each predicate(conjunct) and return a list of pairs: variables-predicate:
    %% [ {[variables], predicate}, {[variables],predicate}, {[variables],predicate}, .....  ]
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
    %{a,b} - choice "a or b"
    %[a,b] - sequence "a then b"	
    Pattern = [a,b,{{c, {c,d}},{x, {x,d}}}], %% a->b->((c->(c or d)) or (x->(x or d)))
    G = rivus_cep_query_planner:pattern_to_graph(Pattern),
    ?assertEqual([a,b],digraph:get_path(G,a,b)),
    ?assertEqual([b,c],digraph:get_path(G,b,c)),
    ?assertEqual([b,x],digraph:get_path(G,b,x)),
    ?assertEqual([a,b,c],digraph:get_path(G,a,c)),
    ?assertEqual([a,b,x],digraph:get_path(G,a,x)),
    ?assertEqual([c,c],digraph:get_path(G,c,c)),
    ?assertEqual([c,d],digraph:get_path(G,c,d)),
    ?assertEqual([x,x],digraph:get_path(G,x,x)),
    ?assertEqual([x,d],digraph:get_path(G,x,d)),
    ?assertEqual( [a,b,x,d],digraph:get_path(G,a,d)),
    ?assertNot(digraph:get_path(G,d,a)),
    ?assertNot(digraph:get_path(G,x,a)),
    ?assertNot(digraph:get_path(G,c,a)),
    ?assertNot(digraph:get_path(G,b,a)),
    ?assertNot(digraph:get_path(G,c,b)),
    ?assertNot(digraph:get_path(G,x,b)),
    ?assertNot(digraph:get_path(G,d,b)),
    ?assertNot(digraph:get_path(G,d,c)),
    
    ?assertEqual([x,c], digraph_utils:loop_vertices(G)),
    ?assertEqual([a,b,c], digraph:get_path(G, a, c)),
    ?assertEqual([a,b,x], digraph:get_path(G, a, x)),
    ?assertEqual(1, digraph:out_degree(G, a)),
    ?assertEqual(2, digraph:out_degree(G, b)),
    ?assertEqual(2, digraph:out_degree(G, c)),
    ?assertEqual(2, digraph:out_degree(G, x)),
    ?assertEqual(0, digraph:out_degree(G, d)),
    ?assertEqual(2, digraph:in_degree(G, d)),
    ?assertEqual(2, digraph:in_degree(G, c)),
    ?assertEqual(2, digraph:in_degree(G, x)),
    ?assertEqual(0, digraph:in_degree(G, a)),
    ?assertEqual(1, digraph:in_degree(G, b)),
    ?assertEqual([c,d], digraph_utils:reachable_neighbours([c],G)),
    ?assertEqual([x,d], digraph_utils:reachable_neighbours([x],G)),
    ?assertNot(digraph:get_cycle(G, a)),
    ?assertNot(digraph:get_cycle(G, b)),
    ?assertNot(digraph:get_cycle(G, d)),
    ?assertEqual([c], digraph:get_cycle(G, c)),
    ?assertEqual([x], digraph:get_cycle(G, x)),
    
    %% ?debugMsg(io_lib:format("Is tree: ~p~n",[digraph_utils:is_tree(G)])),
    %% ?debugMsg(io_lib:format("Top sort: ~p~n",[digraph_utils:topsort(G)])),
    ?debugMsg(io_lib:format("Subgraph  [a,d]: ~p~n",[digraph_utils:subgraph(G,[a,d],[{keep_labels,true}])])).

set_get_predicates_on_edge_test() ->
    Predicate = {'and',{eq,{event1,eventparam1},{event2,eventparam2}},
		       {eq,{event2,eventparam1},{event3,eventparam2}}},
    Pattern = [event1,event2,event3], % event1 -> event2 ->event3
    CNF =  rivus_cep_query_planner:to_cnf(Predicate),    
    %%?debugMsg(io_lib:format("CNF: ~p~n",[CNF])),
    PL =  rivus_cep_query_planner:predicates_to_list(CNF),
    %%?debugMsg(io_lib:format("PredicateList: ~p~n",[PL])),
    %%PredicateList: [{eq,{event1,eventparam1},{event2,eventparam2}},
    %%                {eq,{event2,eventparam1},{event3,eventparam2}}]
    
    PV =  rivus_cep_query_planner:get_predicate_variables(PL),
    %%?debugMsg(io_lib:format("PredicateVars: ~p~n",[PV])),
    %%PredicateVars: [{[{event1,eventparam1},{event2,eventparam2}],
                %%     {eq,{event1,eventparam1},{event2,eventparam2}}},
                %%    {[{event2,eventparam1},{event3,eventparam2}],
                %%     {eq,{event2,eventparam1},{event3,eventparam2}}}]

    G = digraph:new(),
    V1 = digraph:add_vertex(G, event1),
    V2 = digraph:add_vertex(G, event2),
    V3 = digraph:add_vertex(G, event3),
        
    Start = rivus_cep_query_planner:get_start_state(Pattern),
    ?assertEqual(event1, Start),
    E1 = digraph:add_edge(G, V1, V2, []),
    {NewPV, Label1} = rivus_cep_query_planner:set_predicates_on_edge(Start, V2, PV, G),
    digraph:add_edge(G, E1, V1, V2, Label1),

    E2 = digraph:add_edge(G, V2, V3, []),    
    {NewPV2, Label2} = rivus_cep_query_planner:set_predicates_on_edge(Start, V3, NewPV, G),    
    digraph:add_edge(G, E2, V2, V3, Label2),

    %%?debugMsg(io_lib:format("Label1: ~p~n",[Label1])),
    %%?debugMsg(io_lib:format("Label2: ~p~n",[Label2])),

    ?assertEqual(Label1, rivus_cep_query_planner:get_predicates_on_edge(G,V1,V2)),
    ?assertEqual(Label2, rivus_cep_query_planner:get_predicates_on_edge(G,V2,V3)).



assign_predicates_test() ->
    Stmt = [{pattern1},
		 {[{event1,eventparam1},
		   {event2,eventparam2},
		   {event2,eventparam3},
		   {event1,eventparam2}]},
		 {pattern,{[event1,{event2,event3}]}}, 
		 {{'and',{eq,{event1,eventparam1},{event2,eventparam2}},
		   {eq,{event2,eventparam1},{event3,eventparam2}}}},
		 {60}],
    
    Predicate = {'and',{eq,{event1,eventparam1},{event2,eventparam2}},
		   {eq,{event2,eventparam1},{event3,eventparam2}}},
    Pattern = [event1,{event2,event3}], % event1 -> (event2 or event3)
    CNF =  rivus_cep_query_planner:to_cnf(Predicate),
    ?debugMsg(io_lib:format("CNF: ~p~n",[CNF])),
    PL =  rivus_cep_query_planner:predicates_to_list(CNF),
    ?debugMsg(io_lib:format("PredicateList: ~p~n",[PL])),
    PV =  rivus_cep_query_planner:get_predicate_variables(PL),
    ?debugMsg(io_lib:format("PredicateVars: ~p~n",[PV])),    
    G = rivus_cep_query_planner:pattern_to_graph(Pattern),
    %%?debugMsg(io_lib:format("G edges: ~p~n",[digraph:edges(G)])),
    ?assertEqual([event1,event2],digraph:get_path(G, event1, event2)),
    ?assertNot(digraph:get_path(G, event2, event3)),
    ?assertEqual([event1,event3],digraph:get_path(G, event1, event3)),
    ?assertNot(digraph:get_path(G, event3, event2)),
    ?assertNot(digraph:get_path(G, event3, event1)),
    ?assertNot(digraph:get_path(G, event2, event1)),
    FsmStates = lists:reverse(digraph_utils:preorder(G)),
    ?debugMsg(io_lib:format("FSM States: ~p~n",[FsmStates])),
    lists:foreach(fun(State) -> ?debugMsg(io_lib:format("Reaching Vertex:~p Path: ~p~n",[State, digraph_utils:reachable([State], G)])) end, FsmStates),
    %% PL = [{eq,{event1,eventparam1},{event2,eventparam2}},
    %%       {eq,{event2,eventparam1},{event3,eventparam2}}]
    PL,
    ?debugMsg(io_lib:format("Is tree: ~p~n",[digraph_utils:is_tree(G)])),
    ?debugMsg(io_lib:format("Top sort: ~p~n",[digraph_utils:topsort(G)])).


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

get_start_state_test() ->
    ?assertEqual(a, rivus_cep_query_planner:get_start_state([a,b,c])),
    ?assertError(badpattern, rivus_cep_query_planner:get_start_state([[a,b],c,d])),
    ?assertError(badpattern, rivus_cep_query_planner:get_start_state([{a,b},c,d])).


    
    
    
    
