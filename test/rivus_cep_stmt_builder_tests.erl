-module(rivus_cep_stmt_builder_tests).
-compile([debug_info, export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

    %% cep_window:new(as, slide, 60),
    %% cep_window:update(as, {event1, 10,b,c}), % *
    %% cep_window:update(as, {event1, 15,bbb,c}),
    %% cep_window:update(as, {event1, 20,b,c}), % *
    %% cep_window:update(as, {event2, 30,b,cc,d}),
    %% cep_window:update(as, {event2, 40,bb,cc,dd}),

    %% %% WITHIN clause
    %% {Reservoir, Oldest} = cep_window:get_window(as),
    %% Ms1 = ets:fun2ms(fun({{Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==event1  -> Value end),
    %% Ms2 = ets:fun2ms(fun({{Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==event2  -> Value end),

    %% %% FROM clause
    %% QH1 = ets:table(Reservoir, [{traverse, {select, Ms1}}]),
    %% QH2 = ets:table(Reservoir, [{traverse, {select, Ms2}}]),

    %% %% WHERE clause
    %% QH = qlc:q([ Е1 || Е1 <- QH1, Е2 <- QH2, element(3,Е1) == element(3,Е2)]),

    %% %% SELECT clause
    %% ResSet = lists:foldl(fun(X, Acc) -> sets:add_element(X,Acc) end, sets:new(),  qlc:e(QH)),
    %% Sum = lists:foldl(fun(E, Acc) -> element(2, E) + Acc end, 0, sets:to_list(ResSet)),

    %% ?assertEqual(30, Sum ).   

build_stmt_test() ->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
        {ok, Tokens, Endline} = rivus_cep_scanner:string(
				  "define correlation stmt1 as
                                   select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
                                   from event1 as ev1, event2 as ev2
                                   where ev1.eventparam1 = ev2.eventparam2 and ev1.eventparam1 > ev2.eventparam2
                                   within 60 seconds; ", 1),    
    ?assertEqual({ok,[{attribute,1,module,stmt1},
						{[{event1,eventparam1},
						  {event2,eventparam2},
						  {event2,eventparam3},
						  {event1,eventparam2}]},
						 {[event1,event2]},
						 {{'and',{eq,{event1,eventparam1},{event2,eventparam2}},
						  {gt,{event1,eventparam1},{event2,eventparam2}}}},
						 {60}]}, rivus_cep_parser:parse(Tokens) ),
    {ok, [Module, Select, From, {Where}, Within]} = rivus_cep_parser:parse(Tokens),
    
    WhereRes = rivus_cep_stmt_builder:build_where_clause(Where),    
    %%?debugMsg(io_lib:format("Where: ~p~n ",[WhereRes])).
    ?assertEqual(re:replace("((event1:get_param_by_name(event1, eventparam1) == event2:get_param_by_name(event2,eventparam2)) andalso (event1:get_param_by_name(event1,eventparam1) > event2:get_param_by_name(event2,eventparam2)))", "\\s+", "", [global,{return,list}]),
		 re:replace(WhereRes, "\\s+", "", [global,{return,list}])),

    WithinRes = rivus_cep_stmt_builder:build_within_clause(Module), 
    ?assertEqual(re:replace("{Reservoir, Oldest} = rivus_cep_window:get_window( stmt1 ),
                             MatchSpecs = [ create_match_spec(Event, Oldest) || Event<- State#state.events]", "\\s+", "", [global,{return,list}]),
		 re:replace(WithinRes, "\\s+", "", [global,{return,list}])),
    
    FromRes = rivus_cep_stmt_builder:build_from_clause(From), 
    ?assertEqual(re:replace("FromQueryHandlers = [ create_from_qh(MS, Reservoir) || MS <- MatchSpecs]", "\\s+", "",
			    [global,{return,list}]),
		 re:replace(FromRes, "\\s+", "", [global,{return,list}])),
	
    QueryHandler = rivus_cep_stmt_builder:build_qh(Select, From, {Where}),
    ?assertEqual("QH = qlc:q([{(element(1,E1)):get_param_by_name(E1,eventparam1),(element(1,E2)):get_param_by_name(E2,eventparam2),(element(1,E2)):get_param_by_name(E2,eventparam3),(element(1,E1)):get_param_by_name(E1,eventparam2)} || E1<-hd(FromQueryHandlers), E2<-hd(tl(FromQueryHandlers)), (((element(1,E1)):get_param_by_name(E1,eventparam1) == (element(1,E2)):get_param_by_name(E2,eventparam2)) andalso ((element(1,E1)):get_param_by_name(E1,eventparam1) > (element(1,E2)):get_param_by_name(E2,eventparam2)))])", QueryHandler).


build_select_clause_test()->
    Select = [{event1,eventparam1},
	       {sum,{event2,eventparam2}},
	       {minus,{plus,{plus,{plus,{event1,eventparam1},
				   {mult,{event2,eventparam2},{integer,5}}},
			     {integer,6}},
		       {event2,eventparam4}},
		{event1,eventparam1}},
	       {count,{event2,eventparam3}}],
    EventVariables =  [{"E1", "event1"}, {"E2","event2"}, {"E3","event3"}],
    Res = rivus_cep_stmt_builder:build_select_clause(Select, EventVariables, ""),
    %% ?debugMsg(io_lib:format("Select: ~p~n",[Res])),
    ?assertEqual("(element(1,E1)):get_param_by_name(E1,eventparam1),{sum, {(element(1,E2)):get_param_by_name(E2,eventparam2)}},{minus,{{plus,{{plus,{{plus,{(element(1,E1)):get_param_by_name(E1,eventparam1)}, {mult,{(element(1,E2)):get_param_by_name(E2,eventparam2)}, 5}}}, 6}}, (element(1,E2)):get_param_by_name(E2,eventparam4)}}, (element(1,E1)):get_param_by_name(E1,eventparam1)},{count,{(element(1,E2)):get_param_by_name(E2,eventparam3)}}", Res).
	      
