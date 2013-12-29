-module(rivus_cep_window_tests).

-compile([debug_info, export_all]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("stdlib/include/qlc.hrl").

-include_lib("../deps/folsom/include/folsom.hrl").

window_test_() ->
    {setup,
     fun () -> folsom:start() end,
     fun (_) -> folsom:stop() end,

     [{"Create new window & insert event",
       fun new/0},
      {"Create sliding window, insert, remove after 2 second",
       fun new_sliding/0},
      {"Select events where event.param = a",
       fun select/0},
      {"Select events where event.param = a and time of query > slide time",
       fun select_outside/0},
      {"Test select from window using qlc",
       fun select_using_qlc/0},
      {"Select events where event1.param2 = event2.param2",
       fun select_where_op_equal/0},
      {"Select Count(event)",
       fun select_count/0},
      {"Select Count(event) where event1.param2 = event2.param2",
       fun select_count_where_op_equal/0},
      {"Select Sum(event1.param1) where event1.param2 = event2.param2",
       fun select_sum_where_op_equal/0},
      {"Dynamic MatchSpec and QueryHandler",
       fun dynamic_qh/0}]
    }.

new() ->
     rivus_cep_window:new(testevent),
     rivus_cep_window:update(testevent, <<"blabla1">>),
     rivus_cep_window:update(testevent, <<"blabla2">>),
     rivus_cep_window:update(testevent, <<"blabla3">>),
     ?assertEqual([<<"blabla1">>, <<"blabla2">>,<<"blabla3">>],rivus_cep_window:get_values(testevent)).

new_sliding() ->   
     rivus_cep_window:new(testevent_slide, slide, 2), %% 2 seconds sliding-window
     rivus_cep_window:update(testevent_slide, <<"blabla1">>),
     rivus_cep_window:update(testevent_slide, <<"blabla2">>),
     rivus_cep_window:update(testevent_slide, <<"blabla3">>),
     ?assertEqual([<<"blabla1">>,<<"blabla2">>,<<"blabla3">>],rivus_cep_window:get_values(testevent_slide)),
     timer:sleep(4000),
     ?assertEqual([],rivus_cep_window:get_values(testevent_slide)).

select_2() ->   
     rivus_cep_window:new(select, slide, 2), %% 2 seconds sliding-window
     rivus_cep_window:update(select, <<"blabla1">>),
     rivus_cep_window:update(select, <<"blabla2">>),
     rivus_cep_window:update(select, <<"blabla3">>),
     ?assertEqual([<<"blabla1">>,<<"blabla2">>,<<"blabla3">>],rivus_cep_window:select(select, "blah")).

select() ->
     rivus_cep_window:new(w_select, slide, 2),
     rivus_cep_window:update(w_select, {event1, a,b,c}),
     rivus_cep_window:update(w_select, {event1, aa,b,c}),
     rivus_cep_window:update(w_select, {event1, a,bbb,c}),
     ?assertEqual([{event1, a,b,c}, {event1, a,bbb,c}], rivus_cep_window:select(w_select, "blah")).
    
select_outside() ->
    rivus_cep_window:new(w_select2, slide, 2),
    rivus_cep_window:update(w_select2, {event1, a,b,c}),
    rivus_cep_window:update(w_select2, {event1, aa,b,c}),
    rivus_cep_window:update(w_select2, {event1, a,bbb,c}),
    ?assertEqual([{event1, a,b,c}, {event1, a,bbb,c}], rivus_cep_window:select(w_select2, "blah")),
    timer:sleep(4000),
    ?assertEqual([],rivus_cep_window:select(w_select2, "blah")).


select_using_qlc() ->
    rivus_cep_window:new(as, slide, 60),
    rivus_cep_window:update(as, {event1, a,b,c}),
    rivus_cep_window:update(as, {event1, aa,b,c}),
    rivus_cep_window:update(as, {event1, a,bbb,c}),
    rivus_cep_window:update(as, {event2, a,bb,cc,d}),
    rivus_cep_window:update(as, {event2, a,bb,cc,dd}),

    Window = folsom_metrics_histogram:get_value(as),
    Sample = Window#histogram.sample,
    Size = Sample#slide.window,
    Reservoir = Sample#slide.reservoir,    
    Oldest = folsom_sample_slide:moment() - Size,

    Ms1 = ets:fun2ms(fun({{Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==event1  -> Value end),
    Ms2 = ets:fun2ms(fun({{Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==event2  -> Value end),
 
    QH1 = ets:table(Reservoir, [{traverse, {select, Ms1}}]),
    QH2 = ets:table(Reservoir, [{traverse, {select, Ms2}}]),

    QH = qlc:q([ {X,Y} || X <- QH1, Y <- QH2, element(2,X) == element(2,Y)]),
    ResSet = lists:foldl(fun({X,Y}, Acc) -> sets:add_element(Y, sets:add_element(X,Acc)) end, sets:new(),  qlc:e(QH)),
    Res =  sets:to_list(ResSet),
    ?assertEqual([ {event1, a,b,c}, {event1, a, bbb, c}, {event2, a,bb,cc,d}, {event2, a,bb,cc,dd}], Res).

select_where_op_equal() ->
    rivus_cep_window:new(as, slide, 60),
    rivus_cep_window:update(as, {event1, a,b,c}),
    rivus_cep_window:update(as, {event1, aa,b,c}),
    rivus_cep_window:update(as, {event1, a,bbb,c}),
    rivus_cep_window:update(as, {event2, a,bb,cc,d}),
    rivus_cep_window:update(as, {event2, a,bb,cc,dd}),
    
    {Reservoir, Oldest} = rivus_cep_window:get_window(as),
    
    Ms1 = ets:fun2ms(fun({{Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==event1 -> Value end),
    Ms2 = ets:fun2ms(fun({{Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==event2 -> Value end),
 
    QH1 = ets:table(Reservoir, [{traverse, {select, Ms1}}]),
    QH2 = ets:table(Reservoir, [{traverse, {select, Ms2}}]),

    QH = qlc:q([ {X,Y} || X <- QH1, Y <- QH2, element(2,X) == element(2,Y)]),
    ResSet = lists:foldl(fun({X,Y}, Acc) -> sets:add_element(Y, sets:add_element(X,Acc)) end, sets:new(),  qlc:e(QH)),
    Res =  sets:to_list(ResSet),
    ?assertEqual([ {event1, a,b,c}, {event1, a, bbb, c}, {event2, a,bb,cc,d}, {event2, a,bb,cc,dd}], Res).

select_count() ->
    rivus_cep_window:new(as, slide, 60),
    rivus_cep_window:update(as, {event1, a,b,c}),
    rivus_cep_window:update(as, {event1, aa,b,c}),
    rivus_cep_window:update(as, {event1, a,bbb,c}),
    rivus_cep_window:update(as, {event2, a,bb,cc,d}),
    rivus_cep_window:update(as, {event2, a,bb,cc,dd}),
    
    {Reservoir, Oldest} = rivus_cep_window:get_window(as),
    
    Ms1 = ets:fun2ms(fun({{Time,'_'},Value}) when Time >= Oldest, (element(1,Value)==event1 orelse  element(1,Value)==event2) -> Value end),
    QH1 = ets:table(Reservoir, [{traverse, {select, Ms1}}]),
    QH = qlc:q([ X || X <- QH1]),
    Count =  length(qlc:e(QH)),
    %%?assertEqual(5, lists:foldl(fun(_, Count) -> Count + 1 end, 0, qlc:e(QH)) ).
    ?assertEqual(5, Count ).

select_count_where_op_equal() ->
    rivus_cep_window:new(as, slide, 60),
    rivus_cep_window:update(as, {event1, a,b,c}),
    rivus_cep_window:update(as, {event1, aa,b,c}),
    rivus_cep_window:update(as, {event1, a,bbb,c}),
    rivus_cep_window:update(as, {event2, a,bb,cc,d}),
    rivus_cep_window:update(as, {event2, a,bb,cc,dd}),

    %% WITHIN clause
    {Reservoir, Oldest} = rivus_cep_window:get_window(as),
    Ms1 = ets:fun2ms(fun({{Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==event1  -> Value end),
    Ms2 = ets:fun2ms(fun({{Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==event2  -> Value end),

    %% FROM clause
    QH1 = ets:table(Reservoir, [{traverse, {select, Ms1}}]),
    QH2 = ets:table(Reservoir, [{traverse, {select, Ms2}}]),

    %% WHERE clause
    QH = qlc:q([ {X,Y} || X <- QH1, Y <- QH2, element(2,X) == element(2,Y)]),

    %% SELECT clause
    ResSet = lists:foldl(fun({X,Y}, Acc) -> sets:add_element(Y, sets:add_element(X,Acc)) end, sets:new(),  qlc:e(QH)),
    Count = sets:size(ResSet),

    ?assertEqual(4, Count ).    
    

select_sum_where_op_equal() ->
    rivus_cep_window:new(as, slide, 60),
    rivus_cep_window:update(as, {event1, 10,b,c}), % *
    rivus_cep_window:update(as, {event1, 15,bbb,c}),
    rivus_cep_window:update(as, {event1, 20,b,c}), % *
    rivus_cep_window:update(as, {event2, 30,b,cc,d}),
    rivus_cep_window:update(as, {event2, 40,bb,cc,dd}),

    %% WITHIN clause
    {Reservoir, Oldest} = rivus_cep_window:get_window(as),
    Ms1 = ets:fun2ms(fun({{Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==event1  -> Value end),
    Ms2 = ets:fun2ms(fun({{Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==event2  -> Value end),

    %% FROM clause
    QH1 = ets:table(Reservoir, [{traverse, {select, Ms1}}]),
    QH2 = ets:table(Reservoir, [{traverse, {select, Ms2}}]),

    %% WHERE clause
    QH = qlc:q([ X || X <- QH1, Y <- QH2, element(3,X) == element(3,Y)]),

    %% SELECT clause
    ResSet = lists:foldl(fun(X, Acc) -> sets:add_element(X,Acc) end, sets:new(),  qlc:e(QH)),
    Sum = lists:foldl(fun(E, Acc) -> element(2, E) + Acc end, 0, sets:to_list(ResSet)),

    ?assertEqual(30, Sum ).   

create_match_spec(Event, Oldest) ->
    ets:fun2ms(fun({ {Time,'_'},Value}) when Time >= Oldest andalso element(1,Value)==Event  -> Value end).
    
    
create_from_qh(MatchSpec, Reservoir) ->
     ets:table(Reservoir, [{traverse, {select, MatchSpec}}]).

dynamic_qh() ->
    rivus_cep_window:new(as, slide, 60),
    rivus_cep_window:update(as, {event1, 10,b,c}), % *
    rivus_cep_window:update(as, {event1, 15,bbb,c}),
    rivus_cep_window:update(as, {event1, 20,b,c}), % *
    rivus_cep_window:update(as, {event2, 30,b,cc,d}),
    rivus_cep_window:update(as, {event2, 40,bb,cc,dd}),

    %% WITHIN clause
    {Reservoir, Oldest} = rivus_cep_window:get_window(as),    
    
    MatchSpecs =
        [ 
	  create_match_spec(Event, Oldest) ||
	    Event <- [event1, event2]
        ],
    FromQueryHandlers =
        [ 
	  create_from_qh(MS, Reservoir) ||
	    MS <- MatchSpecs
        ],
    QH =
        qlc:q([ 
		E1 ||
		  E1 <- hd(FromQueryHandlers),
		  E2 <- hd(tl(FromQueryHandlers)),
		  element(3,E1) == element(3,E2)
              ]),

   %% SELECT clause
    ResSet = lists:foldl(fun(X, Acc) -> sets:add_element(X,Acc) end, sets:new(),  qlc:e(QH)),
    Sum = lists:foldl(fun(E, Acc) -> element(2, E) + Acc end, 0, sets:to_list(ResSet)),

    ?assertEqual(30, Sum ).   
    
