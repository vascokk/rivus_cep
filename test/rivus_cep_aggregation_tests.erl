-module(rivus_cep_aggregation_tests).
-compile([debug_info, export_all]).
-compile([{parse_transform, lager_transform}]).

-include_lib("eunit/include/eunit.hrl").
-include("rivus_cep.hrl").

%% aggr_1_test() ->
%%     application:start(gproc),
%%     application:start(lager),
%%     application:start(folsom),
    
%%     ResSet = [{{param,b},{plus,{sum,{param,10}},{sum,{param,50}}},{param,c},{param,d}},
%% 	      {{param,b},{plus,{sum,{param,10}},{sum,{param,50}}},{param,f},{param,d}},
%% 	      {{param,b},{plus,{sum,{param,20}},{sum,{param,50}}},{param,c},{param,d}}],
    
%%     Result = rivus_cep_aggregation:eval_resultset(test_stmt, ResSet, #res_eval_state{}),
%%     %% ?debugMsg(io_lib:format("Result: ~p~n",[Result])),
%%     ?assertEqual([{b,130,c,d},{b,60,f,d}], Result),
%%     application:stop(gproc),
%%     application:stop(lager),
%%     application:stop(folsom).

    
%% group_key_1_test() ->
%%     ResSet = [{{param,b},{plus,{sum,{param,10}},{sum,{param,50}}},{param,c},{param,d}},
%% 	      {{param,b},{plus,{sum,{param,10}},{sum,{param,50}}},{param,f},{param,d}},
%% 	      {{param,b},{plus,{sum,{param,20}},{sum,{param,50}}},{param,c},{param,d}}],
%%     Key1 = rivus_cep_aggregation:get_group_key(hd(ResSet)),
%%     Key2 = rivus_cep_aggregation:get_group_key(hd(tl(ResSet))),
%%     Key3 = rivus_cep_aggregation:get_group_key(hd(tl(tl(ResSet)))),
%%     ?assertEqual([{b,c,d},{b,f,d},{b,c,d}],[Key1, Key2,Key3]).

%% group_key_2_test() ->
%%     ResSet = [{{param,b},{plus,{param,10},{param,50}},{param,c},{param,d}},
%% 	      {{param,b},{plus,{param,10},{param,50}},{param,f},{param,d}},
%% 	      {{param,b},{plus,{param,20},{param,50}},{param,c},{param,d}}],
%%     Key1 = rivus_cep_aggregation:get_group_key(hd(ResSet)),
%%     Key2 = rivus_cep_aggregation:get_group_key(hd(tl(ResSet))),
%%     Key3 = rivus_cep_aggregation:get_group_key(hd(tl(tl(ResSet)))),
%%     ?assertEqual([{b,60,c,d},{b,60,f,d},{b,70,c,d}],[Key1, Key2,Key3]).


group_key_3_test() ->
    QueryStr = "define aggr_query as
                  select ev1.eventparam1, ev1.eventparam2, sum(ev1.eventparam3) 
                  from event1(eventparam2=b) as ev1
                    within 5 seconds batch; ",
    {ok, Tokens, _Endline} = rivus_cep_scanner:string(QueryStr, 1),
    %% ResSet = [{event1,gr1,b,30},
    %% 	      {event1,gr1,b,40,d},
    %% 	      {event1,gr3,b,40,d},
    %% 	      {event1,gr1,b,10}],

    %% ?assertEqual({ok,[]}, rivus_cep_parser:parse(Tokens)).
    ?debugMsg(io_lib:format("Select: ~p~n",[ rivus_cep_parser:parse(Tokens)])),
    SelectClause = [{event1,eventparam1},
		     {event1,eventparam2},
		     {sum,{event1,eventparam3}}],
    Key1 = rivus_cep_aggregation:get_group_key({event1,gr1,b,30}, SelectClause),
    Key2 = rivus_cep_aggregation:get_group_key({event1,gr3,b,40,d}, SelectClause),

    ?assertEqual([{gr1,b}],[Key1]),
    ?assertEqual([{gr3,b}],[Key2]).

group_key_4_test() ->
    QueryStr = "define aggr_query as
                  select ev1.eventparam1, ev1.eventparam2, ev1.eventparam3 + ev1.eventparam4, sum(ev1.eventparam3) 
                  from event1(eventparam2=b) as ev1
                    within 5 seconds batch; ",
    {ok, Tokens, _Endline} = rivus_cep_scanner:string(QueryStr, 1),

    ?debugMsg(io_lib:format("Select: ~p~n",[ rivus_cep_parser:parse(Tokens)])),
    SelectClause = [{event1,eventparam1},
               {event1,eventparam2},
               {plus,{event1,eventparam3},{event1,eventparam4}},
               {sum,{event1,eventparam3}}],
    Key1 = rivus_cep_aggregation:get_group_key({event1,gr1,b,30,10}, SelectClause),
    Key2 = rivus_cep_aggregation:get_group_key({event1,gr3,b,40,10,d}, SelectClause),

    ?assertEqual([{gr1,b,40}],[Key1]),
    ?assertEqual([{gr3,b,50}],[Key2]).
    
    
