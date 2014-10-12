-module(rivus_cep_aggregation_tests).
-compile([debug_info, export_all]).
-compile([{parse_transform, lager_transform}]).

-include_lib("eunit/include/eunit.hrl").
-include("rivus_cep.hrl").

group_key_1_test() ->
    QueryStr = "define aggr_query as
                  select ev1.eventparam1, ev1.eventparam2, sum(ev1.eventparam3) 
                  from event1(eventparam2=b) as ev1
                    within 5 seconds batch; ",
    {ok, Tokens, _Endline} = rivus_cep_scanner:string(QueryStr, 1),

    ?debugMsg(io_lib:format("Select: ~p~n",[ rivus_cep_parser:parse(Tokens)])),
    SelectClause = [{event1,eventparam1},
		     {event1,eventparam2},
		     {sum,{event1,eventparam3}}],
    Key1 = rivus_cep_aggregation:get_group_key({event1,gr1,b,30}, SelectClause),
    Key2 = rivus_cep_aggregation:get_group_key({event1,gr3,b,40,d}, SelectClause),

    ?assertEqual([{gr1,b}],[Key1]),
    ?assertEqual([{gr3,b}],[Key2]).

group_key_2_test() ->
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
    
    
