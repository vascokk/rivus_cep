-module(rivus_cep_parser_tests).

-compile([debug_info, export_all]).
-compile([{parse_transform, lager_transform}]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("stdlib/include/qlc.hrl").

-record(event, {id,
		name,
		param1,
		param2,
	        ts}).

parse_query_1_test()->
    {ok, Tokens, _Endline} = rivus_cep_scanner:string("define correlation1 as
                                                         select eventparam
                                                         from event1; ", 1),
    ?assertEqual({ok,[{correlation1},
    		      {[{event1,eventparam}]},{[event1]},{nil},{nil},{[]}]},
    		 rivus_cep_parser:parse(Tokens)).

parse_query_2_test() ->
    {ok, Tokens, _Endline} = rivus_cep_scanner:string("define correlation1 as
                                                         select eventparam1, eventparam2
                                                         from event1, event2; ", 1),

    ?assertError({error, missing_event_qualifier}, rivus_cep_parser:parse(Tokens)).

parse_query_3_test() ->
        {ok, Tokens, _Endline} = rivus_cep_scanner:string("define correlation1 as
                                                         select eventparam1
                                                         from event1
                                                         where eventparam1 = 20
                                                         within 60 seconds; ", 1),

    ?assertEqual({ok,[{correlation1},
		      {[{event1,eventparam1}]},
		      {[event1]},
		      {{eq,{event1,eventparam1},{integer,20}}},
		      {60, sliding},{[]}]},
		 rivus_cep_parser:parse(Tokens)).
    


parse_query_4_test() ->
        {ok, Tokens, _Endline} = rivus_cep_scanner:string("define correlation1 as
                                                         select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
                                                         from event1 as ev1, event2 as ev2
                                                         where ev1.eventparam1 = ev2.eventparam2 and ev1.eventparam1 > ev2.eventparam2
                                                         within 60 seconds; ", 1),
    ?assertEqual({ok,[{correlation1},
		      {[{event1,eventparam1},
			{event2,eventparam2},
			{event2,eventparam3},
			{event1,eventparam2}]},
		      {[event1,event2]},
		      {{'and',{eq,{event1,eventparam1},{event2,eventparam2}},
			{gt,{event1,eventparam1},{event2,eventparam2}}}},
		      {60, sliding},{[]}]},
		 rivus_cep_parser:parse(Tokens)).

parse_query_5_test() ->
        {ok, Tokens, _Endline} = rivus_cep_scanner:string(
	   "define correlation1 as
      select ev1.eventparam1, ev2.eventparam2,
             ((ev1.eventparam1 + ev2.eventparam2 * 5 + 6) + ev2.eventparam4) - ev1.eventparam1, ev2.eventparam3
        from event1 as ev1, event2 as ev2
        where  ( ev1.eventparam1 * ev2.eventparam2 + 4 > ev2.eventparam4)
               or ev1.eventparam1 = ev2.eventparam2
               and  ev1.eventparam1 > ev2.eventparam2
        within 60 seconds; ", 1),

    ?assertEqual({ok,[{correlation1},
		      {[{event1,eventparam1},
			{event2,eventparam2},
			{minus,{plus,{plus,{plus,{event1,eventparam1},
					    {mult,{event2,eventparam2},{integer,5}}},
				      {integer,6}},
				{event2,eventparam4}},
			 {event1,eventparam1}},
			{event2,eventparam3}]},
		      {[event1,event2]},
		      {{'or',{gt,{plus,{mult,{event1,eventparam1},{event2,eventparam2}},{integer,4}},
			      {event2,eventparam4}},
			{'and',{eq,{event1,eventparam1},{event2,eventparam2}},
			 {gt,{event1,eventparam1},{event2,eventparam2}}}}},
		      {60, sliding},{[]}]},
		 rivus_cep_parser:parse(Tokens)).
parse_query_6_test() ->
        {ok, Tokens, _Endline} = rivus_cep_scanner:string(
	   "define correlation1 as
      select ev1.eventparam1, sum(ev2.eventparam2),
             ((ev1.eventparam1 + ev2.eventparam2 * 5 + 6) + ev2.eventparam4) - ev1.eventparam1, count(ev2.eventparam3)
        from event1 as ev1, event2 as ev2
        where  ( ev1.eventparam1 * ev2.eventparam2 + 4 > ev2.eventparam4)
               or ev1.eventparam1 = ev2.eventparam2
               and  ev1.eventparam1 > ev2.eventparam2
        within 60 seconds; ", 1),

    ?assertEqual({ok,[{correlation1},
		      {[{event1,eventparam1},
			{sum,{event2,eventparam2}},
			{minus,{plus,{plus,{plus,{event1,eventparam1},
					    {mult,{event2,eventparam2},{integer,5}}},
				      {integer,6}},
				{event2,eventparam4}},
			 {event1,eventparam1}},
			{count,{event2,eventparam3}}]},
		      {[event1,event2]},
		      {{'or',{gt,{plus,{mult,{event1,eventparam1},{event2,eventparam2}},{integer,4}},
			      {event2,eventparam4}},
			{'and',{eq,{event1,eventparam1},{event2,eventparam2}},
			 {gt,{event1,eventparam1},{event2,eventparam2}}}}},
		      {60, sliding},{[]}]},
		 rivus_cep_parser:parse(Tokens)).

parse_pattern_test() ->
        {ok, Tokens, _Endline} = rivus_cep_scanner:string("define pattern1 as
                                                         select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
                                                         from event1 as ev1 -> event2 as ev2
                                                         where ev1.eventparam1 = ev2.eventparam2 and ev1.eventparam1 > ev2.eventparam2
                                                         within 60 seconds; ", 1),
    ?assertEqual({ok,[{pattern1},
		      {[{event1,eventparam1},
			{event2,eventparam2},
			{event2,eventparam3},
			{event1,eventparam2}]},
		      {pattern,{[event1,event2]}},
		      {{'and',{eq,{event1,eventparam1},{event2,eventparam2}},
			{gt,{event1,eventparam1},{event2,eventparam2}}}},
		      {60, sliding},{[]}]},
		 rivus_cep_parser:parse(Tokens)).


parse_pattern_2_test() ->
        {ok, Tokens, _Endline} = rivus_cep_scanner:string("define pattern1 as
                                                         select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
                                                         from event1 as ev1 -> event2 as ev2 -> event3 as ev3
                                                         where ev1.eventparam1 = ev2.eventparam2 and ev2.eventparam1 = ev3.eventparam2
                                                         within 60 seconds; ", 1),

    ?assertEqual({ok,[{pattern1},
		      {[{event1,eventparam1},
			{event2,eventparam2},
			{event2,eventparam3},
			{event1,eventparam2}]},
		      {pattern,{[event1,{event2,event3}]}},
		      {{'and',{eq,{event1,eventparam1},{event2,eventparam2}},
			{eq,{event2,eventparam1},{event3,eventparam2}}}},
		      {60, sliding},{[]}]},
		 rivus_cep_parser:parse(Tokens)).

parse_query_filter_1_test()->
    {ok, Tokens, _Endline} = rivus_cep_scanner:string("define query1 as
                                                         select eventparam1
                                                         from event1(eventparam3=100, eventparam4=\"APPL\"); ", 1),
    ?assertEqual({ok,[{query1},
    		      {[{event1,eventparam1}]},{[event1]},{nil},{nil},
		      {orddict:from_list([{event1,[{eq,{event1,eventparam3},{integer,100}},
						   {eq,{event1,eventparam4},{string,"APPL"}}]}])}
		     ]},
    		 rivus_cep_parser:parse(Tokens)).


parse_query_filter_2_test()->
    {ok, Tokens, _Endline} = rivus_cep_scanner:string("define query1 as
                                                         select ev1.eventparam1, ev2.eventparam2
                                                         from event1(eventparam3=100, eventparam4=\"APPL\") as ev1,
                                                              event2(eventpam1=300) as ev2; ", 1),
    ?assertEqual({ok,[{query1},
    		      {[{event1,eventparam1},{event2,eventparam2}]},{[event1,event2]},{nil},{nil},
		      { orddict:from_list([{event1,[{eq,{event1,eventparam3},{integer,100}},
						    {eq,{event1,eventparam4},{string,"APPL"}}]},
					   {event2,[{eq,{event2,eventpam1},{integer,300}}]}])}
		     ]},
    		 rivus_cep_parser:parse(Tokens)).

parse_batch_window_test()->
        {ok, Tokens, _Endline} = rivus_cep_scanner:string("define correlation1 as
                                                         select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
                                                         from event1 as ev1, event2 as ev2
                                                         where ev1.eventparam1 = ev2.eventparam2 and ev1.eventparam1 > ev2.eventparam2
                                                         within 60 seconds batch ; ", 1),
    ?assertEqual({ok,[{correlation1},
		      {[{event1,eventparam1},
			{event2,eventparam2},
			{event2,eventparam3},
			{event1,eventparam2}]},
		      {[event1,event2]},
		      {{'and',{eq,{event1,eventparam1},{event2,eventparam2}},
			{gt,{event1,eventparam1},{event2,eventparam2}}}},
		      {60, batch},{[]}]},
		 rivus_cep_parser:parse(Tokens)).

parse_query_filter_batch_aggr_test()->
    {ok, Tokens, _Endline} = rivus_cep_scanner:string("define aggr_query as
                                                          select ev1.eventparam1, ev2.eventparam2, sum(ev2.eventparam3) 
                                                          from event1(eventparam2=b) as ev1, event2(eventparam2=b) as ev2
                                                          within 5 seconds batch;  ", 1),
    ?assertEqual({ok,[{aggr_query},
    		      {[{event1,eventparam1},{event2,eventparam2},{sum, {event2, eventparam3}}]},
		        {[event1,event2]},{nil},{5, batch},
		      { orddict:from_list([{event1,[{eq,{event1,eventparam2},{atom,b}}]},
					   {event2,[{eq,{event2,eventparam2},{atom,b}}]}])}
		     ]},
    		 rivus_cep_parser:parse(Tokens)).
