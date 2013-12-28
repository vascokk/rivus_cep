-module(rivus_cep_parser_utils_tests).

-compile([debug_info, export_all]).

-include_lib("eunit/include/eunit.hrl").

replace_select_aliases_test() ->
    Tuples = [{ev1,eventparam1},
	      {ev2,eventparam2},
	      {ev2,eventparam3},
	      {ev1,eventparam2}],
    FromTuples =  [{ev1,event1},{ev2,event2}],
    ?assertEqual( [{event1,eventparam1},
		   {event2,eventparam2},
		   {event2,eventparam3},
		   {event1,eventparam2}], rivus_cep_parser_utils:replace_select_aliases(Tuples, FromTuples)).

replace_select_aliases_2_test() ->
    Select = [{ev1,eventparam1},
	      {ev2,eventparam2},
	      {minus,{plus,{plus,{plus,{ev1,eventparam1},
				  {mult,{ev2,eventparam2},{integer,5}}},
			    {integer,6}},
		      {ev2,eventparam4}},
	       {ev1,eventparam1}},
	      {ev2,eventparam3}],
    FromTuples = [{ev1,event1}, {ev2,event2}],
    Expected = [{event1,eventparam1},
		{event2,eventparam2},
		{minus,{plus,{plus,{plus,{event1,eventparam1},
				    {mult,{event2,eventparam2},{integer,5}}},
			      {integer,6}},
			{event2,eventparam4}},
		 {event1,eventparam1}},
		{event2,eventparam3}],
    ?assertEqual(Expected, rivus_cep_parser_utils:replace_select_aliases(Select, FromTuples)).


replace_select_aliases_3_test() ->
    Select =  [{ev1,eventparam1},
               {sum,{ev2,eventparam2}},
               {minus,
                   {plus,
                       {plus,
                           {plus,
                               {ev1,eventparam1},
                               {mult,{ev2,eventparam2},{integer,5}}},
                           {integer,6}},
                       {ev2,eventparam4}},
                   {ev1,eventparam1}},
               {count,{ev2,eventparam3}}],
    FromTuples = [{ev1,event1}, {ev2,event2}],
    Expected = [{event1,eventparam1},
			{sum,{event2,eventparam2}},
			{minus,{plus,{plus,{plus,{event1,eventparam1},
					    {mult,{event2,eventparam2},{integer,5}}},
				      {integer,6}},
				{event2,eventparam4}},
			 {event1,eventparam1}},
			{count,{event2,eventparam3}}],
    ?assertEqual(Expected, rivus_cep_parser_utils:replace_select_aliases(Select, FromTuples)).


replace_where_aliases_test() ->
    Where = {'and',{eq,{ev1,eventparam1},{ev2,eventparam2}},
			{gt,{ev1,eventparam1},{ev2,eventparam2}}},
    From = [{ev1, event1}, {ev2,event2}],
    ?assertEqual({'and',{eq,{event1,eventparam1},{event2,eventparam2}},{gt,{event1,eventparam1},{event2,eventparam2}}}, rivus_cep_parser_utils:replace_where_aliases(Where,From)).


remove_from_aliases_test() ->
    FromTuples =  [{ev1,event1},{ev2,event2}],
    ?assertEqual([event1,event2], rivus_cep_parser_utils:remove_from_aliases(FromTuples)).


assign_var_to_event_test() ->
    From = [event1, event2, event3],
    Expected = [{"E1", "event1"}, {"E2","event2"}, {"E3","event3"}],
    ?assertEqual(Expected, rivus_cep_parser_utils:assign_var_to_event(From)).

replace_events_with_vars_test() ->
    EventVariables = [{"E1", "event1"}, {"E2", "event2"}],
    Where = "((event1:get_param_by_name(event1,eventparam1) == event2:get_param_by_name(event2,eventparam2)) andalso (event1:get_param_by_name(event1,eventparam1) > event2:get_param_by_name(event2,eventparam2)))",
    %%"((element(3,event1) == element(3,event2)) andalso (element(3,event1) > element(3,event2)))",	
    Expected = "(((element(1,E1)):get_param_by_name(E1,eventparam1) == (element(1,E2)):get_param_by_name(E2,eventparam2)) andalso ((element(1,E1)):get_param_by_name(E1,eventparam1) > (element(1,E2)):get_param_by_name(E2,eventparam2)))",
    %%"((element(3,E1) == element(3,E2)) andalso (element(3,E1) > element(3,E2)))",
    ?assertEqual(Expected, rivus_cep_parser_utils:replace_events_with_vars(EventVariables, Where)).

