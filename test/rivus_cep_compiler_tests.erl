-module(rivus_cep_compiler_tests).

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
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
    {ok, Tokens, Endline} = rivus_cep_scanner:string("define correlation1 as
                                                         select eventparam
                                                         from event1; ", 1),
    ?assertEqual({ok,[{correlation1},
    		      {[{event1,eventparam}]},{[event1]},{nil},{nil}]},
    		 rivus_cep_parser:parse(Tokens)).

parse_query_2_test() ->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
    {ok, Tokens, Endline} = rivus_cep_scanner:string("define correlation1 as
                                                         select eventparam1, eventparam2
                                                         from event1, event2; ", 1),

    ?assertError({error, missing_event_qualifier}, rivus_cep_parser:parse(Tokens)).

parse_query_3_test() ->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
        {ok, Tokens, Endline} = rivus_cep_scanner:string("define correlation1 as
                                                         select eventparam1
                                                         from event1
                                                         where eventparam1 = 20
                                                         within 60 seconds; ", 1),

    ?assertEqual({ok,[{correlation1},
		      {[{event1,eventparam1}]},
		      {[event1]},
		      {{eq,{event1,eventparam1},{integer,20}}},
		      {60}]},
		 rivus_cep_parser:parse(Tokens)).
    


parse_query_4_test() ->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
        {ok, Tokens, Endline} = rivus_cep_scanner:string("define correlation1 as
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
		      {60}]},
		 rivus_cep_parser:parse(Tokens)).

parse_query_5_test() ->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
        {ok, Tokens, Endline} = rivus_cep_scanner:string(
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
		      {60}]},
		 rivus_cep_parser:parse(Tokens)).
parse_query_6_test() ->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
        {ok, Tokens, Endline} = rivus_cep_scanner:string(
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
		      {60}]},
		 rivus_cep_parser:parse(Tokens)).

parse_pattern_test() ->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
        {ok, Tokens, Endline} = rivus_cep_scanner:string("define pattern1 as
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
		      {60}]},
		 rivus_cep_parser:parse(Tokens)).

template_module_test_() ->
    {setup,
     fun () -> folsom:start(),
	       lager:start(),
	       application:start(gproc)
     end,
     fun (_) -> folsom:stop(),
		application:stop(lager),
		application:stop(gproc)
     end,

     [{"Test template with query without aggregations",
       fun query_1/0},
      {"Test template with aggregation query",
       fun query_2/0},
      {"Tes query on event sequence (event pattern matching)",
       fun pattern/0},
      {"Test load_query(Query, Subscriber)",
       fun load_query/0}]
    }.

query_1() ->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
        {ok, Tokens, Endline} = rivus_cep_scanner:string("define correlation1 as
                                                         select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
                                                         from event1 as ev1, event2 as ev2
                                                         where ev1.eventparam2 = ev2.eventparam2
                                                         within 60 seconds; ", 1),

    StmtClauses = rivus_cep_parser:parse(Tokens),
    ?assertEqual({ok,[{correlation1},
		      {[{event1,eventparam1},
			{event2,eventparam2},
			{event2,eventparam3},
			{event1,eventparam2}]},
		      {[event1,event2]},
		      {{eq,{event1,eventparam2},{event2,eventparam2}}},
		      {60}]}, StmtClauses ),

    {ok, [StmtName, SelectClause, FromClause, WhereClause, WithinClause]} = StmtClauses,
    
    Stmt = rivus_cep_stmt_builder:build_rs_stmt(StmtName, SelectClause, FromClause, WhereClause),
    %% ?debugMsg(io_lib:format("Stmt: ~p~n",[Stmt])),
    
    erlydtl:compile("../priv/stmt_template.dtl", stmt_template),
    {ok, Templ} = stmt_template:render([
					{stmtName, list_to_binary(atom_to_list(element(1,StmtName)))},
					{timeout, element(1,WithinClause)},
					{resultsetStmt, list_to_binary(Stmt)},
					{eventList, element(1,FromClause)}
				       ]),

    %% ?debugMsg(io_lib:format("Templ: ~p~n",[Templ])),
    
    Data = iolist_to_binary(Templ),
    %% ?debugMsg(io_lib:format("Data: ~p~n",[Data])),
    
    Source = binary_to_list(Data),
    %% ?debugMsg(io_lib:format("Source: ~p~n",[Source])),

    Forms = erl_syntax:revert(rivus_cep_compiler:scan_parse([], Source, 0, [])),
    
    Mod = compile:forms(Forms, [return]),
    %% ?debugMsg(io_lib:format("Module: ~p~n",[Mod])).
    {CompileRes, ModName, Bin, _} = Mod,
    ?assertEqual({ok,correlation1}, {CompileRes, ModName}),

    %% load compiled module into memory
    ?assertEqual({module, ModName}, code:load_binary(ModName, atom_to_list(ModName), Bin)),

    %% send some events
    
    lager:set_loglevel(lager_console_backend, debug),
    
    {ok,Pid} = result_subscriber:start_link(),

    
    ModName:start_link(Pid),

    Event1 = {event1, 10,b,c}, 
    Event2 = {event1, 15,bbb,c},
    Event3 = {event1, 20,b,c},
    Event4 = {event2, 30,b,cc,d},
    Event5 = {event2, 40,bb,cc,dd},
   
    gproc:send({p, l, {Pid, element(1, Event1)}}, {element(1, Event1), Event1}),
    gproc:send({p, l, {Pid, element(1, Event2)}}, {element(1, Event2), Event2}),
    gproc:send({p, l, {Pid, element(1, Event3)}}, {element(1, Event3), Event3}),
    gproc:send({p, l, {Pid, element(1, Event4)}}, {element(1, Event4), Event4}),
    gproc:send({p, l, {Pid, element(1, Event5)}}, {element(1, Event5), Event5}),

    timer:sleep(2000),
    
    {ok,Values} = gen_server:call(Pid, get_result),
    %% ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    ?assertEqual([{10,b,cc,b},{20,b,cc,b}], Values),
     gen_server:call(Pid,stop).




query_2() ->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
        {ok, Tokens, Endline} = rivus_cep_scanner:string("define correlation2 as
                                                         select ev1.eventparam1, ev2.eventparam2, sum(ev2.eventparam3) 
                                                         from event1 as ev1, event2 as ev2
                                                         where ev1.eventparam2 = ev2.eventparam2
                                                         within 60 seconds; ", 1),

    StmtClauses = rivus_cep_parser:parse(Tokens),
    ?assertEqual({ok,[{correlation2},
		      {[{event1,eventparam1},
			{event2,eventparam2},
			{sum,{event2,eventparam3}}]},
		      {[event1,event2]},
		      {{eq,{event1,eventparam2},{event2,eventparam2}}},
		      {60}]}, StmtClauses ),

    {ok, [StmtName, SelectClause, FromClause, WhereClause, WithinClause]} = StmtClauses,
    
    Stmt = rivus_cep_stmt_builder:build_rs_stmt(StmtName, SelectClause, FromClause, WhereClause),
    %% ?debugMsg(io_lib:format("Stmt: ~p~n",[Stmt])),
    
    erlydtl:compile("../priv/stmt_template.dtl", stmt_template),
    {ok, Templ} = stmt_template:render([
					{stmtName, list_to_binary(atom_to_list(element(1,StmtName)))},
					{timeout, element(1,WithinClause)},
					{resultsetStmt, list_to_binary(Stmt)},
					{eventList, element(1,FromClause)}
				       ]),

    %% ?debugMsg(io_lib:format("Templ: ~p~n",[Templ])),
    
    Data = iolist_to_binary(Templ),
    %% ?debugMsg(io_lib:format("Data: ~p~n",[Data])),
    
    Source = binary_to_list(Data),
    %% ?debugMsg(io_lib:format("Source: ~p~n",[Source])),

    Forms = erl_syntax:revert(rivus_cep_compiler:scan_parse([], Source, 0, [])),
    
    Mod = compile:forms(Forms, [return]),
    %% ?debugMsg(io_lib:format("Module: ~p~n",[Mod])).
    {CompileRes, ModName, Bin, _} = Mod,
    ?assertEqual({ok,correlation2}, {CompileRes, ModName}),

    %% load compiled module into memory
    ?assertEqual({module, ModName}, code:load_binary(ModName, atom_to_list(ModName), Bin)),

    %% send some events

    lager:set_loglevel(lager_console_backend, debug),
    
    {Res, Pid} = result_subscriber:start_link(),
    
    ModName:start_link(Pid),
    
    Event1 = {event1, gr1,b,10}, 
    Event2 = {event1, gr2,bbb,20},
    Event3 = {event1, gr3,b,30},
    Event4 = {event2, gr1,b,40,d},
    Event5 = {event2, gr3,bb,50,dd},
    Event6 = {event2, gr2,b,40,d},

    gproc:send({p, l, {Pid, element(1, Event1)}}, {element(1, Event1), Event1}),
    gproc:send({p, l, {Pid, element(1, Event2)}}, {element(1, Event2), Event2}),
    gproc:send({p, l, {Pid, element(1, Event3)}}, {element(1, Event3), Event3}),
    gproc:send({p, l, {Pid, element(1, Event4)}}, {element(1, Event4), Event4}),
    gproc:send({p, l, {Pid, element(1, Event5)}}, {element(1, Event5), Event5}),
    gproc:send({p, l, {Pid, element(1, Event6)}}, {element(1, Event6), Event6}),

    timer:sleep(2000),
    {ok,Values} = gen_server:call(Pid, get_result),
    %% ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    ?assertEqual([{gr1,b,80},{gr3,b,80}], Values),
    gen_server:call(Pid,stop).


pattern() ->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
        {ok, Tokens, Endline} = rivus_cep_scanner:string("define pattern1 as
                                                         select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev2.eventparam4
                                                         from event1 as ev1 -> event2 as ev2
                                                         where ev1.eventparam2 = ev2.eventparam2
                                                         within 60 seconds; ", 1),

    StmtClauses = rivus_cep_parser:parse(Tokens),
    ?assertEqual({ok,[{pattern1},
		      {[{event1,eventparam1},
			{event2,eventparam2},
			{event2,eventparam3},
			{event2,eventparam4}]},
		      {pattern, {[event1,event2]}},
		      {{eq,{event1,eventparam2},{event2,eventparam2}}},
		      {60}]}, StmtClauses ),

    {ok, [StmtName, SelectClause, FromClause, WhereClause, WithinClause]} = StmtClauses,

    {IsPattern, _FromClause} = case FromClause of
				   {pattern, Events} -> {true, Events};
				   _ -> {false, FromClause}
			       end,
    
    Stmt = rivus_cep_stmt_builder:build_rs_stmt(StmtName, SelectClause, _FromClause, WhereClause),
    %% ?debugMsg(io_lib:format("Stmt: ~p~n",[Stmt])),
    
    erlydtl:compile("../priv/pattern_stmt_template.dtl", stmt_template,[{custom_filters_modules,[erlydtl_custom_filters]}]),
    {ok, Templ} = stmt_template:render([
					{stmtName, list_to_binary(atom_to_list(element(1,StmtName)))},
					{timeout, element(1,WithinClause)},
					{resultsetStmt, list_to_binary(Stmt)},
					{eventList, element(1, _FromClause)}
				       ]),

    %% ?debugMsg(io_lib:format("Templ: ~p~n",[Templ])),
    
    Data = iolist_to_binary(Templ),
    %% ?debugMsg(io_lib:format("Data: ~p~n",[Data])),
    
    Source = binary_to_list(Data),
    %%?debugMsg(io_lib:format("Source: ~p~n",[Source])),

    Forms = erl_syntax:revert(rivus_cep_compiler:scan_parse([], Source, 0, [])),
    
    Mod = compile:forms(Forms, [return]),
    %% ?debugMsg(io_lib:format("Module: ~p~n",[Mod])).
    {CompileRes, ModName, Bin, _} = Mod,
    ?assertEqual({ok,pattern1}, {CompileRes, ModName}),

    %% load compiled module into memory
    ?assertEqual({module, ModName}, code:load_binary(ModName, atom_to_list(ModName), Bin)),

    %% send some events

    lager:set_loglevel(lager_console_backend, debug),
    
    {Res, Pid} = result_subscriber:start_link(),
    
    ModName:start_link(Pid),

    Event1 = {event1, 10,b,c}, 
    Event2 = {event1, 15,bbb,c},
    Event3 = {event1, 20,b,c},
    Event4 = {event2, 30,b,cc,d},
    Event5 = {event2, 40,bb,cc,dd},
   
    gproc:send({p, l, {Pid, element(1, Event1)}}, {element(1, Event1), Event1}),
    gproc:send({p, l, {Pid, element(1, Event2)}}, {element(1, Event2), Event2}),
    gproc:send({p, l, {Pid, element(1, Event3)}}, {element(1, Event3), Event3}),
    gproc:send({p, l, {Pid, element(1, Event4)}}, {element(1, Event4), Event4}),
    gproc:send({p, l, {Pid, element(1, Event5)}}, {element(1, Event5), Event5}),

    timer:sleep(2000),       
    
    {ok,Values} = gen_server:call(Pid, get_result),
    %% ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    ?assertEqual([{20,b,cc,d}], Values),
    gen_server:call(Pid,stop).


load_query()->
    {Res, Pid} = result_subscriber:start_link(),
    
    QueryStr = "define pattern2 as
                    select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev2.eventparam4
                    from event1 as ev1 -> event2 as ev2
                    where ev1.eventparam2 = ev2.eventparam2
                    within 60 seconds; ",

    rivus_cep_compiler:load_query(QueryStr, Pid),
    
    %% send some events

    lager:set_loglevel(lager_console_backend, debug),
    
    Event1 = {event1, 10,b,c}, 
    Event2 = {event1, 15,bbb,c},
    Event3 = {event1, 20,b,c},
    Event4 = {event2, 30,b,cc,d},
    Event5 = {event2, 40,bb,cc,dd},
   
    gproc:send({p, l, {Pid, element(1, Event1)}}, {element(1, Event1), Event1}),
    gproc:send({p, l, {Pid, element(1, Event2)}}, {element(1, Event2), Event2}),
    gproc:send({p, l, {Pid, element(1, Event3)}}, {element(1, Event3), Event3}),
    gproc:send({p, l, {Pid, element(1, Event4)}}, {element(1, Event4), Event4}),
    gproc:send({p, l, {Pid, element(1, Event5)}}, {element(1, Event5), Event5}),

    timer:sleep(2000),       
    
    {ok,Values} = gen_server:call(Pid, get_result),
    %% ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    ?assertEqual([{20,b,cc,d}], Values),
    gen_server:call(Pid,stop).    


