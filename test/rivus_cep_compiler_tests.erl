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

compile_test()->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
    {ok, Tokens, Endline} = rivus_cep_scanner:string("define correlation1 as
                                                         select eventparam
                                                         from event1; ", 1),
    ?assertEqual({ok,[{correlation1},
    		      {[{event1,eventparam}]},{[event1]},{nil},{nil}]},
    		 rivus_cep_parser:parse(Tokens)).

compile_2_test() ->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
    {ok, Tokens, Endline} = rivus_cep_scanner:string("define correlation1 as
                                                         select eventparam1, eventparam2
                                                         from event1, event2; ", 1),

    ?assertError({error, missing_event_qualifier}, rivus_cep_parser:parse(Tokens)).

compile_3_test() ->
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
    


compile_4_test() ->
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

compile_5_test() ->
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
compile_6_test() ->
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

template_1_test() ->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
        {ok, Tokens, Endline} = rivus_cep_scanner:string("define correlation1 as
                                                         select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
                                                         from event1 as ev1, event2 as ev2
                                                         where ev1.eventparam2 = ev2.eventparam2
                                                         within 60 seconds; ", 1),

    TemplateVars = rivus_cep_parser:parse(Tokens),
    ?assertEqual({ok,[{correlation1},
		      {[{event1,eventparam1},
			{event2,eventparam2},
			{event2,eventparam3},
			{event1,eventparam2}]},
		      {[event1,event2]},
		      {{eq,{event1,eventparam2},{event2,eventparam2}}},
		      {60}]}, TemplateVars ),

    {ok, [StmtName, SelectClause, FromClause, WhereClause, WithinClause]} = TemplateVars,
    
    Stmt = rivus_cep_stmt_builder:build_rs_stmt(StmtName, SelectClause, FromClause, WhereClause),
    %% ?debugMsg(io_lib:format("Stmt: ~p~n",[Stmt])),
    
    erlydtl:compile("../priv/stmt_template.dtl", stmt_template),
    {ok, Templ} = stmt_template:render([
					{stmtName, list_to_binary(atom_to_list(element(1,StmtName)))},
					{timeout, list_to_binary("60")},
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
    Partition = blabla,

    application:start(gproc),
    application:start(lager),
    application:start(folsom),
    
    ModName:start_link(Partition),

    Event1 = {event1, 10,b,c}, 
    Event2 = {event1, 15,bbb,c},
    Event3 = {event1, 20,b,c},
    Event4 = {event2, 30,b,cc,d},
    Event5 = {event2, 40,bb,cc,dd},
   
    gproc:send({p, l, {Partition, element(1, Event1)}}, {element(1, Event1), Event1}),
    gproc:send({p, l, {Partition, element(1, Event2)}}, {element(1, Event2), Event2}),
    gproc:send({p, l, {Partition, element(1, Event3)}}, {element(1, Event3), Event3}),
    gproc:send({p, l, {Partition, element(1, Event4)}}, {element(1, Event4), Event4}),
    gproc:send({p, l, {Partition, element(1, Event5)}}, {element(1, Event5), Event5}),
    Pids = gproc:lookup_pids({p, l, get_result}),

    {ok,Values} = gen_server:call(hd(Pids), get_result),
    %% ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    ?assertEqual([{10,b,cc,b},{20,b,cc,b}], Values),
    application:stop(gproc),
    application:stop(lager),
    application:stop(folsom).



template_2_test() ->
    rivus_cep_compiler:compile("../src/rivus_cep_scanner.xrl","../src/rivus_cep_parser.yrl"),
        {ok, Tokens, Endline} = rivus_cep_scanner:string("define correlation1 as
                                                         select ev1.eventparam1, ev2.eventparam2, sum(ev2.eventparam3) 
                                                         from event1 as ev1, event2 as ev2
                                                         where ev1.eventparam2 = ev2.eventparam2
                                                         within 60 seconds; ", 1),

    TemplateVars = rivus_cep_parser:parse(Tokens),
    ?assertEqual({ok,[{correlation1},
		      {[{event1,eventparam1},
			{event2,eventparam2},
			{sum,{event2,eventparam3}}]},
		      {[event1,event2]},
		      {{eq,{event1,eventparam2},{event2,eventparam2}}},
		      {60}]}, TemplateVars ),

    {ok, [StmtName, SelectClause, FromClause, WhereClause, WithinClause]} = TemplateVars,
    
    Stmt = rivus_cep_stmt_builder:build_rs_stmt(StmtName, SelectClause, FromClause, WhereClause),
    %% ?debugMsg(io_lib:format("Stmt: ~p~n",[Stmt])),

%% {(element(1,E1)):get_param_by_name(E1,eventparam1),(element(1,E2)):get_param_by_name(E2,eventparam2),{sum, {(element(1,E2)):get_param_by_name(E2,eventparam3)}}},
    
    erlydtl:compile("../priv/stmt_template.dtl", stmt_template),
    {ok, Templ} = stmt_template:render([
					{stmtName, list_to_binary(atom_to_list(element(1,StmtName)))},
					{timeout, list_to_binary("60")},
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
    Partition = blabla,

    application:start(gproc),
    application:start(lager),
    application:start(folsom),
    
    ModName:start_link(Partition),

    Event1 = {event1, 10,b,10}, 
    Event2 = {event1, 15,bbb,20},
    Event3 = {event1, 20,b,30},
    Event4 = {event2, 30,b,40,d},
    Event5 = {event2, 40,bb,50,dd},
    Event6 = {event2, 50,b,40,d},

    gproc:send({p, l, {Partition, element(1, Event1)}}, {element(1, Event1), Event1}),
    gproc:send({p, l, {Partition, element(1, Event2)}}, {element(1, Event2), Event2}),
    gproc:send({p, l, {Partition, element(1, Event3)}}, {element(1, Event3), Event3}),
    gproc:send({p, l, {Partition, element(1, Event4)}}, {element(1, Event4), Event4}),
    gproc:send({p, l, {Partition, element(1, Event5)}}, {element(1, Event5), Event5}),
    gproc:send({p, l, {Partition, element(1, Event6)}}, {element(1, Event6), Event6}),
    Pids = gproc:lookup_pids({p, l, get_result}),

    %%timer:sleep(2000),
    {ok,Values} = gen_server:call(hd(Pids), get_result),
    %% ?debugMsg(io_lib:format("Values: ~p~n",[Values])),
    ?assertEqual([{20,b,80},{10,b,80}], Values),
    application:stop(gproc),
    application:stop(lager),
    application:stop(folsom).




