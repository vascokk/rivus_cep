-module(rivus_cep_aggregation_tests).
-compile([debug_info, export_all]).
-compile([{parse_transform, lager_transform}]).

-include_lib("eunit/include/eunit.hrl").
-include("rivus_cep.hrl").

aggr_1_test() ->
    application:start(gproc),
    application:start(lager),
    application:start(folsom),
    
    ResSet = [{{param,b},{plus,{sum,{param,10}},{sum,{param,50}}},{param,c},{param,d}},
	      {{param,b},{plus,{sum,{param,10}},{sum,{param,50}}},{param,f},{param,d}},
	      {{param,b},{plus,{sum,{param,20}},{sum,{param,50}}},{param,c},{param,d}}],
    
    Result = rivus_cep_aggregation:eval_resultset(test_stmt, ResSet, #res_eval_state{}),
    %% ?debugMsg(io_lib:format("Result: ~p~n",[Result])),
    ?assertEqual([{b,130,c,d},{b,60,f,d}], Result),
    application:stop(gproc),
    application:stop(lager),
    application:stop(folsom).

    
group_key_1_test() ->
    ResSet = [{{param,b},{plus,{sum,{param,10}},{sum,{param,50}}},{param,c},{param,d}},
	      {{param,b},{plus,{sum,{param,10}},{sum,{param,50}}},{param,f},{param,d}},
	      {{param,b},{plus,{sum,{param,20}},{sum,{param,50}}},{param,c},{param,d}}],
    Key1 = rivus_cep_aggregation:get_group_key(hd(ResSet)),
    Key2 = rivus_cep_aggregation:get_group_key(hd(tl(ResSet))),
    Key3 = rivus_cep_aggregation:get_group_key(hd(tl(tl(ResSet)))),
    ?assertEqual([{b,c,d},{b,f,d},{b,c,d}],[Key1, Key2,Key3]).

group_key_2_test() ->
    ResSet = [{{param,b},{plus,{param,10},{param,50}},{param,c},{param,d}},
	      {{param,b},{plus,{param,10},{param,50}},{param,f},{param,d}},
	      {{param,b},{plus,{param,20},{param,50}},{param,c},{param,d}}],
    Key1 = rivus_cep_aggregation:get_group_key(hd(ResSet)),
    Key2 = rivus_cep_aggregation:get_group_key(hd(tl(ResSet))),
    Key3 = rivus_cep_aggregation:get_group_key(hd(tl(tl(ResSet)))),
    ?assertEqual([{b,60,c,d},{b,60,f,d},{b,70,c,d}],[Key1, Key2,Key3]).
