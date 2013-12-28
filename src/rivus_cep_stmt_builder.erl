-module(rivus_cep_stmt_builder).

-export([build_select_clause/3,
	 build_within_clause/1,
	 build_where_clause/1,
	 build_from_clause/1,
	 build_compr/4,
	 build_qh/3]).

-export([build_rs_stmt/4]).


build_rs_stmt(StmtName, Select, From, Where) ->
    build_within_clause(StmtName) ++ "," ++
    build_from_clause(From) ++ "," ++
    build_qh(Select, From, Where) ++ ",".

build_within_clause({StmtName}) ->
    "{Reservoir, Oldest} = rivus_cep_window:get_window( " ++ atom_to_list(StmtName) ++ " ),
     MatchSpecs = [create_match_spec(Event, Oldest) || Event<- State#state.events]".

build_where_clause(Where) ->
    where_eval(Where).

build_from_clause(_) ->
     "FromQueryHandlers = [create_from_qh(MS, Reservoir) || MS <- MatchSpecs]".

 %% build_qh({[event1,event2]},
 %% 	  {{'and',{eq,{event1,eventparam1},{event2,eventparam2}},
 %%        {gt,{event1,eventparam1},{event2,eventparam2}}}})
build_qh({Select}, {From}, {Where}) ->
    EventVariables = rivus_cep_parser_utils:assign_var_to_event(From),
    WhereStr = build_where_clause(Where),
    Filter = rivus_cep_parser_utils:replace_events_with_vars(EventVariables, WhereStr),
    SelectEval = build_select_clause(Select, EventVariables, ""),
    
    %%"QH = qlc:q([ Е1 || Е1 <- QH1, Е2 <- QH2, "++ Filter ++"])",
    "QH = qlc:q(" ++ build_compr(SelectEval, From,  "FromQueryHandlers", Filter) ++ ")".



where_eval({Op, Left, Right}) ->
    case Op of
	'and' -> "(" ++ where_eval(Left) ++ " andalso " ++ where_eval(Right) ++ ")";
	'or' -> "(" ++ where_eval(Left) ++ " orelse " ++ where_eval(Right) ++ ")";
	eq -> "(" ++ where_eval(Left) ++ " == " ++ where_eval(Right) ++ ")";
	gt -> "(" ++ where_eval(Left) ++ " > " ++ where_eval(Right) ++ ")"
    end;
where_eval({Type, Value}) ->
    case Type of
	integer -> Value;
	float -> Value;
	_ -> atom_to_list(Type) ++ ":get_param_by_name(" ++ atom_to_list(Type) ++ "," ++ atom_to_list(Value) ++ ")"

    end.

%% Build list comprehension expression for cartesian product from the elements of arbitrary number of lists,
%% given by L=[[...], [...], [...],....]
%% build_compr(L) ->   "[{X1, X2, X3, ...} || X1<-hd(L), X2<-hd(tl(L)), X3<-hd(tl(tl(L))),...]".
%% in generators - L is substituted with the ListName as a string
  
build_compr(Select, FromList, ListName, Filter) ->
    build_compr(FromList, Select, " || ", ListName, 1, Filter).

build_compr([_|T], Select, Generators, ListName, 1, Filter) ->
    build_compr(T, Select, Generators ++ "E1" ++ "<-" ++ "hd("++ListName++")", "tl("++ListName++")", 2, Filter);
build_compr([_|T], Select, Generators, ListName, Idx, Filter) ->
    I = integer_to_list(Idx),
    build_compr(T, Select, Generators ++ ", E" ++ I ++ "<-" ++ "hd("++ListName++")", "tl("++ListName++")", Idx +1, Filter);
build_compr([], Select, Generators, _, _, Filter ) ->
    "[{" ++ Select ++ "}" ++ Generators ++ ", " ++ Filter ++ "]".


build_select_clause([H|T], EventVariables, Acc) when T =/= []->
     NewAcc = Acc ++ select_eval(H) ++ ",",
     build_select_clause(T, EventVariables, NewAcc);
build_select_clause([H|T], EventVariables, Acc) when T == []->
     rivus_cep_parser_utils:replace_events_with_vars(EventVariables, Acc ++ select_eval(H)).

select_eval({integer, Value}) ->
     integer_to_list(Value);
select_eval({float, Value}) ->
    float_to_list(Value);
select_eval({EventName,ParamName}) when not is_tuple(EventName) andalso not is_tuple(ParamName)->
    atom_to_list(EventName) ++ ":get_param_by_name(" ++ atom_to_list(EventName) ++ "," ++ atom_to_list(ParamName) ++ ")";
select_eval({sum,SumTuple}) ->
    "{sum, {" ++ select_eval(SumTuple) ++"}}"; 
select_eval({avg,AvgTuple}) ->
    "{avg, {" ++ select_eval(AvgTuple) ++"}}"; 
select_eval({count,CountTuple}) ->
    "{count,{" ++ select_eval(CountTuple) ++"}}"; 
select_eval({min,MinTuple}) ->
    "{min,{" ++ select_eval(MinTuple) ++"}}";
select_eval({max,MaxTuple}) ->
    "{max,{" ++ select_eval(MaxTuple) ++ "}}";
select_eval({Op,Left,Right}) ->
     "{" ++ atom_to_list(Op) ++ ",{" ++ select_eval(Left)  ++ "}, " ++ select_eval(Right) ++ "}".
