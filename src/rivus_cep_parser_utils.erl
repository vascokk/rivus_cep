-module(rivus_cep_parser_utils).

-export([replace_select_aliases/2,
	 replace_where_aliases/2,
	 remove_from_aliases/1,
	 assign_var_to_event/1,
	 replace_events_with_vars/2]).

replace_select_aliases({nil, E2}, FromTuples) ->
    case length(FromTuples) == 1 of
	true -> {element(2,hd(FromTuples)), E2} ;
	_ -> erlang:error({error, missing_event_qualifier})
    end;
replace_select_aliases({E1, E2}, FromTuples)  ->
    {_, Event} = lists:keyfind(E1, 1, FromTuples),
    [Event, E2];
replace_select_aliases(Tuples, FromTuples) when is_list(Tuples) ->
    lists:map(fun({E1, E2, E3}) when is_tuple(E2) andalso is_tuple(E3) ->
		      list_to_tuple(replace_select_aliases([E1, E2, E3], FromTuples));
		 ({E1, E2}) when is_tuple(E2)  ->
		      {E1, list_to_tuple(replace_select_aliases(E2, FromTuples))};
		 ({E1, E2}) ->
		      case E1 of
			  integer -> {integer,E2};
			  float -> {float,E2};
			  nil -> case length(FromTuples) == 1 of
				     true -> {element(2,hd(FromTuples)), E2} ;
				     _ -> erlang:error({error, missing_event_qualifier})
				 end;
			  _ -> {_, Event} = lists:keyfind(E1, 1, FromTuples),
			       {Event, E2}
		      end;
		 ({EventParam}) ->
		      case length(FromTuples) of
			  1 -> {element(1,hd(FromTuples)), EventParam};
			  _ -> erlang:error({error, missing_event_qualifier})
		      end;
		 (nil) -> erlang:error({error, missing_event_qualifier});
		 (Atom) when is_atom(Atom) -> Atom
	      end,
	      Tuples).


replace_where_aliases(nil, _) ->
    nil;
replace_where_aliases({Op, Left, Right}, FromTuples) ->
    {Op, replace_where_aliases(Left, FromTuples), replace_where_aliases(Right, FromTuples)} ;
replace_where_aliases({E1, E2}, FromTuples) ->
    case E1 of
	integer -> {integer, E2};
	float -> {float, E2};
	_ ->  {_, Event} = lists:keyfind(E1,1,FromTuples),
	      {Event, E2}
	%% TODO: the case when there is no event aliases
    end.

remove_from_aliases(FromTuples) ->
    lists:map(fun({Alias, EventName}) ->
		      EventName
	      end,
	      FromTuples).

assign_var_to_event(From) ->
    {Res, _} = lists:mapfoldl(fun(Event, Idx) -> {{"E" ++ integer_to_list(Idx), atom_to_list(Event)}, Idx+1} end, 1, From),
    Res.
    
replace_events_with_vars(EventVariables, StmtClause) ->
    Tmp = lists:foldl(fun({Var, Event}, Acc)-> re:replace(Acc, Event ++":",   "(element(1,"++Var++")):",[global, {return,list}]) end, StmtClause, EventVariables ),
    lists:foldl(fun({Var, Event}, Acc)-> re:replace(Acc, Event, Var,[global, {return,list}]) end, Tmp, EventVariables ).
    
