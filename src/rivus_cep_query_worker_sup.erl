-module(rivus_cep_query_worker_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

-compile([{parse_transform, lager_transform}]).

start_link() ->
    supervisor:start_link(?MODULE, []).
 
init(Args) ->
    lager:debug("query_worker_sup, Args:  ~p~n",[Args]),
    MaxRestart = 5,
    MaxTime = 3600,
    {ok, {{simple_one_for_one, MaxRestart, MaxTime},
	  [{query_worker,
	    {rivus_cep_query_worker, start_link, []},
	    transient, brutal_kill, worker, [rivus_cep_query_worker]}]}}.
