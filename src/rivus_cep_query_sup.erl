-module(rivus_cep_query_sup).
-export([start_link/1, init/1]).
-behaviour(supervisor).
 
start_link(MFA = {_,_,_}) ->
    supervisor:start_link(?MODULE, MFA).
 
init({M,F,A}) ->
    MaxRestart = 5,
    MaxTime = 3600,
    {ok, {{one_for_one, MaxRestart, MaxTime},
	  [{rivus_cep,
	    {M,F,A},
	    permanent, brutal_kill, worker, [M]}]}}.
