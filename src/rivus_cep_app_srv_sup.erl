%%%-------------------------------------------------------------------
%%% @author vasco
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Nov 2014 10:09 PM
%%%-------------------------------------------------------------------
-module(rivus_cep_app_srv_sup).
-compile([{parse_transform, lager_transform}]).
-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    lager:debug("-----> rivus_cep_app_sup, Args:  ~p~n", [[]]),
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = permanent,
    Shutdown = 2000,
    Type = worker,

    AChild = {rivus_cep_app_srv, {rivus_cep_app_srv, start_link, [self()]},
        Restart, Shutdown, Type, [rivus_cep_app_srv]},

    {ok, {SupFlags, [AChild]}}.
