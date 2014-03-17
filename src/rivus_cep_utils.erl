-module(rivus_cep_utils).

-export([timestamp/0]).

timestamp() ->
    folsom_utils:now_epoch().
