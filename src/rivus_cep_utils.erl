-module(rivus_cep_utils).

-export([timestamp/0, epoch/1]).

timestamp() ->
    epoch(os:timestamp()).

epoch({MegaSec, Sec, _}) ->
    (MegaSec * 1000000 + Sec). 
