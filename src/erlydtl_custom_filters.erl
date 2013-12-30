-module(erlydtl_custom_filters).

-export([next/2]).

next(List, Idx) ->
    list_to_binary(atom_to_list(lists:nth(Idx+1, List))).
