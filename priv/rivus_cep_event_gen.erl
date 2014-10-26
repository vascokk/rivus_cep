
-module(rivus_cep_event_gen).

%% API
-export([generate_event/3]).

generate_event(_Id, EventName, ParamCount) when is_integer(ParamCount) andalso ParamCount>0 ->
    generate(ParamCount, [EventName]).

generate(0, Acc) ->
    list_to_tuple(Acc);
generate(ParamCount, Acc) ->
    generate(ParamCount-1, Acc ++ [random:uniform(1000)]).

