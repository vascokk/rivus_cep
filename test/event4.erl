-module(event4).
-behaviour(event_behaviour).
-export([get_param_by_name/2, get_param_names/0]).

get_param_by_name(Event, ParamName) ->
  case ParamName of
    name -> element(1, Event);
    attr1 -> element(2, Event);
    attr2 -> element(3, Event);
    qttr3 -> element(4, Event);
    attr4 -> element(5, Event)
  end.

get_param_names() ->
  [attr1, attr2, attr3, attr4].