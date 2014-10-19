-module(event5).
-behaviour(event_behaviour).
-export([get_param_by_name/2, get_param_names/0]).

get_param_by_name(Event, ParamName) ->
  case ParamName of
    name -> element(1, Event);
    attr11 -> element(2, Event);
    attr12 -> element(3, Event);
    qttr13 -> element(4, Event);
    attr4 -> element(5, Event)
  end.

get_param_names() ->
  [attr11, attr12, attr13, attr4].