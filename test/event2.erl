-module(event2).
-behaviour(event_behaviour).
-export([get_param_by_name/2, get_param_names/0]).

get_param_by_name(Event, ParamName) ->
  case ParamName of
    name -> element(1, Event);
    eventparam1 -> element(2, Event);
    eventparam2 -> element(3, Event);
    eventparam3 -> element(4, Event);
    eventparam4 -> element(5, Event)
  end.

get_param_names() ->
  [eventparam1, eventparam2, eventparam3, eventparam4].
