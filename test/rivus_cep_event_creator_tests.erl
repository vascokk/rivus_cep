-module(rivus_cep_event_creator_tests).

-include_lib("eunit/include/eunit.hrl").



create_event_1_test() ->
  {ok, Tokens, _Endline} = rivus_cep_scanner:string("define event10 as (attr1, attr2, attr3);"),
  {ok, ParseRes} = rivus_cep_parser:parse(Tokens),
  ?assertEqual({event, {event10, [attr1, attr2, attr3]}}, ParseRes),
  {event, EventDef} = ParseRes,
  ?assertEqual({module, event10}, rivus_cep_event_creator:load_event_mod(EventDef)),
  Event = {event10, a, b, c},
  ?assertEqual(event10, event10:get_param_by_name(Event, name)),
  ?assertEqual(a, event10:get_param_by_name(Event, attr1)),
  ?assertEqual(b, event10:get_param_by_name(Event, attr2)),
  ?assertEqual(c, event10:get_param_by_name(Event, attr3)),
  ?assertError({case_clause,attr4}, event10:get_param_by_name(Event, attr4)).

