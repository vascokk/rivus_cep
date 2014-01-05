[![Build Status](https://travis-ci.org/vascokk/rivus_cep.png)](https://travis-ci.org/vascokk/rivus_cep)

#Rivus CEP

Rivus CEP is an Erlang library for complex event processing. It uses a declarative SQL-like DSL for processing the events.

With Rivus you can do things like:

```
select 
    ev1.eventparam1, ev2.eventparam2, sum(ev2.eventparam3) 
from 
    event1 as ev1, event2 as ev2
where 
    ev1.eventparam2 = ev2.eventparam2
within 60 seconds
```

or 'select' events from pattern:

``` 
select 
    ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev2.eventparam4
from 
    event1 as ev1 -> event2 as ev2
where
    ev1.eventparam2 = ev2.eventparam2
within 60 seconds
```

In the second query, the result will be generated only in case when event2 stricltly follows event1, within a 60 seconds window.

Here is how to use it:

``` erlang
application:start(rivus_cep)

QueryStr = "define correlation2 as
                  select ev1.eventparam1, ev2.eventparam2, sum(ev2.eventparam3) 
                  from event1 as ev1, event2 as ev2
                   where ev1.eventparam2 = ev2.eventparam2
                    within 60 seconds; "

Producer = event_producer_1,
{ok, SubscriberPid} = result_subscriber:start_link(), 

{ok, QueryPid} = rivus_cep:load_query(QueryStr, [Producer], [SubscriberPid], [{shared_streams, true}]),
    
%% create some evetnts
Event1 = {event1, gr1,b,10}, 
Event2 = {event2, gr2,bbb,20},

%% send the events
rivus_cep:notify(Producer, Event1),
rivus_cep:notify(Producer, Event2).

%% or if you don't care about the producers
rivus_cep:notify(Event1),
rivus_cep:notify(Event2).
	
```

The query is started with `rivus_cep:load_query/4`. It takes as arguments: query string, list of event producers, list of query result subscribers and options as a proplist.

Each query worker will register itself to the  [gproc](https://github.com/uwiger/gproc) process registry, for the events listed in the "from" clause.

If the events are sent via `rivus_cep:notify/1`, the event will be received by any query subscribed for this event type. With `notify/2` - only the queries subscribed to the particular Producer will receive the event.

For each query there must be at least one Subscriber to receive the query results.

See `tests/rivus_cep_tests.erl` for more usage examples. 

Internally, the events are stored in an ETS-based sliding window. DSL statments are translated to Erlang "match specifications" and QLC queries.

For each event type there must be a module implementing the `event_behavior` with the same name used in the "from" clause. The important function that needs to be implemented is - `get_param_by_name(Event, ParamName)`. 

For memory efficiency, `{shared_streams, true}` can be provided in the options list. In this case, the query will work with a shared event sliding window. The window's size will be equal of the maximum "within" clause of all sharing queries.
Queries based on event pattern use only non-shared windows.

Only "strict" event patterns are supported at the moment. This means, for a pattern "event1 -> event2", if some other event is received in between (event1->other event->event2), the query won't generate any result.

See some DSL examples in `test/rivus_cep_parser_tests.erl`.

###Dependencies

- [gproc](https://github.com/uwiger/gproc)
- [folsom](https://github.com/boundary/folsom)
- [lager](https://github.com/basho/lager) 

###Current limitations

The project is in its infancy and there is a number of limitations/TODOs:

- more aggregation functions are yet to be implemented;
- using event aliases is mandatory;
- only sliding windows are supported (no 'batch' windows);
- no benchmarks at all;
- extensive testing needed.

###Contributions

Contributions and suggestions are most appreciated!
