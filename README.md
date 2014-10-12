[![Build Status](https://travis-ci.org/vascokk/rivus_cep.png)](https://travis-ci.org/vascokk/rivus_cep)

# Overview

Rivus CEP is an Erlang application for Complex Event Processing. It uses a declarative SQL-like DSL to define operations over event streams.
In the CEP-based systems the events(data) are processed as they arrive, as opposite to the DB, where data is first persisted, then fetched and processed:

<pre>

                               |------CEP Engine------| 
                               |                      | 
DataSource ------------------->|  Continuous Query    |-------------> Result Subscriber 
/Provider/     Event stream    | over a time interval |   Result         /Consumer/
                               |----------------------|
              
</pre>

#Queries

There are two type of queries: 
- simple queries:

```
select 
    ev1.eventparam1, ev2.eventparam2, sum(ev2.eventparam3) 
from 
    event1 as ev1, event2 as ev2
where 
    ev1.eventparam2 = ev2.eventparam2
within 60 seconds
```

The above query will join all the events of type `event1` and `event2` arrived whithin the last 60 seconds ("sliding window").
In case of "join" queries, the events within the time window will be persisted in memory. For queries that do not require "join" - the result will be calculated immediately, wihout events persistence. 

- pattern matching queries: 

``` 
select 
    ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev2.eventparam4
from 
    event1 as ev1 -> event2 as ev2
where
    ev1.eventparam2 = ev2.eventparam2
within 60 seconds
```

Here the result will be generated only in case when `event2` stricltly follows `event1` within a 60 seconds window. Pattern-based queries always persist the events. Pattern matching mechanism based on a directed graph FMS, using the `digraph` module.

#Windows

A "window" in Rivus actually means two things:

- a "time window" - the time interval, which the query operates on, and:
- the temporary storage where the events within the "time window" are being persisted;

There are two types of windows currently implemented:
- sliding window (default) - this is a moving length window, which contains the events from the last X second. A new Result will be generated after each received event. 
- batch window - contains the events from a given moment in the past (t0) up to the moment t0+X. The Result will be generated only after the amount of time X expires. Once the Result is calculated the window will be cleaned up.

The underlying persistence mechanism is pluggable (see the `rivus_cep_window.erl` module). Default implementation is based on ETS tables.

##Filters

Filters are used to remove the events from the stream, based on certain criteria, so that to speed up the result generation and reduce the memory requirements:

```
select 
	ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
from 
	event1(eventparam1>5, eventparam1<30) as ev1, 
	event2(eventparam1<50) as ev2
where 
	ev1.eventparam2 = ev2.eventparam2
within 60 seconds
```

Multiple criteria separated by `','` could be used. The above query will filter out all the `event1` events where `eventparam1>5 AND eventparam1<30` and also the `event2`, where `eventparam1<50`. The difference netween the filters and `where` clause criteria is that the filters are executed before the event to be persisted in memory. In this way the user can reduce the event volume in Result calculations.

#Aggregations

The following aggregation functions are currently supported:

- sum
- count
- min
- max
 
#Usage

Here is how to use Rivus:

``` erlang
application:start(rivus_cep).

QueryStr = "define query1 as
                  select ev1.eventparam1, ev2.eventparam2, sum(ev2.eventparam3) 
                  from event1 as ev1, event2 as ev2
                   where ev1.eventparam2 = ev2.eventparam2
                    within 60 seconds; ".

Producer = event_producer_1.
{ok, SubscriberPid} = result_subscriber:start_link().

{ok, QueryPid} = rivus_cep:load_query(QueryStr, [Producer], [SubscriberPid], [{shared_streams, true}]).
    
%% create some evetnts
Event1 = {event1, gr1,b,10}.
Event2 = {event2, gr2,bbb,20}.

%% send the events
rivus_cep:notify(Producer, Event1).
rivus_cep:notify(Producer, Event2).

%% or if you don't care about the producers
rivus_cep:notify(Event1).
rivus_cep:notify(Event2).
	
```

The query is started with `rivus_cep:load_query/4`. It takes as arguments: query string, list of event producers, list of query result subscribers and options as a proplist.

Each query worker will register itself to the  [gproc](https://github.com/uwiger/gproc) process registry, for the events listed in the "from" clause.

If the events are sent via `rivus_cep:notify/1`, the event will be received by any query subscribed for this event type. With `notify/2` - only the queries subscribed to the particular Producer will receive the event.

For each query there must be at least one Subscriber to receive the query results.

See `tests/rivus_cep_tests.erl` for more examples. 

#Events representation

For each event type there must be a module implementing the `event_behavior` with the same name as the one used in the "from" clause. The important function that needs to be implemented is - `get_param_by_name(Event, ParamName)`.


#Streaming events via TCP

You can stream events via TCP connection:

```
  {ok, {Host, Port}} = application:get_env(rivus_cep, rivus_tcp_serv),
  {ok, Socket} = gen_tcp:connect(Host, Port, [{active, false}, {nodelay, true}, {packet, 4}, binary]),

  Event1 = {event1, 10, b, c},
  Event2 = {event1, 15, bbb, c},
  Event3 = {event1, 20, b, c},
  Event4 = {event2, 30, b, cc, d},
  Event5 = {event2, 40, bb, cc, dd},

  gen_tcp:send(Socket, term_to_binary({event, test_query_1, Event1})),
  gen_tcp:send(Socket, term_to_binary({event, test_query_1, Event2})),
  gen_tcp:send(Socket, term_to_binary({event, test_query_1, Event3})),
  gen_tcp:send(Socket, term_to_binary({event, test_query_1, Event4})),
  gen_tcp:send(Socket, term_to_binary({event, test_query_1, Event5})),
```

Currently only Erlang clients are supported, but events serialization using [BERT](http://bert-rpc.org/) is work in progress.


#Shared streams

For memory efficiency, `{shared_streams, true}` can be provided in the options list. In this case, the query will work with a shared event sliding window. The window's size will be equal of the maximum "within" clause of all sharing queries.
Queries based on event pattern use only non-shared windows.


###Dependencies

- [gproc](https://github.com/uwiger/gproc)
- [folsom](https://github.com/boundary/folsom)
- [lager](https://github.com/basho/lager) 

###Current limitations

There is a number of limitations/TODOs:

- using event aliases is mandatory (too lazy to implement a check for event parameter name duplication :) ) ;
- no benchmarks;
- needs extensive testing.
