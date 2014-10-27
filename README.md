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

Multiple criteria separated by `','` could be used. The above query will filter out all the `event1` events, which do not satisfy the condition `eventparam1>5 AND eventparam1<30` and also the `event2` for which `eventparam1<50` is not true.
The difference netween the filters and `where` clause criteria is that the filters are executed before the event to be persisted in memory. In this way the user can reduce the event volume in Result calculations.

#Aggregations

The following aggregation functions are currently supported:

- sum
- count
- min
- max
 
#Usage

Here is how to use Rivus:

Clone and build:

``` bash
$ git clone https://github.com/vascokk/rivus_cep.git
$ ./rebar get-deps
$ ./rebar compile

```

Update `rel/vars.config` according to your preferences or use the default values. 

Create a release using `relx`:

``` sh
$ ./relx

```

Start the application:

``` sh
$ ./_rel/rivus_cep/bin/rivus_cep console

```

Try the following in the Erlang console:

``` erlang

%% define the events to be recognised by the engine:
EventDefStr1 = "define event1 as (eventparam1, eventparam2, eventparam3);".
EventDefStr2 = "define event2 as (eventparam1, eventparam2, eventparam3);".

rivus_cep:execute(EventDefStr1).
rivus_cep:execute(EventDefStr2).

% deploy the query
QueryStr = "define correlation1 as
                     select ev1.eventparam1, ev2.eventparam2, ev2.eventparam3, ev1.eventparam2
                     from event1 as ev1, event2 as ev2
                     where ev1.eventparam2 = ev2.eventparam2
                     within 60 seconds; ".

Producer = event_producer_1.
{ok, SubscriberPid} = result_subscriber:start_link().

{ok, QueryPid, QueryDetails} = rivus_cep:execute(QueryStr, [Producer], [SubscriberPid], [{shared_streams, true}]).
    
%% create some evetnts
Event1 = {event1, 10, b, c}.
Event2 = {event1, 15, bbb, c}.
Event3 = {event1, 20, b, c}.
Event4 = {event2, 30, b, cc}.
Event5 = {event2, 40, bb, cc}.

%% send the events (if you do not care about the producers, you can use notify/1)
rivus_cep:notify(Producer, Event1).
rivus_cep:notify(Producer, Event2).
rivus_cep:notify(Producer, Event3).
rivus_cep:notify(Producer, Event4).
rivus_cep:notify(Producer, Event5).

%% result
%% you should get:
%% {ok,[{10,b,cc,b},{20,b,cc,b}]} 

gen_server:call(SubscriberPid, get_result).
	
```

The query is started with `rivus_cep:execute/4` (previously `load_query/4`, which is now deprecated). It takes as arguments: query string, list of event producers, list of query result subscribers and options as a proplist.

Each query worker will register itself to the  [gproc](https://github.com/uwiger/gproc) process registry, for the events listed in the "from" clause.

If the events are sent via `rivus_cep:notify/1`, the event will be received by any query subscribed for this event type. With `notify/2` - only the queries subscribed to the particular Producer will receive the event.

For each query there must be at least one Subscriber to receive the query results.

See `tests/rivus_cep_tests.erl` for more examples. 

#Events representation

Events are tuples in the format: `{<name>, <attribute 1>, <attribute 2>,.....,<attribute N>}`. The `<name>` must be unique.
For each event type there must be a module implementing the `event_behavior` with the same name as the name of the event. The important function that needs to be implemented is - `get_param_by_name(Event, ParamName)`.
You can define events in runtime, using the following statement with `rivus_cep:execute/1`:

```
define <name> as (<attribute 1>, <attribute 2>,.....,<attribute N>);
```

Here is an example (you can find it in the eunit suites):

``` erlang
  {ok,Pid} = result_subscriber:start_link(),

  EventDefStr = "define event11 as (attr1, attr2, attr3, attr4);",
  QueryStr = "define query1 as
                     select sum(ev1.attr1)
                     from event11 as ev1
                     within 60 seconds; ",

  ok = rivus_cep:execute(EventDefStr),

  {ok, QueryPid, _} = rivus_cep:execute(QueryStr, [test_query_1], [Pid], []),

  Event1 = {event11, 10,b,c},
  Event2 = {event11, 15,bbb,c},
  Event3 = {event11, 20,b,c},
  Event4 = {event11, 30,b,cc,d},
  Event5 = {event11, 40,bb,cc,dd},

  rivus_cep:notify(test_query_1, Event1),
  rivus_cep:notify(test_query_1, Event2),
  rivus_cep:notify(test_query_1, Event3),
  rivus_cep:notify(test_query_1, Event4),
  rivus_cep:notify(test_query_1, Event5),

  timer:sleep(2000),

  {ok,Values} = gen_server:call(Pid, get_result)
```

#Streaming events via TCP

You can stream events via TCP connection:

``` erlang
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

#Benchmarking

You can load-test your queries using [Basho Bench](https://github.com/basho/basho_bench). Use the Basho Bench driver `basho_bench_driver_rivus.erl` and configuration file `rivus.config` provided in the `/priv` directory. Edit `rivus.config` according to your needs.


#Dependencies

- [gproc](https://github.com/uwiger/gproc)
- [folsom](https://github.com/boundary/folsom)
- [lager](https://github.com/basho/lager) 

#Disclaimer

This is not a production ready project. You can use it on your own risk 



