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

For each continuous query statement, similar to the above, a gen\_server or gen\_fsm \(in case of "pattern matching" query\) module will be created, using an [ErlyDTL](https://github.com/erlydtl/erlydtl) template. 

Internally, the events are stored in an ETS-based sliding window. DSL statments are translated to Erlang "match specifications" and QLC queries, which are embedded in the template.

Template-generated module will register itself to the  [gproc](https://github.com/uwiger/gproc) process registry, for the events listed in the "from" clause. To send events, gproc:send() should be used. Once sent, each event will be received by multiple subscibers (query modules).

For each event type there must be a module implementing the 'event' behavior with the same name used in the "from" clause. The important function that needs to be implemented is - get\_param\_by\_name(Event, ParamName).

See the unit tests for details how to use the library. There are several DSL examples too.

###Dependencies

- [ErlyDTL](https://github.com/erlydtl/erlydtl)
- [gproc](https://github.com/uwiger/gproc)
- [folsom](https://github.com/boundary/folsom)
- [lager](https://github.com/basho/lager)

###Current limitations

The project is in its infancy, so there is a number of limitations:

- avg() aggregation is yet to be implemented;
- using event aliases is mandatory;
- only sliding windows are supported (no 'batch' windows);
- no benchmarks at all;
- extensive testing needed.

###Contributions

Contributions and suggestions are most appreciated!