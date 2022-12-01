In this blog post we'll see how Materialize uses a technique we call reclocking in order to provide
awlays consistent query results when those queries refer to data coming from external systems, such
as a Kafka source or a PostgreSQL database.

Materialize is a streaming SQL database so what does consistency mean in this context? Materialize
stores and manipulates streams of data that convey two pieces of information as they evolve. First,
every event that appears in a stream is associated with a timestamp which prescribes at exactly
what time that event took place. Secondly, each stream provides a notion of progress which allows a
consumer of the stream to know when they have seen all the data for a particular timestamp. Results
produced by Materialize are no exception to this and are also timestamped streams with progress
statements. The consistency proprety that Materialize always satisfies then is that it only
presents results to users when those results are final. That is to say, you can only observe the
result of a query at timestamps that are not still "in progress".

## Motivation for reclocking

The above is great news for users that care about correctness but it creates some tension when we
start looking at data imported from external systems. Let's see why. For now we will assume that
our external source is a Kafka topic with a single partition. Things get more interesting when
multiple partitions are involved and we'll come back to those. For those not familiar with Kafka, a
partition of a topic stores a sequence of arbitrary messages where each message is associated with
an offset. When adding a message to a partition its offset will be higher than any other message
already in the partition, usually just one plus the latest offset.

Visualizing the above we see this picture:

```
topic_a: [ m1, m2, m3, m4, m5 ]
```

This looks a lot like a stream of data, so let's try to treat it as such. We can use the offset of
each message as its timestamp and we can easily derive progress statements since we know that each
offset is only used once.

**Note:** Even though the word "timestamp" strongly implies our colloquial understanding of time,
as far as a stream is concerned the notion of time only needs to be expressive enough to allow us
to say whether event A happened before event B, and offsets perfectly fit that role.

Let's run some `SELECT` queries against this source to get a feel for those offset timestamps. When
issuing a plain `SELECT` query Materialize will pick an appropriate time to run it at but we can
also request that a specific time is used with the `AS OF` statement. We can see it in action here:

```sql
> SELECT * FROM topic_a AS OF 2
msg
---
m1
m2

> SELECT * FROM topic_a AS OF 5
msg
---
m1
m2
m3
m4
m5
```

Great! ..or so it seems. Let's ingest a second topic that happens to have a lot more data and run
some interesting queries. We'll assume the second topic has one thousand messages like so:

```
topic_b: [ m1, m2, .., m1000 ]
```

Selecting data from it works as expected:

```sql
> SELECT * FROM topic_b AS OF 1000
msg
---
m1
m2
...
m1000
```

Materialize is a SQL database so we should be able to join these two Kafka topics and get some
interesting results. But if we attempt to do this we find ourselves in a pickle:

```
> SELECT * FROM topic_a JOIN topic_b AS OF ????
```

We have no sensible timestamp value to set, because the two offsets are completely unrelated.
However, from the perspective of the user the offsets *are* related. The user lives in the real
world and experiences events according to real time and so from their perspective joining the two
topics should take into account all five records of `topic_a` and all 1000 records of `topic_b`
because this is the state of the two collection *at the same time*.

How do we explain to Materialize the relationship between offsets and real time in a way that
preserves the correctness properties that we laid out in the beginning? Enter reclocking.

## Reclocking a stream

Reclocking is a method that allows us to capture this correspondence of real time to source
offsets. More formally, it is a method for translating the events of a stream from one gauge of
progress (read: timestamp) to another gauge of progress. In Materialize we employ reclocking to
translate from the native way a particular source of data describes progress (e.g Kafka offsets,
Postgres LSNs) into a common timeline of milliseconds since the unix epoch.

In order to explore how Materialize uses reclocking we need to first look at the concept of
progress and upper frontiers. As we said in the beginning, Materialize deals with streams of data
that carry two pieces of information, data and progress. The easiest to visualize these streams is
to imagine the possible timestamps in an axis where at each time there can be one or more message.
The progress in that stream is represented by a frontier that can be thought of as a line that
split the times into two areas. The times that are beyond the frontier are times for which we might
receive data in the future. The times that are not beyond the frontier are times that we have
already seen all the data there is for them.

```
                      m6
                m3    m5
messages    m1  m2    m4
----------------------------
time        0   1  ^  2   3
                   |
                 upper
                frontier
```

If the diagram above was our state of reading this particular stream we can be certain that at
timestamp `1` there are only two messages. We can't say the same for timestamp `2` however because
it is beyond the upper frontier. All we know is that we have received two messages but there might
be more. We can represent a frontier as the set of smallest times for which we might see data in
the future. In this example the upper frontier is simply `{2}`. You might be wondering why use a
set for the frontier. For one, it allows to represent that we have fully processed a stream. When
this happens the upper frontier becomes the empty set, which means that there are no times for
which we might see data in the future, we have seen it all. Additionally, it allows us to represent
more complex frontiers that can't be represented by a single number. Hold on to that thought for
now.

With the understanding of frontiers under our belt we can now describe the reclocking process.
Informally what we want to do is to periodically write down an association of the real time clock
and the upper frontier of all the sources. Every time we write down one of these associations we
make a statement that "at real time `t` this source had complete data until frontier `f`".

Let's go back to the example of a slow moving topic and a fast moving topic from above. If we
looked at their upper frontiers every hour we would observe something like this:

```
topic_a:   0    1     3     6

topic_b:   0    100   250   1000

real time: 1pm  2pm   3pm   4pm
```

In order to record our observations which we do periodically we need some way of representing
collections that evolve over time. Turns out we already have a way of doing that in Materialize,
storing collections that evolve in time is what it is all about! The representation of these
evolving collections is a stream of changes that happened at a particular timestamp. Each change
looks like a triplet of `(data, time, diff)` where `data` is *what* changed, `time` is *when* it
changed, and `diff` is *how* it changed. For now we will think of `data` as a row and `diff` is an
integer where `+1` represents that the row was inserted and `-1` representing that a row was
deleted.

Let's see how we would represent our observations for `topic_a`:

```
(0, 1pm, +1) // At 1pm topic_a was empty, as no timestamp is before 0

(0, 2pm, -1) // At 2pm we delete the fact that it was empty and replace it with the fact that its
(1, 2pm, +1) // upper was at 1. Not that this happens atomically as these events happen at the same time

(1, 3pm, -1)
(3, 3pm, +1)

(3, 4pm, -1)
(6, 4pm, +1)
```

Similarly our recorded observations for `topic_b` would look like this:

```
(   0, 1pm, +1) // At 1pm topic_b was empty, as no timestamp is before 0

(   0, 2pm, -1) // At 2pm we delete the fact that it was empty and replace it with the fact that its uuper
( 100, 2pm, +1) // upper was at 100. Not that this happens atomically as these events happen at the same time

( 100, 3pm, -1)
( 250, 3pm, +1)

( 250, 4pm, -1)
(1000, 4pm, +1)
```

We call these collections "remap collections" and we generate one for every source we ingest. These
collections can be queried via SQL just like any other collection in Materialize. For example we
could ask until which offset of `topic_b` did materialize knew about at `3pm` with a query like
this:

```sql
> SELECT * FROM topic_b_remap AS OF 3pm
offset
------
250
```

We are finally ready to tackle the problem that we faced before were we couldn't find a suitable
timestamp at which to evaluate this query:

```sql
SELECT * FROM topic_a JOIN topic_b AS OF ????
```

We can use the remap collection that we created for each topic and generate two new collections.
Let's call them `topic_a_reclocked` and `topic_b_reclocked`. These collection will have the exact
same updates as the original streams but we will switch their timestamps according to the remap
colletion. Our goal is to produce a collection that at every time `t_reclocked` contains all the
messages that were not beyond the frontier that was recorded at the remap collection at
`t_reclocked`. This was a mouthful so let's see how that would pan out for `topic_a`.

The raw collection for `topic_a` looks like this:

```
(m0, 0, +1)
(m1, 1, +1)
(m2, 2, +1)
(m3, 3, +1)
(m4, 4, +1)
(m5, 5, +1)
```

After applying the reclocking transformation we end up with a collection `topic_a_reclocked` that
looks like this:


```
(m0, 2pm, +1) // At 2pm the frontier was {1} so only the message at time 0 is present
(m1, 3pm, +1)
(m2, 3pm, +1)
(m3, 4pm, +1)
(m4, 4pm, +1)
(m5, 4pm, +1)
```

If we do the same transformation for `topic_b` and recreate `topic_b_reclocked` we can revisit our
original query as:

```sql
SELECT * FROM topic_a_reclocked JOIN topic_b_reclocked AS OF 2pm
```

This will now correctly produce the join of these two relations using the contents they had when we
checked at 2pm! It turns out that this is so useful that Materialize doesn't even expose, or
record, the raw offset-timestamped collections. When a source is ingested in materialize the remap
collection is generated on the fly and the reclocked collection is recorded. This features brings
all ingested sources onto a common timeline that enables writing queries that mix and match data
from completely different source streams.

### Guarantees of the reclocking process

Keen readers might have noticed that when we generated the `topic_a_reclocked` collection we lost
some information. While with the raw offsets we knew that `m3` happened before `m4`, in the
reclocked collection we only know they happened at the same time, 4pm. Could this be a problem? It
turns out that this artifact cannot lead to wrong results. The reason is that Materialize only
allows users to write computations that can be thought of as transformating the whole collection at
each particular time. In other words, it is not allowed for any computation to produce results at
that depend at any other state than the state of the collection at that particular timestamp. This
means that even though we lost some granularity because we recorded the frontier every hour we can
be rest assured that the result for 4pm will be correct.

Another consideration for the reclocking process is to respect transaction boundaries. Transactions
in streams are represented by multiple changes that happen at the same time. We saw a flavor of
those just a moment ago when we retracted a statement in the remap collection for `topic_a` at 2pm
and immediately added a new statement. This represents a transaction Materialize will work hard to
never present data at a timestamp for which we don't have the complete data of. The reclocking
process respects that and makes the following guarantees about two events that happened at two
offsets `a` and `b`:

* If `a <= b` then `reclock(a) <= reclock(b)`
* If multiple messages happened at `a` then all of them will be reclocked at `reclock(a)`

## Multiple partitions and partially ordered times

So far we have dealt with topics that had a single partition, but in practice kafka topics may have
multiple partitions and even change the number of them dynamically. In order to successfully
capture these we need to depart from the common notion of a stream as a *sequence* of events and
instead start thinking in terms of partially ordered times. When a stream is timestamped with a
partial ordered time it means that we may come across two events for which we cannot say whether
one happened before the other. Let's look at a kafka topic with two partitions:

```
offsets  0   1   2   3
----------------------
part0:   a   b   c   d
part1:   e   f   g   h
```

In this example message `e`, which happened at timestamp `(part1, e)` is *incomparable* to message
`c` which happened at timestamp `(part0, 2)`. We simply cannot know if a user first published `a`,
`b`, `c`, `d` and then `e` or some other order. We do however know that `b` came before `c`,
because they are in the same partition.

So how can we capture the notion of progress in a partially ordered time domain? Previously we
represented the frontier as a point that separated the times in times that were beyond and not
beyond that point. Naturally, we can extend this notion and imagine a line that split the partially
ordered time domain into two *areas*.


```
offsets         0   1   2     3
---------------------------------
              ,             ,    ,
       part0: | a   b   c  /  d  |
              |    ,------´  ___/
       part1: | e / f   g   / h
          ,--´    |         |
       upper    upper    upper
      at 1pm   at 2pm   at 3pm
```

This is what the frontier would look like if at `2pm` we looked at this kafka topic containing two
partitions and `part0` contains `a`, `b`, `c` and `part1` contained only `e`. Every time we check
each partition might have progressed a bit and this defines the shape of the frontier as time goes
by. We previously represented the partition as a set containing a single timestamp but to capture
this multi-partition topic we will have to include multiple timestamps, one for each partition.
More generally, we would like our frontier to contain the minimum set of *incomparable* timestamps
beyond which we might still see data.

In the example above here is what we would record in our remap collection:

```
((part0, 0), 1pm, +1)
((part1, 0), 1pm, +1)

((part0, 0), 2pm, -1)
((part1, 0), 2pm, -1)
((part0, 3), 2pm, +1)
((part1, 1), 2pm, +1)

((part0, 3), 3pm, -1)
((part1, 1), 3pm, -1)
((part0, 4), 3pm, +1)
((part1, 3), 3pm, +1)
```

After combining this remap collection with the original data we get the following reclocked
collection:

```
(a, 2pm, +1)
(b, 2pm, +1)
(c, 2pm, +1)
(e, 2pm, +1)

(d, 3pm, +1)
(f, 3pm, +1)
(g, 3pm, +1)

// h is missing because it is still beyond the latest upper frontier
```

By using partially ordered times we have managed to capture a collection that evolves according to
a partial order and reclock it onto a totally ordered time domain! Reclocking can also be used to
map a partially order time domain onto another partially ordered time domain but the mechanics of
that are left as an exercise to the reader.


## Conclusion

We saw how reclocking allows us to have a common consistent view of otherwise unrelated streams
by recording a small amount of metadata. This invisible process that happens automatically upon
data ingestion is what allows users of Materialize to freely mix and match sources in their queries
while ensuring that transaction boundaries are respected and the answers provided by Materialize
will always be consistent with the upstream systems the data came from.
