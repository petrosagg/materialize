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

Reclocking a method for translating the events of a stream from one gauge of progress (read:
timestamp) to another. In Materialize we employ reclocking to translate from the native way a
particular source of data describes progress (e.g Kafka offsets, Postgres LSNs) into a common
timeline of milliseconds since the unix epoch.

Let's name the timestamp of the stream to be reclocked `FromTime` and the timestamp we want to
reclock into `IntoTime`. The goal is then is come up with a way to associate each `from_ts`
timestamp of an event to an `into_ts` timestamp and emit it in the reclocked stream. Doing that
gets us half the way there but remember that streams in Materialize also carry a second piece of
information, progress. Our association of `FromTime` to `IntoTime` must be done in a way that we
know when there will be no more events mapped to some particular `into_ts`, meaning that this
timestamp is no longer in progress.

Informally what we want to do is look at our real time clock and at the same time look at the
current maximum offset present in the source. We now have an association of a real time timestamp
and an upper bound on the offsets that exist in the source which we will proceed and write down
durably. Materialize already has an abstraction for recording values that change over time, the
differential collection! Let's walk through an example. Suppose we check for offsets at 1pm, 2pm
and 3pm and the kafka offsets are 100, 150, and 300 respectively. Using these observations we can
generate a differential collection that looks like this:


```
(100, 1pm, +1)

(100, 2pm, -1)
(150, 2pm, +1)

(150, 3pm, -1)
(300, 3pm, +1)
```

This is exactly the representation that materialize uses for normal tables and so by writing things
down this way we can easily explore the contents of this collection via SQL. Here's what this would
look like:


```sql
> SELECT * FROM topic_a_progress AS OF 2pm
offset
------
150

> SELECT * FROM topic_b_progress AS OF 3pm
offset
------
300
```

This is great! We have a produced and durably recorded a description of the previously implicit
association of the real time with the progression of offsets in a topic. What's left to figure out
is how to use this information to transalate event timestamps and progress statements from the
offset domain to the real time domain. Here is the rule we'll follow:

For every association between offset X and time T that we write down.






## Multiple partitions and partially ordered times



## Compaction friendly
