## Overview

This design document aims to define with precision the definition of a source
type. The definition should be complete and cover all aspects of a source, from
sql parsing and planning, to managing external state, to ultimately getting data
in dataflows.

## Goals

* Define a family of traits that capture the behaviour of a source. A correct
  implementation of those traits should be all that's needed to teach
  materialize how to ingest a new source
* Consolidate all the various bits and pieces needed for a source under a
  separate crate.  A minimal source implementation currently requires [touching
  many files](https://github.com/MaterializeInc/materialize/pull/6189) in many
  different crates often in repetitive ways.  In an ideal world you could
  imagine having `src/sources/src/postgres`, `src/sources/src/kafka`, etc, with
  each module containing the corresponding trait implementations and the rest of
  the system automatically picking them up.

## Assumptions

The traits defined below have been based on a few assumptions about the system.
The assumptions are listed here explicitly to both explain the following traits
and also debate them on their own.

* **We need to track and gracefully cleanup external state every time we talk to
  an external system**

For some external systems (e.g Postgres) we need to create foreign objects in
order to consume their data. It is desirable that materialize automatically
creates and crucially, cleans up this state when a dataflow is retired or a user
`DROP`s the source. The consequence of stating this assumption is the definition
of the `InstanceConfig` trait that must be able to be `Serialize`d into the
catalog.

* **Materialize is a multi-node system, with the timely cluster being separate
  from the coordintator node.**

This assumption drives the separation between the `InstanceConfig` and the
`Instance` itself. From the previous assumptions it followed that whenever the
coordinator decides to read a source it needs to create and persist its
configuration. Since the workers that will actually consume this source might be
on another machine the `InstanceConfig` must sendable over the network. It is
only when it reaches the final worker that it can be converted into the actual
`Instance`.

* **A source type might benefit from transient state per node**

An example of transient state could be a TCP connection that can be reused
between many `Instance`s on workers of the same node. For this reason, each
`Source` implementation can provide a `Factory` that serves as a singleton per
node and can hold any transient state that can make `Instance`s more
performant/resilient/or just better. It is crucial that any state stored in the
`Factory` be considered transient and safe to lose at any point.

* **Source SQL syntax is simple enough to derive directly from their types**

This is more of an assumption for the future rather than a statement about the
current sources since if we look at the existing ones we can find violations of
it. The goal of this assumption is to strongly urge new source implementations
to rely on just their struct definition and a `Deserialize` implementation for
their parsing needs instead of parsing directly. The benefit is twofold. Firstly,
it is much easier to define the expected syntax, and secondly our syntax will
naturally follow a pattern as a consequence of not giving arbitrary access to
the parser.

## Source architecture

The following diagram describes the state of the system after a `CREATE
MATERIALIZED SOURCE foo ...` statement. Here is what happened.

First, the `Source` describes the result of parsing `Source::SqlOptions` and
`Source::WithOptions` and using `Source::new` to ensure that what the user
typed in their create statement was valid. `Source` needs to implement `ToSql`
which is needed to persist it in the catalog. When materialize restarts the
opposite procedure (`FromSql`) will be used to recreate it.

Since the user requested a materialized view the coordinator called
`Source::create_instances(4)` to signal to the source that there are at most 4
workers available. This particular implementation has only three shards, and so
it returned three `Config`s to the coordinator. After serializing and storing
these `Config` objects in the catalog, the coordinator constructed a
`DataflowDesc` including these `Config`s and sent them to the dataflow layer.

The dataflow layer consisted of two nodes with two theads each. Since there were
only three `Config` objects the first node was assigned two, and the second one
the third. This assignment is arbitrary.

The first time each node sees a particular source config, it constructs the
corresponding singleton factory in order to instancate it. Then, for each
`Config` that is assigned to each worker it uses `Factory::instantiate` to start
receiving data from this particular `Instance`.

[![https://imgur.com/oBqep3v.png](https://imgur.com/oBqep3v.png)](https://imgur.com/oBqep3v.png)

```rust
/// The trait that defines a new kind of source.
trait Source: ToSql + FromSql {
    /// The name after CREATE SOURCE FROM <name>
    const NAME: &'static str;

    /// Type holding any parameters than can be specified using SQL keywords.
    /// For example CREATE SOURCE FROM <name> HOST 'example.com'` would
    /// correspond to `SqlOptions` having a field `host` of type `String`.
    ///
    /// Types that implement `Deserialize` automatically implement `FromSql`
    type SqlOptions: FromSql;

    /// Type holding any parameters than can be specified in the `WITH` section.
    /// For example CREATE SOURCE FROM <name> WITH (foo = 'bar') would
    /// correspond to `WithOptions` having a field `foo` of type `String`.
    ///
    /// Types that implement `Deserialize` automatically implement `FromSql`
    type WithOptions: FromSql;

    /// Type used to construct instances of this source
    type Factory: InstanceFactory;

    /// Constructs a new source from the parsed SQL and WITH option. This is an
    /// opportunity for the implementation to do any sanity check of the
    /// parameters passed
    fn new(sql_options: Self:Options, with_options: Self::WithOptions) -> Result<Self, Error>;

    /// Creates at most `workers` instance configs. These configs will be
    /// persisted in the catalog and be used to do any associated cleanup later.
    /// It is up to the system to decide how the returned configs will be
    /// distributed among the dataflow workers and converted into instances
    fn create_instances(&self, workers: usize) -> Vec<<Self::Factory as InstanceFactory>::Config>;

    /// The schema that will be produced by this source
    fn desc(&self) -> RelationDesc;
}

trait InstanceFactory {
    /// Holds all the configuration required to construct an instance of this source.
    type Config: InstanceConfig;

    /// The actual instance type that will produce data into the dataflow from a
    /// particular worker
    type Instance: DataflowStream;

    /// Constructor for this Factory. This can be a central object managing
    /// creation of individual instances. Transient state can be shared within
    /// this type (e.g TCP connection sharing, dynamic library loading, etc)
    fn new() -> Self;

    /// Creates a source instance given its configuration
    fn instantiate(&mut self, config: Self::Config) -> Self::Instance;
}

trait InstanceConfig: Serialize + Deserialize {
    /// Optional cleanup logic to run when an instance config is dropped. The
    /// default implementation simply is a no-op
    async fn cleanup(&mut self) -> Result<()> {
        Ok(())
    }
}

trait DataflowStream {
    /// A frontier beyond which all updates will be precise.
    ///
    /// Times not beyond this frontier may have been subject to logical
    /// compaction, and so may not accumulate correctly. Practically,
    /// this means that this source should not be used to produce output
    /// updates at times that are not beyond this frontier.
    ///
    /// The `AntichainToken<Time>` describes an antichain and pins the
    /// antichain so that the source will not advance logical compaction
    /// as long as it exists. It can be further manipulated to relax the
    /// requirements without needing to re-call `valid_frontier()`.
    ///
    /// A `None` result means that there is no notion of correctness,
    /// perhaps because the source is ephemeral. Sources with a `None`
    /// result are warning the user away from consistency guarantees
    /// about the source.
    fn valid_frontier(&self) -> Option<AntichainToken<Timestamp>>;

    /// A frontier beyond which data may differ on restart.
    ///
    /// The described source may have produced data that is not entirely
    /// durable, in that a future instantiation could provide different
    /// data for updates at times beyond this frontier. Updates at times
    /// not beyond this frontier are certain to be re-presented as they
    /// have been presented, in all future instantiations.
    ///
    /// This information is valuable for downstream durability mechanisms,
    /// which may either hold back data to this frontier, or release it
    /// and propagate the lower bound of frontiers onward as durability.
    ///
    /// A `None` result means that the data are not durable at all.
    fn durable_frontier(&self) -> Option<Antichain<Timestamp>>;
}
```

## Example with pubnub


### Syntax parsing


```rust
#[derive(Deserialize)]
struct KafkaOptions {
    broker: String,
    topic: String,
}

#[derive(Deserialize)]
struct KafkaWithOptions {
    client_id: Option<String>,
    cache: Option<bool>,
    group_id_prefix: Option<String>,
    ignore_source_keys: Option<bool>,
    isolation_level: Option<String>,
    security_protocol: Option<String>,
    kafka_time_offset: Option<i64>,
    statistics_interval_ms: Option<i64>,
    start_offset: Option<i64>,
    timestamp_frequency_ms: Option<i64>,
    topic_metadata_refresh_interval_ms: Option<i64>,
    enable_auto_commit: Option<bool>,
    ssl: KafkaSslOptions,
    sasl: KafkaSaslOptions,
}

#[derive(Deserialize)]
struct KafkaSslOptions {
    certificate_location: Option<String>,
    key_location: Option<String>,
    key_password: Option<String>,
    ca_location: Option<String>,
}

#[derive(Deserialize)]
struct KafkaSaslOptions {
    mechanisms: Option<String>,
    username: Option<String>,
    password: Option<String>,
    kerberos: KafkaKerberosOptions,
}

#[derive(Deserialize)]
struct KafkaKerberosOptions {
    keytab: Option<String>,
    kinit_cmd: Option<String>,
    min_time_before_relogin: Option<String>,
    principal: Option<String>,
    service_name: Option<String>,
}

struct Kafka {
    options: KafkaOptions,
    with_options: KafkaWithOptions,
};

impl Source for Kafka {
    type Options = KafkaOptions;
    type WithOptions = KafkaWithOptions;

    fn new(options: Self::Options, with_options: Self::WithOptions) -> Result<Self, Error> {
        // validation
    }
}
```

## Dynamic resource sharing
