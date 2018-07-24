# Bitcoin Transfer exporter

A Kafka Connector which allows to push bitcoin transfers to a kafka topic


## Development environment

This project requires the following dependencies: Bitcoind RPC server,
Kafka, Zookeeper. We have set up a development environment using
Docker Compose which contains Kafka and Zookeeper instances. You will
have to set up the Bitcoin server yourself.

To start the development environment:

1) Copy `.env.sample` to `.env` and edit the variables `BITCOIND_*`.

2) Build the sbt container:

``` sh
export UID
export GID
docker-compose build sbt
```

2) Run sbt using docker-compose:

``` sh
docker-compose run sbt
```

## Testing

### Unit tests
From the dev environment call:

``` sbt
test
```

sbt can watch for file changes and rerun the tests whenever it notices
a change. To watch for file changes run:

``` sbt
~test
```

### Integration tests
From the dev environment:

``` sbt
it:test
```
Similarly you can call `~it:test` to rerun tests on file change.



## Environment variables
The build process depends on some parameters which are given to sbt
using environment variables. To ease development you can use a `.env`
file. During the build process all variables in the .env file will be
put in the environment.


## Building

To build the final docker image use

``` sh
$ docker build -t localhst/btc-exporter-jvm .
```

This will build the image and publish it locally.

## CI/CD

This project is built using Jenkins. Jenkins will run all unit and
integration tests. The tests are run using the docker-compose file
`compose-test.yml` This file describes all the services that need to
be set-up during the integration tests

## Operations

This program reads Bitcoin blocks from the `bitcoind` server and
writes data to Kafka. It uses Zookeeper to keep track of the last
processed block. We try to keep exactly-once semantics. This means
that every block will be processed and the data will be written to
Kafka exactly one time - there will be no missing data and no
duplicated messages. To achieve this guarantee the process uses a
certain commit procedure which guarantees that either data will be
written exactly once in Kafka or the process will fail to start.

If the commit procedure fails somehow the process will fail with the message: `Inconsistent state: written=$written, committed=$committed`. To fix that one has to perform the following procedure:

1) Check the kafka topic and see what is the number of the last block written there. One can do that with:

``` sh
$ kafka-console-consumer.sh --bootstrap-server $KAFKA_URL --topic btc-transfers-1 --partition 0 --offset LASTOFFSET--isolation-level read_committed 

```

To get an offset close to the last one run

``` sh
bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $KAFKA_URL --topic btc-transfers-1
```

Then for LASTOFFSET choose the given offset - 100

2) Check the values of the zookeeper variables that keep the last block. The easiest way to do this is with sbt

``` sh
$ docker-compose run -e ZOOKEEPER_URL=url-to-zookeeper sbt sbt console

scala>
```

This will start the scala interpreter. Then do

``` scala
scala> import net.santiment.Globals._

scala> lastWrittenHeightStore.read
res2: Option[Int] = Some(1233)

scala> lastCommittedHeightStore.read
res3: Option[Int] = Some(1232)
```

If there was a problem the values of `lastWrittenHeightStore` and `lastCommittedHeightStore` should differ by 1.

3) Change the value in Zookeeper.

According to the last block height written in Kafka you would either have to increase `lastCommittedHeightStore` or to decrease `lastWrittenHeightStore` by 1. For example:

``` scala
scala> lastCommittedHeightStore.update(1233)
```


After the two values have been made equal you can safely restart the btc-exporter.
