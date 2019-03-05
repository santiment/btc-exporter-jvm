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

There are different projects for the exporter and for the block processor. To work on the exporter use in sbt:

``` sbt
project rawexporter
```

To switch to the block processor use

``` sbt
project blockprocessor
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


## CI/CD

This project is built using Jenkins. Jenkins will run all unit and
integration tests. The tests are run using the docker-compose file
`compose-test.yml` This file describes all the services that need to
be set-up during the integration tests

