# Bitcoin Transfer exporter

A Kafka Connector which allows to push bitcoin transfers to a kafka topic


## Setting up your development environment

### Environment variables
The build process depends on some parameters which are given to sbt
using environment variables. To ease development you can use a `.env`
file. During the build process all variables in the .env file will be
put in the environment.

After you first clone this repository copy the provided sample file
`.env.sample` to `.env` and then edit whatever variables are necessary

## Installing

???

## Building

To build the final docker image use

``` sh
$ sbt docker:publishLocal
```

This will build the image and publish it locally. You need to have
`docker` installed in the environment where `sbt` is run.


