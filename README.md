# SQL Extractor

A service that schedules and executes queries against a relational database and writes the output to files on the local filesystem or S3.

The primary use case is batch extraction of data for an ETL pipeline.

## Getting Started

This service handles the lifecycle of scheduling the queries and writing them to files. It delegates execution of queries to [SQL Agent](https://github.com/chop-dbhi/sql-agent).

The minimum requirement is a [`config.yml` file](./config.yml). The main sections of the config include:

- `sqlagent` - The connection info to the SQL Agent service.
- `connections` - A map of database connection info by name.
- `queries` - An array of queries defined inline or referencing a file.
- `schedule` - The schedule to run this set of queries.

Additional options are provided to define where files are written and their format.

## Deployment

Currently the simplest way to deploy this service is to use Docker. A [docker-compose.yml](./docker-compose.yml) is provided for reference.

## Development

The [dep](https://github.com/golang/dep) tool is used for managing dependencies. Install by running:

```
go get github.com/golang/dep/...
```

Then run the following to install the dependencies:

```
dep ensure
```
