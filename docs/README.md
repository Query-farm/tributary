# DuckDB Tributary Extension

The **Tributary** extension provides seamless integration between DuckDB and [Apache Kafka](https://kafka.apache.org/), enabling real-time querying and analysis of streaming data. With this extension, users can consume messages directly from Kafka topics into DuckDB for immediate processing, as well as write processed data back to Kafka streams.

## Key Features

- **Direct Kafka Ingestion:** Stream records from Kafka topics directly into DuckDB tables using SQL.
- **Flexible Topic Consumption:** Supports consuming from specific partitions, offsets, or continuously from the latest messages.
- **Real-Time Analytics:** Perform analytical queries on streaming data as it arrives.
- **Kafka Output:** Optionally write results or processed data back to Kafka topics.
- **SQL-Native Interface:** Kafka integration is fully accessible via SQL, enabling easy adoption for data engineers and analysts.

## Installation

```sql
INSTALL tributary FROM community;
LOAD tributary;
```

## Example Usage
```sql

-- Scan an entire Kafka topic.
SELECT *
FROM tributary_scan_topic('test-topic',
  "bootstrap.servers" := 'localhost:9092'
);
```

# Documentation

See the [extension documentation](https://query.farm/duckdb_extension_tributary.html).

## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
cd <your-working-dir-not-the-plugin-repo>
git clone https://github.com/Microsoft/vcpkg.git
sh ./vcpkg/scripts/bootstrap.sh -disableMetrics
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/<extension_name>/<extension_name>.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `<extension_name>.duckdb_extension` is the loadable binary as it would be distributed.

### Tips for speedy builds
DuckDB extensions currently rely on DuckDB's build system to provide easy testing and distributing. This does however come at the downside of requiring the template to build DuckDB and its unittest binary every time you build your extension. To mitigate this, we highly recommend installing [ccache](https://ccache.dev/) and [ninja](https://ninja-build.org/). This will ensure you only need to build core DuckDB once and allows for rapid rebuilds.

To build using ninja and ccache ensure both are installed and run:

```sh
GEN=ninja make
```

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`. This shell will have the extension pre-loaded.

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```
