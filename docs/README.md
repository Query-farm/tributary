<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# DuckDB Tributary Extension

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/tributary.html)
[![v1.5 build](https://github.com/Query-farm/tributary/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/tributary/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

The **Tributary** extension provides seamless integration between DuckDB and [Apache Kafka](https://kafka.apache.org/), enabling real-time querying and analysis of streaming data. With this extension, users can consume messages directly from Kafka topics into DuckDB for immediate processing, as well as write processed data back to Kafka streams.

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/tributary](https://query.farm/products/extensions/tributary)**

## Installation

```sql
INSTALL tributary FROM community;
LOAD tributary;
```

## Development

For instructions on building the extension from source and running its tests, see [BUILDING.md](BUILDING.md).
