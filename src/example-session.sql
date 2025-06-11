.mode line
select * from tributary_metadata(
  "bootstrap.servers" := 'localhost:9092'
);

explain analyze
select * from tributary_scan_topic('test-topic',
  "bootstrap.servers" := 'localhost:9092'
);