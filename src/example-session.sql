.mode line
select * from tributary_metadata(
  "bootstrap.servers" := 'localhost:9092'
);

SELECT * FROM tributary_scan_topic('test-topic',
  "bootstrap.servers" := 'localhost:9092'
);

.mode duckbox
SELECT tributary_version();