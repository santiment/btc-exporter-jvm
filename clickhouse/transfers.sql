CREATE TABLE btc_transfers_kafka (
  in UInt8,
  ts UInt64,
  height UInt32,
  txPos UInt32,
  index UInt32,
  value Float64,
  address String
) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092',
                          kafka_topic_list = 'btc-transfers',
                          kafka_group_name = 'btc_transfers_clickhouse',
                          kafka_format = 'JSONEachRow';


CREATE TABLE btc_transfers(
  dt DateTime,
  height UInt32,
  txPos UInt32,
  out UInt8,
  index UInt32,
  value Float64,
  address String
) ENGINE = ReplacingMergeTree()
    PARTITION BY toYYYYMM(dt)
    ORDER BY (height, txPos, out, index);
  

CREATE MATERIALIZED VIEW btc_transfers_mv TO btc_transfers
AS
SELECT
  toDateTime(ts) as dt,
  height,
  txPos,
  (NOT in) as out,
  index,
  value,
  address
FROM btc_transfers_kafka;

-- DAA approx
select day, uniq(address) from btc_transfers group by toStartOfDay(dt) as day;

-- DAA exact
select day, uniqExact(address) from btc_transfers group by toStartOfDay(dt) as day;

