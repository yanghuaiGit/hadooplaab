/**
  {
    "e":"kline",
    "E":1685090490001,
    "s":"BTCUSDT",
    "k":{
        "t":1685090489000,
        "T":1685090489999,
        "s":"BTCUSDT",
        "i":"1s",
        "f":3126715399,
        "L":3126715401,
        "o":"26481.18000000",
        "c":"26481.17000000",
        "h":"26481.18000000",
        "l":"26481.17000000",
        "v":"0.06131000",
        "n":3,
        "x":true,
        "q":"1623.56101270",
        "V":"0.04800000",
        "Q":"1271.09664000",
        "B":"0"
    }
}
 */
CREATE TABLE t1
(
    `s` STRING,            -- 币种
    `k` ROW<                -- K线数据
        `o` STRING,        -- 开盘价
    `c` STRING,        -- 收盘价
    `h` STRING,        -- 最高价
    `l` STRING,        -- 最低价
    `v` STRING,        -- 成交量
    `q` STRING,        -- 成交额
    `V` STRING,        -- 主动买单成交量
    `Q` STRING,        -- 主动买单成交额
    `T` BIGINT         -- 收盘时间 (unix timestamp, milliseconds)
        >,
    proctime AS PROCTIME() -- 使用处理时间
) WITH (
      'connector' = 'kafka'
      ,'topic' = 'kline'
      ,'properties.bootstrap.servers' = '172.16.100.109:9092'
      ,'properties.group.id' = 'luna_g'
      ,'scan.startup.mode' = 'latest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      );
SELECT
    `s` as `币种`,
    TUMBLE_END(proctime, INTERVAL '5' MINUTE)  as `结束时间`,
    MAX(CAST(`k`.`h` AS DECIMAL))  as `最高价`,
    MIN(CAST(`k`.`l` AS DECIMAL))  as `最低价`,
    SUM(CAST(`k`.`q` AS DECIMAL)) as `成交总额`,
    SUM(CAST(`k`.`Q` AS DECIMAL)) as `主动买单成交额`
FROM t1
GROUP BY `s`, TUMBLE(proctime, INTERVAL '5' MINUTE);



