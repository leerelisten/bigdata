CREATE TABLE IF NOT EXISTS dw.dwd_place_order_change_sale_records
(
    kafka_offset    bigint COMMENT 'kafka的offset',
    change_datebase string COMMENT '变更数据库名',
    table_name      string COMMENT '变更表名',
    table_type      string COMMENT '变更类型',
    operate_time    string COMMENT '更新时间',
    id              bigint COMMENT 'mysql表的主键id字段',
    member_id       bigint COMMENT 'member_id',
    special_id      string COMMENT '期次ID',
    new_sales_id    INT COMMENT '新sales_id',
    new_department  int COMMENT '新部门',
    new_user_group  int COMMENT '新组',
    old_sales_id    INT COMMENT '旧sales_id',
    old_department  int COMMENT '旧部门',
    old_user_group  int COMMENT '旧组'
)
    COMMENT 'tdlive变更记录'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/olap/db/dwd_place_order_change_sale_records';


-- 解析基础字段
CREATE TEMPORARY TABLE place_order_raw AS
SELECT GET_JSON_OBJECT(detail, '$.id')       AS kafka_offset
     , GET_JSON_OBJECT(detail, '$.database') AS change_datebase
     , GET_JSON_OBJECT(detail, '$.table')    AS table_name
     , GET_JSON_OBJECT(detail, '$.type')     AS table_type
     , GET_JSON_OBJECT(detail, '$.data')     AS data
     , GET_JSON_OBJECT(detail, '$.old')      AS old
FROM dw.ods_topic_db_tdlive
WHERE dt = '${datebuf}';


-- 筛选place_order表中sales_id变更段记录
CREATE TEMPORARY TABLE place_order AS
SELECT kafka_offset, change_datebase, table_name, table_type, data, old
FROM place_order_raw
WHERE table_name = 'place_order'
  AND (table_type = 'INSERT'
    OR (table_type = 'UPDATE' AND old LIKE '%sales_id%'));


-- 解析data/old字段中段json数组
INSERT OVERWRITE TABLE dw.dwd_place_order_change_sale_records PARTITION (dt = '${datebuf}')
SELECT kafka_offset
     , change_datebase
     , table_name
     , table_type
     , GET_JSON_OBJECT(json_str_data, '$.updated_at') AS operate_time
     , GET_JSON_OBJECT(json_str_data, '$.id')         AS id
     , GET_JSON_OBJECT(json_str_data, '$.member_id')  AS member_id
     , GET_JSON_OBJECT(json_str_data, '$.special_id') AS special_id
     , GET_JSON_OBJECT(json_str_data, '$.sales_id')   AS new_sales_id
     , GET_JSON_OBJECT(json_str_data, '$.department') AS new_department
     , GET_JSON_OBJECT(json_str_data, '$.user_group') AS new_user_group
     , GET_JSON_OBJECT(json_str_old, '$.sales_id')    AS old_sales_id
     , GET_JSON_OBJECT(json_str_old, '$.department')  AS old_department
     , GET_JSON_OBJECT(json_str_old, '$.user_group')  AS old_user_group
FROM (SELECT kafka_offset
           , change_datebase
           , table_name
           , table_type
           , REGEXP_REPLACE(REGEXP_REPLACE(data, '\\[|\\]', ''), '\\}\\,\\{', '\\}\\|\\{') AS data
           , REGEXP_REPLACE(REGEXP_REPLACE(old, '\\[|\\]', ''), '\\}\\,\\{', '\\}\\|\\{')  AS old
      FROM place_order) tt
         LATERAL VIEW OUTER EXPLODE(SPLIT(data, '\\|')) data1 AS json_str_data
         LATERAL VIEW OUTER EXPLODE(SPLIT(old, '\\|')) data2 AS json_str_old;