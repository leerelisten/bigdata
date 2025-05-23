CREATE TABLE IF NOT EXISTS dw.topic_db_tdlive
(
    detail string COMMENT '变更记录'
)
    COMMENT 'tdlive变更记录'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/olap/db/topic_db_tdlive';

-- hadoop fs -cp /user/wuhanlexue/flume/topic_db_tdlive/${datebuf} /user/wuhanlexue/flume/topic_db_tdlive_bak/${datebuf};


LOAD DATA INPATH '/user/wuhanlexue/flume/topic_db_tdlive_bak/${datebuf}/*' OVERWRITE INTO TABLE dw.topic_db_tdlive PARTITION (dt = '${datebuf}');



CREATE TABLE dw.tdlive_sale_change_tab
AS
SELECT GET_JSON_OBJECT(detail, '$.database')                                 AS change_datebase
     , GET_JSON_OBJECT(detail, '$.table')                                    AS change_table
     , GET_JSON_OBJECT(detail, '$.data[0].id')                               AS primary_key
     , GET_JSON_OBJECT(detail, '$.data[0].sales_id')                         AS new_sales_id
     , GET_JSON_OBJECT(detail, '$.data[0].department')                       AS new_department
     , GET_JSON_OBJECT(detail, '$.data[0].user_group')                       AS new_user_group
     , GET_JSON_OBJECT(detail, '$.old[0].sales_id')                          AS old_sales_id
     , GET_JSON_OBJECT(detail, '$.old[0].department')                        AS old_department
     , GET_JSON_OBJECT(detail, '$.old[0].user_group')                        AS old_user_group
     , GET_JSON_OBJECT(detail, '$.type')                                     AS operate_type
     , FROM_UNIXTIME(CAST(GET_JSON_OBJECT(detail, '$.ts') / 1000 AS bigint)) AS operate_time
FROM dw.topic_db_tdlive
WHERE GET_JSON_OBJECT(detail, '$.table') = 'place_order'
  AND GET_JSON_OBJECT(detail, '$.old') LIKE '%sales_id%';

