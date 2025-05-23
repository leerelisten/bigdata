CREATE TABLE IF NOT EXISTS dw.dws_place_order_change_sale_records
(
    table_name   string COMMENT '变更表名',
    table_type   string COMMENT '变更类型',
    updated_time string COMMENT '更新时间',
    id           bigint COMMENT 'mysql表的主键id字段',
    member_id    bigint COMMENT 'member_id',
    special_id   string COMMENT '期次ID',
    sales_id     INT COMMENT '新sales_id',
    department   int COMMENT '新部门',
    user_group   int COMMENT '新组'
)
    COMMENT 'tdlive变更记录'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/olap/db/dws_place_order_change_sale_records';


INSERT OVERWRITE TABLE dw.dws_place_order_change_sale_records
SELECT DISTINCT table_name
              , table_type
              , updated_at
              , id
              , member_id
              , special_id
              , IF(sales_id = 0, NULL, sales_id)     AS sales_id
              , IF(department = 0, NULL, department) AS department
              , IF(user_group = 0, NULL, user_group) AS user_group
FROM (SELECT table_name
           , CASE table_type
                 WHEN 'insert' THEN 'INSERT'
                 WHEN 'update' THEN 'UPDATE'
        END AS table_type
           , updated_at
           , id
           , member_id
           , special_id
           , sales_id
           , department
           , user_group
      FROM dw.dwd_place_order_change_sale_records_history
      UNION
      SELECT table_name
           , table_type
           , operate_time   AS updated_at
           , id
           , member_id
           , special_id
           , new_sales_id   AS sales_id
           , new_department AS department
           , new_user_group AS user_group
      FROM dw.dwd_place_order_change_sale_records) a;