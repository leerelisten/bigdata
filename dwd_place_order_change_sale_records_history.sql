-- 从ES抽取的历史数据变更记录，最新数据在中
-- 由于是历史数据，故只跑一次，不上线


SHOW CREATE TABLE dw.dwd_place_order_change_sale_records_history;

DROP TABLE dw.dwd_place_order_change_sale_records_history;

CREATE TEMPORARY TABLE json_parse AS
SELECT GET_JSON_OBJECT(detail, '$.table_name') AS table_name
     , GET_JSON_OBJECT(detail, '$.table_type') AS table_type
     , GET_JSON_OBJECT(detail, '$.updated_at') AS updated_at
     , GET_JSON_OBJECT(detail, '$.old_fields') AS old_fields
     , GET_JSON_OBJECT(detail, '$.id')         AS id
     , GET_JSON_OBJECT(detail, '$.member_id')  AS member_id
     , GET_JSON_OBJECT(detail, '$.special_id') AS special_id
     , GET_JSON_OBJECT(detail, '$.sales_id')   AS sales_id
     , GET_JSON_OBJECT(detail, '$.department') AS department
     , GET_JSON_OBJECT(detail, '$.user_group') AS user_group
FROM dw.ods_es_db_tdlive_history;


-- INSERT OVERWRITE TABLE dw.dwd_place_order_change_sale_records_history
CREATE TABLE dw.dwd_place_order_change_sale_records_history AS
SELECT table_name
     , CASE table_type
           WHEN 'insert' THEN 'INSERT'
           WHEN 'update' THEN 'UPDATE'
    END AS table_type
     , updated_at
     , old_fields
     , id
     , member_id
     , special_id
     , sales_id
     , department
     , user_group
FROM json_parse
WHERE table_name = 'place_order'
  AND (table_type = 'insert'
    OR (table_type = 'update' AND
        old_fields LIKE '%sales_id%'));


-- 防止有重复数据
INSERT OVERWRITE TABLE dw.dwd_place_order_change_sale_records_history
SELECT DISTINCT *
FROM dw.dwd_place_order_change_sale_records_history;