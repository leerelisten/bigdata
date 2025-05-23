
--表使用方法：2025-01-20 为初始化全量数据 之后为每天增量
INSERT  OVERWRITE TABLE dwd.dim_place_costname_change_record PARTITION (dt = '${datebuf}')
SELECT `table`                                           AS table_name
     , type                                              AS alter_type
     , GET_JSON_OBJECT(data, '$[0].id')                  AS id
     , GET_JSON_OBJECT(data, '$[0].name_type')           AS name_type
     , GET_JSON_OBJECT(data, '$[0].main_account')        AS main_account
     , GET_JSON_OBJECT(data, '$[0].open_agent')          AS open_agent
     , GET_JSON_OBJECT(data, '$[0].operation_agent')     AS operation_agent
     , GET_JSON_OBJECT(data, '$[0].material_agent')      AS material_agent
     , GET_JSON_OBJECT(data, '$[0].ad_method')           AS ad_method
     , GET_JSON_OBJECT(data, '$[0].d_cat')               AS d_cat
     , GET_JSON_OBJECT(data, '$[0].d_platform')          AS d_platform
     , GET_JSON_OBJECT(data, '$[0].d_pos')               AS d_pos
     , GET_JSON_OBJECT(data, '$[0].age')                 AS age
     , GET_JSON_OBJECT(data, '$[0].price')               AS price
     , GET_JSON_OBJECT(data, '$[0].collect_phone_type')  AS collect_phone_type
     , GET_JSON_OBJECT(data, '$[0].d_linktype')          AS d_linktype
     , GET_JSON_OBJECT(data, '$[0].sort_number')         AS sort_number
     , GET_JSON_OBJECT(data, '$[0].full_version_name')   AS full_version_name
     , GET_JSON_OBJECT(data, '$[0].simple_version_name') AS simple_version_name
     , GET_JSON_OBJECT(data, '$[0].cost_id')             AS cost_id
     , GET_JSON_OBJECT(data, '$[0].old_name')            AS old_name
     , GET_JSON_OBJECT(data, '$[0].operator_man')        AS operator_man
     , GET_JSON_OBJECT(data, '$[0].created_at')          AS created_at
     , GET_JSON_OBJECT(data, '$[0].updated_at')          AS updated_at
     , GET_JSON_OBJECT(data, '$[0].area')                AS area
FROM ods.ods_tdlive_change_log
WHERE dt = '${datebuf}'
  AND `table` = 'dim_place_costname'
  AND (type = 'INSERT' OR type = 'UPDATE')
  AND isDdl = 'false'
;