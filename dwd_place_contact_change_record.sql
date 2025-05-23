

--表使用方法：2025-01-20 为初始化全量数据 之后为每天增量
INSERT OVERWRITE TABLE dwd.dwd_place_contact_change_record PARTITION (dt = '${datebuf}')
SELECT GET_JSON_OBJECT(data, '$[0].id')                 AS id
     , `table`                                          AS table_name
     , type                                             AS alter_type
     , GET_JSON_OBJECT(data, '$[0].userid')             AS userid             --内部客服账号
     , GET_JSON_OBJECT(data, '$[0].ex_userid')          AS ex_userid          --外部联系人账号
     , GET_JSON_OBJECT(data, '$[0].ex_unionid')         AS ex_unionid         --外部联系人unionid
     , GET_JSON_OBJECT(data, '$[0].ex_nickname')        AS ex_nickname        --外部联系人昵称
     , GET_JSON_OBJECT(data, '$[0].created_at')         AS created_at         --创建时间
     , GET_JSON_OBJECT(data, '$[0].updated_at')         AS updated_at         --更新时间
     , GET_JSON_OBJECT(data, '$[0].delete_at')          AS delete_at          --删除时间
     , GET_JSON_OBJECT(data, '$[0].remark')             AS remark             --备注
     , GET_JSON_OBJECT(data, '$[0].tags')               AS tags               --标签
     , GET_JSON_OBJECT(data, '$[0].wx_relation_status') AS wx_relation_status --1未添加微信,2已添加微信,3单向好友',
     , GET_JSON_OBJECT(data, '$[0].avatar')             AS avatar             --企微头像',
     , GET_JSON_OBJECT(data, '$[0].corpid')             AS corpid             --企业id',
     , GET_JSON_OBJECT(data, '$[0].add_way')            AS add_way            -- 添加客户来源
FROM ods.ods_tdlive_change_log
WHERE dt = '${datebuf}'
  AND `table` = 'place_contact'
  AND (type = 'INSERT' OR type = 'UPDATE')
  AND isDdl = 'false'
;
