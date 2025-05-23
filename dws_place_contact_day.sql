CREATE TABLE IF NOT EXISTS dws.dws_place_contact_day
(
    `id`                 int COMMENT 'ID自增',
    `userid`             string COMMENT '内部客服帐号',
    `ex_userid`          string COMMENT '外部联系人帐号',
    `ex_unionid`         string COMMENT '外部联系人unionid',
    `ex_nickname`        string COMMENT '外部联系人昵称',
    `created_at`         string COMMENT '创建时间',
    `updated_at`         string COMMENT '更新时间',
    `delete_at`          string COMMENT '删除时间',
    `remark`             string COMMENT '备注',
    `tags`               string COMMENT '标签',
    `wx_relation_status` int COMMENT '1未添加微信,2已添加微信,3单向好友',
    `avatar`             string COMMENT '企微头像',
    `corpid`             string COMMENT '企业id',
    `add_way`            smallint COMMENT '添加客户的来源'
)
    COMMENT '直播舞蹈大单课训练营企微联系人'
    PARTITIONED BY (
        `dt` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '',
        'serialization.format' = '');


ALTER TABLE dws.dws_place_contact_day
    DROP IF EXISTS PARTITION (dt = '${datebuf}');
ALTER TABLE dws.dws_place_contact_day
    ADD IF NOT EXISTS PARTITION (dt = '${datebuf}');

INSERT INTO dws.dws_place_contact_day PARTITION (dt = '${datebuf}')
SELECT id
     , userid
     , ex_userid
     , ex_unionid
     , ex_nickname
     , created_at
     , updated_at
     , delete_at
     , remark
     , tags
     , wx_relation_status
     , avatar
     , corpid
     , add_way
FROM ods.ods_place_contact;