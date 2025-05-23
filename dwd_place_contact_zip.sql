-- 拉链表
CREATE TABLE IF NOT EXISTS dwd.dwd_place_contact_zip
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
    `add_way`            smallint COMMENT '添加客户的来源',
    start_time           timestamp COMMENT '生效时间',
    end_time             timestamp COMMENT '失效时间'
)
    COMMENT '企微联系人拉链表'
    STORED AS orcfile;


CREATE TEMPORARY TABLE aaa AS
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
-- 开窗取获取开始时间和结束时间
--      , updated_at                                                               AS start_time
--      , LAG(updated_at, 1, NULL) OVER (PARTITION BY id ORDER BY updated_at DESC) AS end_time
FROM dwd.dwd_place_contact_change_record
WHERE id IS NOT NULL
-- 使用union进行去重
UNION
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
FROM ods.ods_place_contact
;



TRUNCATE TABLE dwd.dwd_place_contact_zip;

INSERT INTO dwd.dwd_place_contact_zip
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
     , IF(rn = 1, created_at, updated_at) AS start_time
     , end_time
FROM (SELECT id
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
           , LAG(updated_at, 1, NULL) OVER (PARTITION BY id ORDER BY updated_at DESC) AS end_time
           , RANK() OVER (PARTITION BY id ORDER BY updated_at ASC)                    AS rn
      FROM aaa) a