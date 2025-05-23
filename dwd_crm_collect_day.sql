CREATE TABLE IF NOT EXISTS dwd.dwd_crm_collect_day
(
    `id`         bigint,
    `form_id`    varchar(64) COMMENT '问卷ID',
    `form_name`  varchar(500) COMMENT '问卷名称',
    `status`     int COMMENT '状态（1-未开启/2-已开启）',
    `business`   varchar(64) COMMENT '业务线',
    `created_at` varchar(19) COMMENT '创建时间',
    `updated_at` varchar(19) COMMENT '更新时间',
    `deleted_at` bigint COMMENT '删除时间',
    `creator_id` bigint COMMENT '创建人',
    `updator_id` bigint COMMENT '更新人',
    `type`       int COMMENT '问卷类型1-普通问卷/2-效果问卷/3-用户协议）',
    `eff_url`    string COMMENT '效果问卷h5地址/用户协议关联产品ID',
    `category`   varchar(16) COMMENT '品类  1:古典舞',
    `goods_type` varchar(16) COMMENT '商品类型',
    `can_update` int COMMENT '是否支持修改（1-支持/2-不支持）',
    `is_login`   int COMMENT '是否需要登录1-需要/2-不需要）'
)
    COMMENT 'CRM问卷设置表'
    PARTITIONED BY (
        `dt` varchar(10) COMMENT '分区日期')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS orcfile;


ALTER TABLE dwd.dwd_crm_collect_day
    DROP IF EXISTS PARTITION (dt = '${datebuf}');
ALTER TABLE dwd.dwd_crm_collect_day
    ADD IF NOT EXISTS PARTITION (dt = '${datebuf}');

INSERT OVERWRITE TABLE dwd.dwd_crm_collect_day PARTITION (dt = '${datebuf}')
SELECT id
     , form_id
     , form_name
     , status
     , business
     , created_at
     , updated_at
     , IF(deleted_at = 0, NULL, FROM_UNIXTIME(deleted_at)) AS deleted_at
     , creator_id
     , updator_id
     , type
     , IF(eff_url = '', NULL, eff_url)                     AS eff_url
     , IF(category = '', NULL, category)                   AS category
     , IF(goods_type = '', NULL, goods_type)               AS goods_type
     , can_update
     , is_login
FROM ods.ods_crm_collect_day
WHERE dt = '${datebuf}'