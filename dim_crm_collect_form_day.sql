CREATE TABLE IF NOT EXISTS dim.dim_crm_collect_form_day
(
    `id`            bigint,
    `form_id`       varchar(64) COMMENT '表单设置ID',
    `content`       string COMMENT '选项内容',
    `type`          varchar(20) COMMENT '表单类型（1-图片/2-输入框/3-下拉菜单/4-单选/5-多选/6-日期/7-地址/8-手机号/9-操作)',
    `label`         string COMMENT '标签配置（json）',
    `is_must_write` int COMMENT '是否必须填写（1-必须/2-不必须）',
    `content_toast` varchar(1000) COMMENT '提示文字',
    `content_label` varchar(500) COMMENT '标题名称',
    `content_desc`  varchar(500) COMMENT '标题描述',
    `is_syn_user`   int COMMENT '是否同步到用户详情（1-同步/2-不同步）',
    `user_field`    varchar(500) COMMENT '同步到用户详情的字段名称',
    `sort`          int COMMENT '排序字段',
    `mult_choose`   int COMMENT '是否支持多选（1-支持/2-不支持）',
    `field`         varchar(100) COMMENT 'form表单字段名',
    `deleted_at`    bigint COMMENT '删除标志',
    `updated_at`    bigint
)
    COMMENT '问卷表单设置表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS orcfile;


TRUNCATE TABLE dim.dim_crm_collect_form_day;

INSERT INTO dim.dim_crm_collect_form_day
SELECT id
     , form_id
     , content
     , type
     , label
     , is_must_write
     , IF(content_toast = '', NULL, content_toast)         AS content_toast
     , content_label
     , IF(content_desc = '', NULL, content_desc)           AS content_desc
     , is_syn_user
     , IF(user_field = '', NULL, user_field)               AS user_field
     , sort
     , mult_choose
     , field
     , IF(deleted_at = 0, NULL, FROM_UNIXTIME(deleted_at)) AS deleted_at
     , FROM_UNIXTIME(updated_at)                           AS updated_at
FROM ods.ods_crm_collect_form_day