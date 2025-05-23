CREATE TABLE IF NOT EXISTS dwd.dwd_admin_users
(
    `id`             BIGINT,
    `name`           STRING COMMENT '修复后的姓名，删除后缀',
    `row_name`       string COMMENT '原始的姓名，删除账号带已删后缀',
    `email`          STRING COMMENT '修复后的邮箱，删除后缀',
    `row_email`      string COMMENT '原始的邮箱，删除邮箱带_n后缀',
    `password`       STRING,
    `remember_token` STRING,
    `created_at`     STRING,
    `updated_at`     STRING,
    `mobile`         STRING COMMENT '手机号',
    `department_id`  INT COMMENT '所属部门ID',
    `avatar`         STRING COMMENT '头像',
    `is_delete`      INT COMMENT '是否已失效'
)
    COMMENT '管理员用户表'
    STORED AS ORC;



INSERT OVERWRITE TABLE dwd.dwd_admin_users
SELECT id
     , REGEXP_REPLACE(REPLACE(name, '(已删)', ''), '\u00A0', '') AS name
     , name                                                      AS row_name
     , REGEXP_REPLACE(email, '_n*$', '')                         AS email
     , email
     , IF(password = '', NULL, password)                         AS password
     , IF(remember_token = '', NULL, remember_token)             AS remember_token
     , created_at
     , updated_at
     , IF(mobile = '', NULL, mobile)                             AS mobile
     , department_id
     , IF(avatar = '', NULL, avatar)                             AS avatar
     -- 如果邮箱不以com结尾，则认为是测试账号，剔除
     , IF(email NOT RLIKE 'com$', 1, 0)                          AS is_delete
FROM ods.ods_admin_users;