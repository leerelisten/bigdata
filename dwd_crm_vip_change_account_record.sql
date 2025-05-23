-- 同周意沟通，该表to_user_id字段和to_xiaoe_user_nickname为客户填写，不一定有
-- relate_member_id不为0说明班主任认领成功，否则未认领算作还未换号完成。
-- 换号场景：同一个学员的课程，vip_member和xiaoe_member有两条记录，分别为换号前和换号后。

CREATE TABLE IF NOT EXISTS dwd.dwd_crm_vip_change_account_record
(
    id                     bigint COMMENT '自增Id',
    xe_app_id              string COMMENT '业务ID',
    vip_member_id          bigint COMMENT 'vip_member_id',
    member_id              bigint COMMENT 'member_id',
    special_id             string COMMENT '课程ID',
    from_xiaoe_user_id     string COMMENT '换号小鹅ID',
    to_xiaoe_user_id       string COMMENT '目标小鹅ID',
    to_xiaoe_user_nickname string COMMENT '',
    relate_member_id       bigint COMMENT '关联的学员ID-新学员',
    reason                 string COMMENT '换号原因',
    qw_sp_no               string COMMENT '',
    qw_sp_status           bigint COMMENT '审批状态:1-审批中；2-已通过；3-已驳回；4-已撤销；6-通过后撤销；7-已删除',
    qw_detail              string COMMENT '审批详情',
    add_sales_id           bigint COMMENT '发起人ID',
    add_sales_name         string COMMENT '添加人姓名',
    created_at             string COMMENT '创建时间',
    updated_at             string COMMENT '更新时间'
)
    COMMENT '换号记录表dwd层'
    PARTITIONED BY (
        dt string COMMENT '分区日期')
    STORED AS orcfile;
;



INSERT OVERWRITE TABLE dwd.dwd_crm_vip_change_account_record PARTITION (dt = '${datebuf}')
SELECT id
     , IF(xe_app_id = '', NULL, xe_app_id)                             AS xe_app_id
     , xiaoe_vip_member_id                                             AS vip_member_id
     , xiaoe_member_id                                                 AS member_id
     , IF(special_id = '', NULL, special_id)                           AS special_id
     , IF(from_xiaoe_user_id = '', NULL, from_xiaoe_user_id)           AS from_xiaoe_user_id
     , IF(to_xiaoe_user_id REGEXP '^[A-Za-z]', to_xiaoe_user_id, NULL) AS to_xiaoe_user_id -- 过滤测试数据，瞎填的数据
     , IF(to_xiaoe_user_nickname = '', NULL, to_xiaoe_user_nickname)   AS to_xiaoe_user_nickname
     , IF(relate_member_id = 0, NULL, relate_member_id)                AS relate_member_id
     , IF(reason = '', NULL, reason)                                   AS reason
     , IF(qw_sp_no = '', NULL, qw_sp_no)                               AS qw_sp_no
     , qw_sp_status
     , IF(qw_detail = '', NULL, qw_detail)                             AS qw_detail
     , add_sales_id
     , IF(add_sales_name = '', NULL, add_sales_name)                   AS add_sales_name
     , created_at
     , updated_at
FROM ods.ods_crm_vip_change_account_record
WHERE xiaoe_member_id NOT IN ( -- 剔除测试账号
                              '2464780',
                              '1948363',
                              '2332518',
                              '538603',
                              '1738455',
                              '1765316',
                              '1998164',
                              '2363006'
    )
  AND relate_member_id <> 0 --大于0 为换号成功，只取换号成功
  AND reason NOT LIKE '%测试%';
