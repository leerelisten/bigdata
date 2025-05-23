CREATE TABLE IF NOT EXISTS dwd.dwd_crm_vip_apply_delay
(
    `id`                   bigint,
    `member_id`            bigint COMMENT 'xiaoe_member.id',
    `apply_type`           int COMMENT '申请类型 1 课前申请 2 课中申请',
    `begin_class_date`     string COMMENT '预计恢复课程时间 YYYYmm',
    `create_uid`           bigint COMMENT '创建者 0 表示用户自己，其他数值表示后台用户',
    `reason`               string COMMENT '延期原因',
    `remark`               string COMMENT '备注',
    `jiaowu`               int COMMENT '教务处理 0 待处理 1已处理',
    `order_id`             bigint COMMENT '订单Id',
    `last_class`           string COMMENT '上一次延期期次 对应的special_id',
    `last_class_sale_id`   bigint COMMENT '上一次延期班主任',
    `curr_class`           string COMMENT '申请时期次 对应的special_id',
    `curr_phase`           string COMMENT '申请时阶段 对应阶段special_id',
    `curr_class_sale_id`   bigint COMMENT '当前班主任',
    `apply_begin_class_at` timestamp COMMENT '申请开课时间',
    `new_class`            string COMMENT '开课期次 对应special_id',
    `new_phase`            string COMMENT '开课期次阶段 对应阶段special_id',
    `new_class_sale_id`    bigint COMMENT '开课分配的班主任',
    `created_at`           timestamp COMMENT '创建时间',
    `updated_at`           timestamp COMMENT '更新时间',
    `begin_class_state`    int COMMENT '开课处理状态 1 待处理 2 已开课',
    `vip_member_id`        bigint COMMENT 'xiaoe_vip_member.id',
    `qw_sp_no`             string COMMENT '企微审批编号',
    `qw_sp_status`         int COMMENT '企微审批状态：1-审批中；2-已通过；3-已驳回；4-已撤销；6-通过后撤销；7-已删除',
    `remark_img_url`       string COMMENT '备注图片'
)
    COMMENT 'DWD层延期学员表'
    STORED AS ORC;

desc dwd.dwd_crm_vip_apply_delay;
CREATE TEMPORARY TABLE aaa AS
SELECT id
     , member_id
     , apply_type
     , IF(begin_class_date = '', NULL, begin_class_date) AS begin_class_date
     , create_uid
     , reason
     , IF(remark = '', NULL, remark)                     AS remark
     , jiaowu
     , order_id
     , IF(curr_class = '', NULL, curr_class)             AS curr_class
     , IF(curr_phase = '', NULL, curr_phase)             AS curr_phase
     , curr_class_sale_id
     , apply_begin_class_at
     , IF(new_class = '', NULL, new_class)               AS new_class
     , IF(new_phase = '', NULL, new_phase)               AS new_phase
     , new_class_sale_id
     , created_at
     , updated_at
     , begin_class_state
     , vip_member_id
     , qw_sp_no
     , qw_sp_status
     , IF(remark_img_url = '', NULL, remark_img_url)     AS remark_img_url
FROM ods.ods_crm_vip_apply_delay;

-- 由于一些异常情况，同一个其次同一个学员延期可能会提交两次，如果同一个期次提交了两次延期，则保留最新的一条。
-- 开窗获取每个期次最新的一次延期
CREATE TEMPORARY TABLE result AS
SELECT id
     , member_id
     , apply_type
     , begin_class_date
     , create_uid
     , reason
     , remark
     , jiaowu
     , order_id
     , curr_class
     , curr_phase
     , curr_class_sale_id
     , apply_begin_class_at
     , new_class
     , new_phase
     , new_class_sale_id
     , created_at
     , updated_at
     , begin_class_state
     , vip_member_id
     , qw_sp_no
     , qw_sp_status
     , remark_img_url
FROM (SELECT id
           , member_id
           , apply_type
           , begin_class_date
           , create_uid
           , reason
           , remark
           , jiaowu
           , order_id
           , curr_class
           , curr_phase
           , curr_class_sale_id
           , apply_begin_class_at
           , new_class
           , new_phase
           , new_class_sale_id
           , created_at
           , updated_at
           , begin_class_state
           , vip_member_id
           , qw_sp_no
           , qw_sp_status
           , remark_img_url
           , ROW_NUMBER() OVER (PARTITION BY member_id,curr_class ORDER BY created_at DESC) AS rank
      FROM aaa) a
WHERE rank = 1;


TRUNCATE TABLE dwd.dwd_crm_vip_apply_delay;
INSERT INTO dwd.dwd_crm_vip_apply_delay
SELECT id
     , member_id
     , apply_type
     , begin_class_date
     , create_uid
     , reason
     , remark
     , jiaowu
     , order_id
     , LAG(curr_class, 1, NULL) OVER (PARTITION BY vip_member_id ORDER BY id)         AS last_class
     , LAG(curr_class_sale_id, 1, NULL) OVER (PARTITION BY vip_member_id ORDER BY id) AS last_class_sale_id
     , curr_class
     , curr_phase
     , curr_class_sale_id
     , apply_begin_class_at
     , new_class
     , new_phase
     , new_class_sale_id
     , created_at
     , updated_at
     , begin_class_state
     , vip_member_id
     , qw_sp_no
     , qw_sp_status
     , remark_img_url
FROM result;