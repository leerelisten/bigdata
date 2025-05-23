CREATE TABLE IF NOT EXISTS dwd.dwd_crm_ai_notifications
(
    special_id string COMMENT '期次ID',
    member_id  bigint COMMENT 'member_id'
)
    COMMENT 'dwd层-AI销售追单通知消息表'
    STORED AS orcfile;


-- 由于一个例子可能多次提醒，只取一条
INSERT OVERWRITE TABLE dwd.dwd_crm_ai_notifications

SELECT special_id, member_id
FROM ods.ods_crm_ai_notifications
WHERE notify_type = 'pending_order'
  AND deleted_at = 0
GROUP BY special_id, member_id