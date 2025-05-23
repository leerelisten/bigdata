CREATE TABLE IF NOT EXISTS dwd.dwd_high_lv_wx_refund_info_dt
(
    id                 bigint,
    record_id          string COMMENT '表格记录id',
    goods_name         string COMMENT '期次',
    user_id            string COMMENT '用户ID（小鹅通ID）',
    refund_commit_time string COMMENT '退款提交时间',
    refund_status      string COMMENT '退款状态',
    lv_type            string COMMENT '转化阶段',
    create_time        timestamp COMMENT '创建时间',
    creator_name       string COMMENT '创建人',
    update_time        timestamp COMMENT '更新时间',
    updater_name       string COMMENT '更新人'
)
    COMMENT 'DWD层高阶段课程企微退款导出数据'
    PARTITIONED BY (dt STRING)
    STORED AS ORC;


ALTER TABLE dwd.dwd_high_lv_wx_refund_info_dt
    DROP IF EXISTS PARTITION (dt = '${datebuf}');
ALTER TABLE dwd.dwd_high_lv_wx_refund_info_dt
    ADD IF NOT EXISTS PARTITION (dt = '${datebuf}');

-- 取最新的权限记录
INSERT INTO dwd.dwd_high_lv_wx_refund_info_dt PARTITION (dt = '${datebuf}')
SELECT id
     , record_id
     , goods_name
     , user_id
     , refund_commit_time
     , refund_status
     , lv_type
     , create_time
     , creator_name
     , update_time
     , updater_name
FROM (SELECT id
           , record_id
           , goods_name
           , IF(user_id = '', NULL, user_id)                                                                       AS user_id
           , IF(refund_commit_time = '', NULL, refund_commit_time)                                                 AS refund_commit_time
           , IF(refund_status = '', NULL, refund_status)                                                           AS refund_status
           , lv_type
           , create_time
           , creator_name
           , update_time
           , updater_name
           , ROW_NUMBER() OVER (PARTITION BY goods_name,user_id,refund_commit_time,refund_status ORDER BY id DESC) AS rn
      FROM ods.ods_high_lv_wx_refund_info
      WHERE dt = '${datebuf}') a
WHERE rn = 1;