CREATE TABLE IF NOT EXISTS dwd.dwd_high_lv_goods_period_end_time_dt
(
    id                bigint,
    record_id         string COMMENT '表格记录id',
    goods_name        string COMMENT '期次',
    goods_period      string COMMENT '期次-简化',
    goods_period_date string COMMENT '期次日期',
    sale_end_date     string COMMENT '销转完成日期',
    lv_type           string COMMENT '转化阶段',
    create_time       timestamp COMMENT '创建时间',
    creator_name      string COMMENT '创建人',
    update_time       timestamp COMMENT '更新时间',
    updater_name      string COMMENT '更新人'
)
    COMMENT 'DWD层高阶段课程销转结束日期维表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC;


ALTER TABLE dwd.dwd_high_lv_goods_period_end_time_dt
    DROP IF EXISTS PARTITION (dt = '${datebuf}');
ALTER TABLE dwd.dwd_high_lv_goods_period_end_time_dt
    ADD IF NOT EXISTS PARTITION (dt = '${datebuf}');

-- 取最新的权限记录
INSERT INTO dwd.dwd_high_lv_goods_period_end_time_dt PARTITION (dt = '${datebuf}')
SELECT id
     , record_id
     , goods_name
     , IF(goods_period = '', NULL, goods_period)           AS goods_period
     , IF(goods_period_date = '', NULL, goods_period_date) AS goods_period_date
     , IF(sale_end_date = '', NULL, sale_end_date)         AS sale_end_date
     , lv_type
     , create_time
     , creator_name
     , update_time
     , updater_name
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY goods_name ORDER BY id DESC) AS rn
      FROM ods.ods_high_lv_goods_period_end_time
      WHERE dt = '${datebuf}') a
WHERE a.rn = 1;