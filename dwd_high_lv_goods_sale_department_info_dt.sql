CREATE TABLE IF NOT EXISTS dwd.dwd_high_lv_goods_sale_department_info_dt
(
    id                bigint,
    record_id         string COMMENT '表格记录id',
    goods_name        string COMMENT '期次',
    goods_period      string COMMENT '期次-简化',
    class_sale_name   string COMMENT '班主任',
    department_leader string COMMENT '部门主管',
    department        string COMMENT '部门名称',
    goods_price       int COMMENT '期次价格',
    goods_dates       string COMMENT '期次持续时间',
    lv_type           string COMMENT '转化阶段',

    create_time       timestamp COMMENT '创建时间',
    creator_name      string COMMENT '创建人',
    update_time       timestamp COMMENT '更新时间',
    updater_name      string COMMENT '更新人'
)
    COMMENT 'DWD层高阶段班主任所属部门维表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC;

ALTER TABLE dwd.dwd_high_lv_goods_sale_department_info_dt
    DROP IF EXISTS PARTITION (dt = '${datebuf}');
ALTER TABLE dwd.dwd_high_lv_goods_sale_department_info_dt
    ADD IF NOT EXISTS PARTITION (dt = '${datebuf}');

-- 取最新的权限记录
INSERT INTO dwd.dwd_high_lv_goods_sale_department_info_dt PARTITION (dt = '${datebuf}')
SELECT id
     , record_id
     , goods_name
     , IF(goods_period = '', NULL, goods_period)           AS goods_period
     , IF(class_sale_name = '', NULL, class_sale_name)     AS class_sale_name
     , IF(department_leader = '', NULL, department_leader) AS department_leader
     , IF(department = '', NULL, department)               AS department
     , IF(goods_price = '', NULL, goods_price)             AS goods_price
     , IF(goods_dates = '', NULL, goods_dates)             AS goods_continue_dates
     , lv_type
     , create_time
     , creator_name
     , update_time
     , updater_name
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY goods_name,class_sale_name ORDER BY id DESC) AS rn
      FROM ods.ods_high_lv_goods_sale_department_info
      WHERE dt = '${datebuf}') a
WHERE a.rn = 1;
