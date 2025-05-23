SET mapred.job.name="c_app_course_period_baidusucai_dashboard#${datebuf}";
USE app;
CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_course_period_baidusucai_dashboard
(
    d_date              string COMMENT '日期',
    period              string COMMENT '期次',
    cat                 string COMMENT '品类',
    ad_department       string COMMENT '投放部门',
    platform_name       string COMMENT '渠道',
    pos                 string COMMENT '版位',
    price               string COMMENT '价格',
    link_type_v2        string COMMENT '链路类型(新)',
    mobile              string COMMENT '收集手机号',
    agent               string COMMENT '代理',
    cost_id             string COMMENT '账户id',
    ad_id               string COMMENT '计划id',
    sucai_id            string COMMENT '素材id',
    sucai_name          string COMMENT '素材名称',
    cost                float COMMENT '账面消耗(元)',
    cost_real           float COMMENT '实际消耗(元)',
    submit_num          int COMMENT '表单填写例子数(个)',
    payment_num         int COMMENT '支付成功例子数(个)',
    user_num            int COMMENT '例子数(个)',
    wx_num              int COMMENT '加微例子数(个)',
    collect_num         int COMMENT '填问卷例子数(个)',
    pay_num_D4          float COMMENT 'D4正价课订单数(单)',
    pay_sum_D4          float COMMENT 'D4正价课GMV(元)',
    roi_D4              float COMMENT 'D4ROI',
    pay_user_num        int COMMENT '购买正价课例子数(个)',
    pay_num             float COMMENT '正价课订单数(单)',
    pay_sum             float COMMENT '正价课GMV(元)',
    roi                 float COMMENT 'ROI=正价课GMV/实际支出',
    conv_rate           float COMMENT '转化率=正价课订单数/例子数(%)',
    cac                 float COMMENT 'CAC=实际支出/例子数(元/个)',
    wx_rate             float COMMENT '加微率=加微例子数/例子数(%)',
    wx_active_rate      float COMMENT '主动加微率=主动加微例子数/例子数(%)',
    collect_rate        float COMMENT '问卷率=填问卷例子数/例子数(%)',
    collect_active_rate float COMMENT '主动问卷率=主动填问卷例子数/例子数(%)',
    ifcome0_rate        float COMMENT '导学课到课率(%)',
    ifok0_rate          float COMMENT '导学课完课率(%)',
    ifcome1_rate        float COMMENT 'D1到课率(%)',
    ifok1_rate          float COMMENT 'D1完课率(%)',
    ifcome2_rate        float COMMENT 'D2到课率(%)',
    ifok2_rate          float COMMENT 'D2完课率(%)',
    ifcome3_rate        float COMMENT 'D3到课率(%)',
    ifok3_rate          float COMMENT 'D3完课率(%)',
    ifcome4_rate        float COMMENT 'D4到课率(%)',
    ifok4_rate          float COMMENT 'D4完课率(%)',
    ifcome5_rate        float COMMENT 'D5到课率(%)',
    ifok5_rate          float COMMENT 'D5完课率(%)',
    wx_active_num       int COMMENT '主动加微例子数(个)',
    collect_active_num  int COMMENT '主动填问卷例子数(个)',
    ifcome0             int COMMENT '导学课到课例子数(个)',
    ifok0               int COMMENT '导学课完课例子数(个)',
    ifcome1             int COMMENT 'D1到课例子数(个)',
    ifok1               int COMMENT 'D1完课例子数(个)',
    ifcome2             int COMMENT 'D2到课例子数(个)',
    ifok2               int COMMENT 'D2完课例子数(个)',
    ifcome3             int COMMENT 'D3到课例子数(个)',
    ifok3               int COMMENT 'D3完课例子数(个)',
    ifcome4             int COMMENT 'D4到课例子数(个)',
    ifok4               int COMMENT 'D4完课例子数(个)',
    ifcome5             int COMMENT 'D5到课例子数(个)',
    ifok5               int COMMENT 'D5完课例子数(个)'
)
    COMMENT '培训主题数仓-百度素材期次报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_period_baidusucai_dashboard';

SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;

WITH cost AS
    (SELECT cost_id
          , agent
          , ad_id
          , sucai_id
          , sucai_name
          , d_date
          , SUM(cost)      AS cost
          , SUM(cost_real) AS cost_real
     FROM da.da_course_daily_cost_by_sucaiid
     WHERE dt = '${datebuf}'
     GROUP BY cost_id
            , agent
            , ad_id
            , sucai_id
            , sucai_name
            , d_date)
   , users AS -- 20240914 本地推直播:直播一组,腾讯视频号直播付费流:直播二组,千川直播:直播三组
    (SELECT
         a.member_id
          ,a.cat
          ,a.ad_department
          ,a.platform
          ,a.platform_name
          ,a.department
          ,a.user_group
          ,a.h5_id
          ,a.pos
          ,a.price
          ,a.mobile
          ,a.link_type_v1
          ,a.link_type_v2
          ,a.cost_id
          ,a.ad_id
          ,a.sucai_id
          ,a.created_at
          ,a.wx_rel_status
          ,a.goods_name
          ,a.xe_id
          ,a.ifcollect
          ,a.trade_state
          ,a.member_status
          ,a.sales_id
          ,a.sales_name
          ,a.ifbuy
          ,a.pay_num
          ,a.pay_sum
          ,a.pay_num_D4
          ,a.pay_sum_D4
          ,a.ifcome0
          ,a.ifok0
          ,a.ifcome1
          ,a.ifok1
          ,a.ifcome2
          ,a.ifok2
          ,a.ifcome3
          ,a.ifok3
          ,a.ifcome4
          ,a.ifok4
          ,a.ifcome5
          ,a.ifok5
          ,a.wx_active
          ,a.collect_active
          ,a.d_date
          ,a.dt
     FROM dws.dws_sale_camping_user_day a
     WHERE a.dt = '${datebuf}'
       AND TO_DATE(CONCAT('20', SUBSTR(a.goods_name, 2, 2), '-', SUBSTR(a.goods_name, 4, 2), '-',
                          SUBSTR(a.goods_name, 6, 2))) >= '2024-05-01'
       AND a.created_at >= '2024-05-01'
       --这里先注释掉 背景：20250403 发现业务方有 期次包含“测试”二字 但是数据是需要的
--        AND a.goods_name NOT LIKE '%测试%'
       AND a.pos IN ('百度搜索', '百度信息流','百度信息流北京')
    )
   -- cost_id2是为了是处理手工导入消耗，百度信息流这里不涉及，不需要
   , user_num AS
    (SELECT cost_id
          , ad_id
          , sucai_id
          , d_date
          , COUNT(*) num
     FROM users
     WHERE dt = '${datebuf}'
       AND member_status = 1
       AND trade_state IN ('SUCCESS', 'PREPARE')
       AND sales_id > 0
     GROUP BY cost_id
            , ad_id
            , sucai_id
            , d_date)
   , cac AS
    (SELECT a.cost_id
          , a.agent
          , a.ad_id
          , a.sucai_id
          , a.sucai_name
          , a.d_date
          , NVL(a.cost / b.num, 0)      AS cac
          , NVL(a.cost_real / b.num, 0) AS cac_real
     FROM cost a
              LEFT JOIN user_num b
                        ON a.cost_id = b.cost_id
                            AND a.ad_id = b.ad_id
                            AND a.sucai_id = b.sucai_id
                            AND a.d_date = b.d_date)
   , user_info AS
    ( -- 所有例子的投放属性&加微、问卷状态&购买正价课状态
        SELECT a.member_id
             , a.cat
             , if(COALESCE(a.ad_department,'无')='信息流四部','信息流一部',COALESCE(a.ad_department,'无'))  AS ad_department
             , a.platform_name
             , a.h5_id
             , a.pos
             , a.mobile
             , a.price
             , CASE WHEN a.link_type_v2 = '新一键' THEN '新一键授权' ELSE a.link_type_v2 END AS link_type_v2
             , a.created_at
             , a.wx_rel_status
             , a.goods_name
             , a.xe_id
             , a.ifcollect
             , a.trade_state
             , a.member_status
             , a.sales_id
             , a.cost_id
             , a.ad_id
             , a.sucai_id
             , a.ifbuy
             , a.pay_num
             , a.pay_sum
             , a.pay_num_D4
             , a.pay_sum_D4
             , a.ifcome0
             , a.ifok0
             , a.ifcome1
             , a.ifok1
             , a.ifcome2
             , a.ifok2
             , a.ifcome3
             , a.ifok3
             , a.ifcome4
             , a.ifok4
             , a.ifcome5
             , a.ifok5
             , a.wx_active
             , a.collect_active
             , d.cac
             , d.cac_real
             , d.agent
             , d.sucai_name
        FROM users a
                 LEFT JOIN cac d -- 消耗数据
                           ON d.cost_id = a.cost_id
                               and d.ad_id = a.ad_id
                               and d.sucai_id = a.sucai_id
                               AND d.d_date = a.d_date)
   , mid AS
    ( -- 按期次+投放属性聚合用户
        SELECT  goods_name
             , cat
             , ad_department
             , platform_name
             , price
             , link_type_v2
             , mobile
             , pos
             , agent
             , CONCAT('id_', cost_id)                                                             AS cost_id
             , CONCAT('id_', ad_id)                                                               AS ad_id
             , CONCAT('id_', sucai_id) 															  AS sucai_id
             , sucai_name
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, cac,
                      0))                                                                         AS cost
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, cac_real,
                      0))                                                                         AS cost_real
             , COUNT(member_id)                                                                   AS submit_num
             , COUNT(IF(trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, member_id, NULL)) AS payment_num
             , COUNT(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, member_id,
                        NULL))                                                                    AS user_num           -- 例子数
             , COUNT(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0 AND
                        wx_rel_status IN (2, 3, 4), member_id,
                        NULL))                                                                    AS wx_num             -- 加微uv
             , COUNT(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0 AND
                        wx_rel_status IN (2, 3, 4) AND wx_active = 1, member_id,
                        NULL))                                                                    AS wx_active_num      -- 主动加微uv
             , COUNT(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0 AND ifcollect = 1,
                        member_id,
                        NULL))                                                                    AS collect_num        -- 填问卷uv
             , COUNT(IF(
                        member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0 AND ifcollect = 1 AND
                        collect_active = 1, member_id,
                        NULL))                                                                            AS collect_active_num -- 主动问卷uv
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, ifbuy,
                      0))                                                                         AS pay_user_num       -- 购买正价课uv
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0 AND pay_num = 0.5,
                      ifbuy,
                      0))                                                                         AS pay999_user_num    -- 购买999正价课uv
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0 AND pay_num = 1,
                      ifbuy,
                      0))                                                                         AS pay1980_user_num   -- 购买1980正价课uv
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num,
                      0))                                                                         AS pay_num            -- 正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum,
                      0))                                                                         AS pay_sum            -- 正价课GMV
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D4,
                      0))                                                                         AS pay_num_D4         -- D4正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D4,
                      0))                                                                         AS pay_sum_D4         -- D4正价课GMV
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, ifcome0,
                      0))                                                                         AS ifcome0            -- 导学课到课
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, ifok0,
                      0))                                                                         AS ifok0              -- 导学课完课
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, ifcome1,
                      0))                                                                         AS ifcome1            -- D1到课
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, ifok1,
                      0))                                                                         AS ifok1              -- D1完课
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, ifcome2,
                      0))                                                                         AS ifcome2
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, ifok2,
                      0))                                                                         AS ifok2
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, ifcome3,
                      0))                                                                         AS ifcome3
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, ifok3,
                      0))                                                                         AS ifok3
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, ifcome4,
                      0))                                                                         AS ifcome4
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, ifok4,
                      0))                                                                         AS ifok4
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, ifcome5,
                      0))                                                                         AS ifcome5
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, ifok5,
                      0))                                                                         AS ifok5
        FROM user_info
        GROUP BY
            goods_name
               , cat
               , ad_department
               , platform_name
               , price
               , link_type_v2
               , mobile
               , pos
               , agent
               , CONCAT('id_', cost_id)
               , CONCAT('id_', ad_id)
               , CONCAT('id_', sucai_id)
               , sucai_name)

INSERT
OVERWRITE
TABLE
app.c_app_course_period_baidusucai_dashboard
PARTITION
(
dt = '${datebuf}'
)
SELECT '${datebuf}'                                          AS d_date
     , goods_name
     , cat
     , ad_department
     , platform_name
     , pos
     , price
     , link_type_v2
     , mobile
     , agent
     , cost_id
     , ad_id
     , sucai_id
     , sucai_name
     , cost
     , cost_real
     , submit_num
     , payment_num
     , user_num
     , wx_num
     , collect_num
     , pay_num_D4
     , pay_sum_D4
     , NVL(pay_sum_D4 / cost_real, 0)                 AS roi_D4
     , pay_user_num
     , pay_num
     , pay_sum
     , NVL(pay_sum / cost_real, 0)                 AS roi
     , NVL(pay_num / user_num * 100, 0)            AS conv_rate
     , NVL(cost_real / user_num, 0)                AS cac
     , NVL(wx_num / payment_num * 100, 0)          AS wx_rate
     , NVL(wx_active_num / payment_num * 100, 0)   AS wx_active_rate
     , NVL(collect_num / user_num * 100, 0)        AS collect_rate
     , NVL(collect_active_num / user_num * 100, 0) AS collect_active_rate
     , NVL(ifcome0 / user_num * 100, 0)               ifcome0_rate
     , NVL(ifok0 / user_num * 100, 0)                 ifok0_rate
     , NVL(ifcome1 / user_num * 100, 0)               ifcome1_rate
     , NVL(ifok1 / user_num * 100, 0)                 ifok1_rate
     , NVL(ifcome2 / user_num * 100, 0)               ifcome2_rate
     , NVL(ifok2 / user_num * 100, 0)                 ifok2_rate
     , NVL(ifcome3 / user_num * 100, 0)               ifcome3_rate
     , NVL(ifok3 / user_num * 100, 0)                 ifok3_rate
     , NVL(ifcome4 / user_num * 100, 0)               ifcome4_rate
     , NVL(ifok4 / user_num * 100, 0)                 ifok4_rate
     , NVL(ifcome5 / user_num * 100, 0)               ifcome5_rate
     , NVL(ifok5 / user_num * 100, 0)                 ifok5_rate
     , wx_active_num                                    AS wx_active_num
     , collect_active_num                               AS collect_active_num
     , ifcome0                                          AS ifcome0
     , ifok0                                            AS ifok0
     , ifcome1                                          AS ifcome1
     , ifok1                                            AS ifok1
     , ifcome2                                          AS ifcome2
     , ifok2                                            AS ifok2
     , ifcome3                                          AS ifcome3
     , ifok3                                            AS ifok3
     , ifcome4                                          AS ifcome4
     , ifok4                                            AS ifok4
     , ifcome5                                          AS ifcome5
     , ifok5                                            AS ifok5
FROM mid;

DFS -touchz /dw/app/c_app_course_period_baidusucai_dashboard/dt=${datebuf}/_SUCCESS;