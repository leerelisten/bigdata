SET mapred.job.name="c_app_course_period_live_dashboard#${datebuf}";
USE app;

CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_course_period_live_dashboard
(
    d_date              string COMMENT '日期',
    grouptype           string COMMENT '分组',
    goods_name          string COMMENT '期次',
    cat                 string COMMENT '品类',
    ad_department       string COMMENT '投放部门',
    platform_name       string COMMENT '渠道',
    pos                 string COMMENT '版位',
    h5_id               string COMMENT 'H5ID',
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
    pay_num_D4          float COMMENT 'D4正价课订单数(单)',
    pay_sum_D4          float COMMENT 'D4正价课GMV(元)',
    roi_D4              float COMMENT 'D4ROI',
    pay_num             float COMMENT '正价课订单数(单)',
    pay_sum             float COMMENT '正价课GMV(元)',
    roi                 float COMMENT 'ROI=正价课GMV/实际支出',
    cac                 float COMMENT '账面CAC(元/个)',
    cac_real            float COMMENT '实际CAC(元/个)',
    conv_rate           float COMMENT '转化率=正价课订单数/例子数(%)',
    wx_rate             float COMMENT '加微率=加微例子数/例子数(%)',
    wx_active_rate      float COMMENT '主动加微率=主动加微例子数/例子数(%)',
    collect_rate        float COMMENT '问卷率=填问卷例子数/例子数(%)',
    collect_active_rate float COMMENT '主动问卷率=主动填问卷例子数/例子数(%)',
    d0_come_rate        float COMMENT '导学课到课率(%)',
    d0_ok_rate          float COMMENT '导学课完课率(%)',
    d1_come_rate        float COMMENT 'D1到课率(%)',
    d1_ok_rate          float COMMENT 'D1完课率(%)',
    d2_come_rate        float COMMENT 'D2到课率(%)',
    d2_ok_rate          float COMMENT 'D2完课率(%)',
    d3_come_rate        float COMMENT 'D3到课率(%)',
    d3_ok_rate          float COMMENT 'D3完课率(%)',
    d4_come_rate        float COMMENT 'D4到课率(%)',
    d4_ok_rate          float COMMENT 'D4完课率(%)',
    d5_come_rate        float COMMENT 'D5到课率(%)',
    d5_ok_rate          float COMMENT 'D5完课率(%)',
    m_olduv_rate        float COMMENT '重复例子占比(%)',
    m_olduv_rate30      float COMMENT '重复例子占比(30天)(%)'
)
    COMMENT '培训主题数仓-直播分期次报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_period_live_dashboard';


SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=2048;
SET mapreduce.reduce.memory.mb=2048;
SET hive.execution.engine = tez;


--改用账户日CAC进行计算
WITH cost AS
    (SELECT cost_id
          --, ad_id
          , agent
          , d_date
          , SUM(cost)      AS cost
          , SUM(cost_real) AS cost_real
     FROM da.da_course_daily_cost_by_adid
     WHERE dt = '${datebuf}'
     GROUP BY cost_id
            --, ad_id
            , agent
            , d_date)
   , user_data AS (SELECT a.member_id
                        , a.cat
                        , a.ad_department
                        , a.platform
                        , a.platform_name
                        , a.department
                        , a.user_group
                        , a.h5_id
                        , a.pos
                        , a.price
                        , a.mobile
                        , a.link_type_v1
                        , a.link_type_v2
                        , a.cost_id
                        , a.cost_id2
                        , a.ad_id
                        , a.sucai_id
                        , a.created_at
                        , a.wx_rel_status
                        , a.goods_name
                        , a.xe_id
                        , a.ifcollect
                        , a.trade_state
                        , a.member_status
                        , a.sales_id
                        , a.sales_name
                        , a.ifbuy
                        , a.first_pay_time
                        , a.pay_num
                        , a.pay_sum
                        , a.pay_num_D4
                        , a.pay_sum_D4
                        , a.pay_num_D4_24h
                        , a.pay_sum_D4_24h
                        , a.pay_num_D5
                        , a.pay_sum_D5
                        , a.pay_num_D6
                        , a.pay_sum_D6
                        , a.pay_num_D7
                        , a.pay_sum_D7
                        , a.pay_num_D14
                        , a.pay_sum_D14
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
                        , a.cac
                        , a.cac_real
                        , a.d_date
                        , a.dt
                   FROM dws.dws_sale_camping_user_day a
                   WHERE a.dt = '${datebuf}'
                     AND TO_DATE(CONCAT('20', SUBSTR(a.goods_name, 2, 2), '-', SUBSTR(a.goods_name, 4, 2), '-',
                                        SUBSTR(a.goods_name, 6, 2))) >= '2024-05-01'
                     AND a.created_at >= '2024-05-01'
                     --这里先注释掉 背景：20250403 发现业务方有 期次包含“测试”二字 但是数据是需要的
--                      AND a.goods_name NOT LIKE '%测试%'
                     AND (a.platform_name != '小糖私域' OR a.pos != '私域群活码') -- 20241113 剔除私域群活码

)
   , user_num AS
    (SELECT cost_id2
          --, ad_id
          , TO_DATE(created_at) AS d_date
          , COUNT(*)               num
     FROM user_data
     WHERE member_status = 1
       AND trade_state IN ('SUCCESS', 'PREPARE')
       AND sales_id > 0
     -- and to_date(created_at) >= '2024-07-01'
     GROUP BY cost_id2
            --, ad_id
            --,sucai_id
            , TO_DATE(created_at))
   , cac AS
    (SELECT a.cost_id
          , a.agent
          --, a.ad_id
          , a.d_date
          , NVL(a.cost / b.num, 0)      AS cac
          , NVL(a.cost_real / b.num, 0) AS cac_real
     FROM cost a
              LEFT JOIN user_num b
                        ON a.cost_id = b.cost_id2 AND a.d_date = b.d_date)
   , user_info AS
    ( -- 所有例子的投放属性&加微、问卷状态&购买正价课状态
        SELECT a.member_id
             , a.cat
             , a.ad_department
             , a.platform
             , a.platform_name
             , a.h5_id
             , a.pos
             , a.price
             , a.mobile
             , a.link_type_v1
             , CASE WHEN a.link_type_v2 = '新一键' THEN '新一键授权' ELSE a.link_type_v2 END AS link_type_v2
             , a.cost_id2                                                                    AS cost_id
             , d.agent
             --, a.ad_id
             --,a.sucai_id
             , a.created_at
             , a.wx_rel_status
             , a.goods_name
             , a.xe_id
             , a.ifcollect
             , a.trade_state
             , a.member_status
             , a.sales_id
             , a.ifbuy
             , a.pay_num
             , a.pay_sum
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
             , NVL(d.cac, 0)                                                                    cac      --因为只有直播,无手工判断cac
             , NVL(d.cac_real, 0)                                                               cac_real --因为只有直播,无手工录入cac
             , a.pay_num_D4
             , a.pay_sum_D4
        FROM user_data a
                 LEFT JOIN cac d
                           ON d.cost_id = a.cost_id2
                               AND d.d_date = TO_DATE(a.created_at))
   , olduser AS
    (SELECT member_id AS oldid
          , goods_name
     FROM (SELECT member_id
                , goods_name
                , created_at
                , cat
                , LAG(created_at) OVER (PARTITION BY member_id,cat ORDER BY created_at ASC) AS lasttime
           FROM dw.dwd_xt_user
           WHERE dt = '${datebuf}'
             AND TO_DATE(created_at) <= '${datebuf}'
             AND member_status = 1
             AND trade_state IN ('SUCCESS', 'PREPARE')
             AND sales_id > 0) t
     WHERE lasttime IS NOT NULL
     GROUP BY member_id, goods_name)
   , olduser30 AS
    (SELECT member_id AS oldid
          , goods_name
     FROM (SELECT member_id
                , goods_name
                , created_at
                , cat
                , UNIX_TIMESTAMP(created_at) -
                  UNIX_TIMESTAMP(LAG(created_at) OVER (PARTITION BY member_id,cat ORDER BY created_at ASC)) AS timediff
           FROM dw.dwd_xt_user
           WHERE dt = '${datebuf}'
             AND TO_DATE(created_at) <= '${datebuf}'
             AND member_status = 1
             AND trade_state IN ('SUCCESS', 'PREPARE')
             AND sales_id > 0) t
     WHERE timediff <= 30 * 24 * 3600
     GROUP BY member_id, goods_name)

   , mid AS
    ( -- 按期次+投放属性聚合用户
        SELECT TO_DATE(created_at)                                                                AS d_date
             , cat
             , ad_department
             , platform_name
             , price
             , link_type_v1
             , link_type_v2
             , mobile
             , pos
             , h5_id                                                                                                    -- 多了h5_id
             , NVL(agent, '')                                                                     AS agent
             , CONCAT('id_', COALESCE(cost_id, 'other'))                                          AS cost_id
             --, CONCAT('id_', COALESCE(ad_id, 'other'))                                            AS ad_id
             --,sucai_id
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
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num,
                      0))                                                                         AS pay_num            -- 正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum,
                      0))                                                                         AS pay_sum            -- 正价课GMV
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D4,
                      0))                                                                         AS pay_num_D4         -- 正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D4,
                      0))                                                                         AS pay_sum_D4         -- 正价课GMV
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
             , COUNT(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, olduser.oldid,
                        NULL))                                                                    AS olduser_num        -- 老用户例子数
             , COUNT(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, olduser30.oldid,
                        NULL))                                                                    AS olduser_num30      -- 老用户例子数30
             , user_info.goods_name
        FROM user_info
                 LEFT JOIN olduser
                           ON user_info.member_id = olduser.oldid AND user_info.goods_name = olduser.goods_name
                 LEFT JOIN olduser30
                           ON user_info.member_id = olduser30.oldid AND user_info.goods_name = olduser30.goods_name
        GROUP BY TO_DATE(created_at)
               , user_info.goods_name
               , cat
               , ad_department
               , platform_name
               , price
               , link_type_v1
               , link_type_v2
               , mobile
               , pos
               , h5_id -- 多了h5_id
               , cost_id
               , agent
        --, ad_id
    )


   , result_adid AS (SELECT '${datebuf}'                                          AS d_date
                          , CASE GROUPING__ID
                                WHEN 3 THEN '0_H5ID'
                                WHEN 0 THEN '1_账户'
        END                                                                       AS grouptype
                          , goods_name
                          , cat
                          , ad_department
                          , platform_name
                          , pos
                          , h5_id
                          , price
                          , link_type_v2
                          , mobile
                          , agent
                          , cost_id
                          , 'ALL-汇总'                                               ad_id
                          , 'ALL-汇总'                                               sucai_id
                          , 'ALL-汇总'                                               sucai_name
                          , SUM(cost)                                             AS cost                -- 账面消耗
                          , SUM(cost_real)                                        AS cost_real           --实际消耗
                          , SUM(submit_num)                                       AS submit_num          --提交
                          , SUM(payment_num)                                      AS payment_num         --支付
                          , SUM(user_num)                                         AS user_num            --例子数
                          , SUM(wx_num)                                           AS wx_num              --加微数
                          --,sum(collect_num) as collect_num --问卷数
                          -- ,sum(pay_user_num) as pay_user_num --购买正价课例子数
                          , SUM(pay_num_D4)                                       AS pay_num_D4
                          , SUM(pay_sum_D4)                                       AS pay_sum_D4
                          , NVL(SUM(pay_sum_D4) / SUM(cost_real), 0)              AS roi_D4
                          , SUM(pay_num)                                          AS pay_num             --正价课订单数 ？？用哪个
                          , SUM(pay_sum)                                          AS pay_sum             --正价课GMV
                          , NVL(SUM(pay_sum) / SUM(cost_real), 0)                 AS roi                 -- roi
                          , NVL(SUM(cost) / SUM(user_num), 0)                     AS cac                 -- 账面cac
                          , NVL(SUM(cost_real) / SUM(user_num), 0)                AS cac_real            -- 实际cac
                          , NVL(SUM(pay_num) / SUM(user_num) * 100, 0)            AS conv_rate           --转化率
                          , NVL(SUM(wx_num) / SUM(payment_num) * 100, 0)          AS wx_rate             -- 加微率
                          , NVL(SUM(wx_active_num) / SUM(payment_num) * 100, 0)   AS wx_active_rate      --主动加微率
                          , NVL(SUM(collect_num) / SUM(user_num) * 100, 0)        AS collect_rate        -- 问卷率
                          , NVL(SUM(collect_active_num) / SUM(user_num) * 100, 0) AS collect_active_rate --主动问卷率
                          , NVL(SUM(ifcome0) / SUM(user_num) * 100, 0)               ifcome0_rate
                          , NVL(SUM(ifok0) / SUM(user_num) * 100, 0)                 ifok0_rate
                          , NVL(SUM(ifcome1) / SUM(user_num) * 100, 0)               ifcome1_rate
                          , NVL(SUM(ifok1) / SUM(user_num) * 100, 0)                 ifok1_rate
                          , NVL(SUM(ifcome2) / SUM(user_num) * 100, 0)               ifcome2_rate
                          , NVL(SUM(ifok2) / SUM(user_num) * 100, 0)                 ifok2_rate
                          , NVL(SUM(ifcome3) / SUM(user_num) * 100, 0)               ifcome3_rate
                          , NVL(SUM(ifok3) / SUM(user_num) * 100, 0)                 ifok3_rate
                          , NVL(SUM(ifcome4) / SUM(user_num) * 100, 0)               ifcome4_rate
                          , NVL(SUM(ifok4) / SUM(user_num) * 100, 0)                 ifok4_rate
                          , NVL(SUM(ifcome5) / SUM(user_num) * 100, 0)               ifcome5_rate
                          , NVL(SUM(ifok5) / SUM(user_num) * 100, 0)                 ifok5_rate
                          , SUM(wx_active_num)                                    AS wx_active_num
                          , SUM(collect_active_num)                               AS collect_active_num
                          , SUM(ifcome0)                                          AS ifcome0
                          , SUM(ifok0)                                            AS ifok0
                          , SUM(ifcome1)                                          AS ifcome1
                          , SUM(ifok1)                                            AS ifok1
                          , SUM(ifcome2)                                          AS ifcome2
                          , SUM(ifok2)                                            AS ifok2
                          , SUM(ifcome3)                                          AS ifcome3
                          , SUM(ifok3)                                            AS ifok3
                          , SUM(ifcome4)                                          AS ifcome4
                          , SUM(ifok4)                                            AS ifok4
                          , SUM(ifcome5)                                          AS ifcome5
                          , SUM(ifok5)                                            AS ifok5
                          , NVL(SUM(olduser_num) / SUM(user_num) * 100, 0)        AS m_olduv_rate-- 老用户例子占比
                          , NVL(SUM(olduser_num30) / SUM(user_num) * 100, 0)      AS m_olduv_rate30-- 老用户例子占比30
                     FROM mid
                     GROUP BY goods_name
                            , cat
                            , ad_department
                            , platform_name
                            , pos
                            , h5_id
                            , price
                            , link_type_v2
                            , mobile
                            , agent
                            , cost_id
                     --, ad_id
                         GROUPING SETS (
                            (goods_name, cat, ad_department, platform_name, pos, h5_id, price, link_type_v2, mobile) --3
                            , (goods_name, cat, ad_department, platform_name, pos, h5_id, price, link_type_v2, mobile
                            , agent
                            , cost_id)                                                                               --0
                         ))

INSERT
OVERWRITE
TABLE
app.c_app_course_period_live_dashboard
PARTITION
(
dt = '${datebuf}'
)
SELECT d_date
     , grouptype
     , goods_name
     , cat
     , NVL(ad_department, 'ALL-汇总') AS ad_department
     , platform_name
     , pos
     , h5_id
     , price
     , link_type_v2
     , mobile
     , COALESCE(agent, 'ALL-汇总')       name
     , COALESCE(cost_id, 'ALL-汇总')     costid
     , COALESCE(ad_id, 'ALL-汇总')       adid
     , sucai_id
     , sucai_name
     , cost
     , cost_real
     , submit_num
     , payment_num
     , user_num
     , wx_num
     , pay_num_D4
     , pay_sum_D4
     , roi_D4
     , pay_num
     , pay_sum
     , roi
     , cac
     , cac_real
     , conv_rate
     , wx_rate
     , wx_active_rate
     , collect_rate
     , collect_active_rate
     , ifcome0_rate
     , ifok0_rate
     , ifcome1_rate
     , ifok1_rate
     , ifcome2_rate
     , ifok2_rate
     , ifcome3_rate
     , ifok3_rate
     , ifcome4_rate
     , ifok4_rate
     , ifcome5_rate
     , ifok5_rate
     , m_olduv_rate
     , m_olduv_rate30
FROM result_adid
WHERE platform_name = '抖音'
  AND pos LIKE '%直播%'
  AND pos <> '千川直播'

