SET mapred.job.name="c_app_course_daily_baidusucai_dashboard#${datebuf}";
USE app;
CREATE  TABLE IF NOT EXISTS app.c_app_course_daily_baidusucai_dashboard
(
    d_date              string COMMENT '日期',
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
    pay_sum_D7          float COMMENT 'D7正价课GMV',
    pay_sum_D8          float COMMENT 'D8正价课GMV',
    pay_sum_D10         float COMMENT 'D10正价课GMV',
    pay_sum_D14         float COMMENT 'D14正价课GMV',
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
    ifok5               int COMMENT 'D5完课例子数(个)',
    impression          float COMMENT '展现(次)',
    click               float COMMENT '点击(次)',
    ctr                 float COMMENT '点击率(%)',
    cpc                 float COMMENT '平均点击价格(元/次)',
    ocpcTargetTrans     float COMMENT '目标转化量(次)',
    ocpcTargetTransCPC  float COMMENT '目标转化成本(元/次)',
    completePlayCount   float COMMENT '播放完成数(次)',
    completePlayCost    float COMMENT '播放完成成本(元/次)',
    completePlayRatio   float COMMENT '播放完成率(%)',
    playCount1          float COMMENT '播放至25%及以上的次数(次)',
    playCount2          float COMMENT '播放至50%及以上的次数(次)',
    playCount3          float COMMENT '播放至75%及以上的次数(次)',
    playCount4          float COMMENT '播放至100%的次数(次)',
    avgPlayTime         float COMMENT '平均播放时长(s/次)',
    manualPlayCount     float COMMENT '主动播放数(次)',
    autoPlayCount       float COMMENT '自动播放数(次)',
    effectivePlayCount  float COMMENT '有效播放数(次)'
)
    COMMENT '培训主题数仓-百度素材日报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_daily_baidusucai_dashboard';

SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;

WITH cost_api AS
    (SELECT d_date
          , cat
          , ad_department
          , platform
          , price
          , mobile
          , link_type_v2
          , pos
          , cost_id
          , advertiser_name
          , agent
          , ad_id
          , sucai_id
          , sucai_name
          , cost
          , cost_real
          , GET_JSON_OBJECT(json_data, '$.impression')         AS impression         --展现
          , GET_JSON_OBJECT(json_data, '$.click')              AS click              --点击
          , GET_JSON_OBJECT(json_data, '$.ctr')                AS ctr                --点击率
          , GET_JSON_OBJECT(json_data, '$.cpc')                AS cpc                --平均点击价格
          , GET_JSON_OBJECT(json_data, '$.ocpcTargetTrans')    AS ocpcTargetTrans    --目标转化量
          , GET_JSON_OBJECT(json_data, '$.ocpcTargetTransCPC') AS ocpcTargetTransCPC --目标转化成本
          , GET_JSON_OBJECT(json_data, '$.completePlayCount')  AS completePlayCount  --播放完成数
          , GET_JSON_OBJECT(json_data, '$.completePlayCost')   AS completePlayCost   --播放完成成本
          , GET_JSON_OBJECT(json_data, '$.completePlayRatio')  AS completePlayRatio  --播放完成率
          , GET_JSON_OBJECT(json_data, '$.playCount1')         AS playCount1         --播放至25%及以上的次数
          , GET_JSON_OBJECT(json_data, '$.playCount2')         AS playCount2         --播放至50%及以上的次数
          , GET_JSON_OBJECT(json_data, '$.playCount3')         AS playCount3         --播放至75%及以上的次数
          , GET_JSON_OBJECT(json_data, '$.playCount4')         AS playCount4         --播放至100%的次数
          , GET_JSON_OBJECT(json_data, '$.avgPlayTime')        AS avgPlayTime        --平均播放时长
          , GET_JSON_OBJECT(json_data, '$.manualPlayCount')    AS manualPlayCount    --主动播放数
          , GET_JSON_OBJECT(json_data, '$.autoPlayCount')      AS autoPlayCount      --自动播放数
          , GET_JSON_OBJECT(json_data, '$.effectivePlayCount') AS effectivePlayCount --有效播放数
     FROM da.da_course_daily_cost_by_sucaiid
     WHERE dt = '${datebuf}'
       AND d_date <= '${datebuf}')
   , mid AS
    ( -- 按日+sucaiid维度聚合用户
        SELECT TO_DATE(created_at)                                                                AS d_date
             --     ,cat
             --     ,platform
             --     ,platform_name
             --     ,price
             --     ,link_type_v2
             --     ,mobile
             --     ,pos
             , ad_department
             , cost_id
             , ad_id
             , sucai_id
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
                      0))                                                                         AS pay_num_D4         -- D4正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D4,
                      0))                                                                         AS pay_sum_D4         -- D4正价课GMV

             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D7,
                      0))                                                                         AS pay_sum_D7         -- D8正价课GMV
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D8,
                      0))                                                                         AS pay_sum_D8         -- D8正价课GMV
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D10,
                      0))                                                                         AS pay_sum_D10         -- D10正价课GMV

             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D14,
                      0))                                                                         AS pay_sum_D14         -- D10正价课GMV
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
        FROM dws.dws_sale_camping_user_day
        WHERE dt = '${datebuf}'
          --11.13日新增剔除小糖私域-私域群活码链路
          AND (platform_name != '小糖私域' OR pos != '私域群活码')
        GROUP BY TO_DATE(created_at)
               --     ,cat
               --     ,platform
               --     ,platform_name
               --     ,price
               --     ,link_type_v2
               --     ,mobile
               --     ,pos
               , ad_department
               , cost_id
               , ad_id
               , sucai_id)
--关联消耗和聚合的用户数
   , r1 AS
    (SELECT COALESCE(a.d_date, b.d_date)                             AS d_date
          , COALESCE(a.cat, '无')                                    AS cat
          , if(COALESCE(b.ad_department,COALESCE(a.ad_department,'信息流一部'),'无')='信息流四部','信息流一部',COALESCE(b.ad_department,COALESCE(a.ad_department,'信息流一部'),'无'))  AS ad_department
          , COALESCE(a.platform, '无')                               AS platform_name
          , COALESCE(a.pos, '无')                                    AS pos
          , COALESCE(a.price, '无')                                  AS price
          , COALESCE(a.link_type_v2, '无')                           AS link_type_v2
          , COALESCE(a.mobile, '无')                                 AS mobile
          , COALESCE(a.agent, '无')                                  AS agent
          , CONCAT('id_', COALESCE(a.cost_id, 'other'))              AS cost_id
          , CONCAT('id_', COALESCE(a.ad_id, 'other'))                AS ad_id
          , CONCAT('id_', COALESCE(a.sucai_id, b.sucai_id, 'other')) AS sucai_id
          , COALESCE(a.sucai_name, '无')                             AS sucai_name
          --, a.sucai_type
          , NVL(cost, 0)                                             AS cost
          , NVL(cost_real, 0)                                        AS cost_real
          , NVL(submit_num, 0)                                       AS submit_num
          , NVL(payment_num, 0)                                      AS payment_num
          , NVL(user_num, 0)                                         AS user_num
          , NVL(wx_num, 0)                                           AS wx_num
          --, NVL(wx_active_num, 0)                                   AS wx_active_num
          , NVL(collect_num, 0)                                      AS collect_num
          --, NVL(collect_active_num, 0)                              AS collect_active_num
          , NVL(pay_num_D4, 0)                                       AS pay_num_D4
          , NVL(pay_sum_D4, 0)                                       AS pay_sum_D4

          , NVL(pay_sum_D4 / cost_real, 0)                           AS roi_D4
          , NVL(pay_sum_D7, 0)                                       AS pay_sum_D7
          , NVL(pay_sum_D8, 0)                                       AS pay_sum_D8
          , NVL(pay_sum_D10, 0)                                       AS pay_sum_D10
          , NVL(pay_sum_D14, 0)                                       AS pay_sum_D14
          , NVL(pay_user_num, 0)                                     AS pay_user_num
          , NVL(pay_num, 0)                                          AS pay_num
          , NVL(pay_sum, 0)                                          AS pay_sum
          , NVL(pay_sum / cost_real, 0)                              AS roi
          , NVL(pay_num / user_num * 100, 0)                         AS conv_rate
          , NVL(cost_real / user_num, 0)                             AS cac
          , NVL(wx_num / payment_num * 100, 0)                       AS wx_rate
          , NVL(wx_active_num / payment_num * 100, 0)                AS wx_active_rate
          , NVL(collect_num / user_num * 100, 0)                     AS collect_rate
          , NVL(collect_active_num / user_num * 100, 0)              AS collect_active_rate
          , NVL(ifcome0 / user_num * 100, 0)                            ifcome0_rate
          , NVL(ifok0 / user_num * 100, 0)                              ifok0_rate
          , NVL(ifcome1 / user_num * 100, 0)                            ifcome1_rate
          , NVL(ifok1 / user_num * 100, 0)                              ifok1_rate
          , NVL(ifcome2 / user_num * 100, 0)                            ifcome2_rate
          , NVL(ifok2 / user_num * 100, 0)                              ifok2_rate
          , NVL(ifcome3 / user_num * 100, 0)                            ifcome3_rate
          , NVL(ifok3 / user_num * 100, 0)                              ifok3_rate
          , NVL(ifcome4 / user_num * 100, 0)                            ifcome4_rate
          , NVL(ifok4 / user_num * 100, 0)                              ifok4_rate
          , NVL(ifcome5 / user_num * 100, 0)                            ifcome5_rate
          , NVL(ifok5 / user_num * 100, 0)                              ifok5_rate
          , NVL(wx_active_num, 0)                                    AS wx_active_num
          , NVL(collect_active_num, 0)                               AS collect_active_num
          , NVL(ifcome0, 0)                                          AS ifcome0
          , NVL(ifok0, 0)                                            AS ifok0
          , NVL(ifcome1, 0)                                          AS ifcome1
          , NVL(ifok1, 0)                                            AS ifok1
          , NVL(ifcome2, 0)                                          AS ifcome2
          , NVL(ifok2, 0)                                            AS ifok2
          , NVL(ifcome3, 0)                                          AS ifcome3
          , NVL(ifok3, 0)                                            AS ifok3
          , NVL(ifcome4, 0)                                          AS ifcome4
          , NVL(ifok4, 0)                                            AS ifok4
          , NVL(ifcome5, 0)                                          AS ifcome5
          , NVL(ifok5, 0)                                            AS ifok5
          , NVL(impression, 0)                                       AS impression--展现
          , NVL(click, 0)                                            AS click--点击
          , NVL(ctr, 0)                                              AS ctr--点击率
          , NVL(cpc, 0)                                              AS cpc--平均点击价格
          , NVL(ocpcTargetTrans, 0)                                  AS ocpcTargetTrans--目标转化量
          , NVL(ocpcTargetTransCPC, 0)                               AS ocpcTargetTransCPC--目标转化成本
          , NVL(completePlayCount, 0)                                AS completePlayCount--播放完成数
          , NVL(completePlayCost, 0)                                 AS completePlayCost--播放完成成本
          , NVL(completePlayRatio, 0)                                AS completePlayRatio--播放完成率
          , NVL(playCount1, 0)                                       AS playCount1--播放至25%及以上的次数
          , NVL(playCount2, 0)                                       AS playCount2--播放至50%及以上的次数
          , NVL(playCount3, 0)                                       AS playCount3--播放至75%及以上的次数
          , NVL(playCount4, 0)                                       AS playCount4--播放至100%的次数
          , NVL(avgPlayTime, 0)                                      AS avgPlayTime--平均播放时长
          , NVL(manualPlayCount, 0)                                  AS manualPlayCount--主动播放数
          , NVL(autoPlayCount, 0)                                    AS autoPlayCount--自动播放数
          , NVL(effectivePlayCount, 0)                               AS effectivePlayCount--有效播放数
     FROM cost_api a
              LEFT JOIN mid b
                        ON a.d_date = b.d_date
                            -- and a.cat = b.cat
-- and a.platform = b.platform_name
-- and a.pos = b.pos
-- and a.price = b.price
-- and a.link_type_v2 = b.link_type_v2
-- and a.mobile = b.mobile
                            AND a.cost_id = b.cost_id
                            AND a.ad_id = b.ad_id
                            AND a.sucai_id = b.sucai_id)

/*  , result AS (SELECT d_date
                    --,'11_品类x渠道x版位x价格x新链路x手机号x代理x账户x计划x素材' as grouptype
                    , cat
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
                    , SUM(cost)                                             AS cost
                    , SUM(cost_real)                                        AS cost_real
                    , SUM(submit_num)                                       AS submit_num
                    , SUM(payment_num)                                      AS payment_num
                    , SUM(user_num)                                         AS user_num
                    , SUM(wx_num)                                           AS wx_num
                    , SUM(collect_num)                                      AS collect_num
                    , SUM(pay_user_num)                                     AS pay_user_num
                    , SUM(pay_num)                                          AS pay_num
                    , SUM(pay_sum)                                          AS pay_sum
                    , NVL(SUM(pay_sum) / SUM(cost_real), 0)                 AS roi
                    , NVL(SUM(pay_num) / SUM(user_num) * 100, 0)            AS conv_rate
                    , NVL(SUM(cost_real) / SUM(user_num), 0)                AS cac
                    , NVL(SUM(wx_num) / SUM(payment_num) * 100, 0)          AS wx_rate
                    , NVL(SUM(wx_active_num) / SUM(payment_num) * 100, 0)   AS wx_active_rate
                    , NVL(SUM(collect_num) / SUM(user_num) * 100, 0)        AS collect_rate
                    , NVL(SUM(collect_active_num) / SUM(user_num) * 100, 0) AS collect_active_rate
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
               FROM r1
               -- where cat is not null
-- and cat <> '无'

               GROUP BY d_date
                      , cat
                      , platform_name
                      , pos
                      , price
                      , link_type_v2
                      , mobile
                      , agent
                      , cost_id
                      , ad_id
                      , sucai_id
                      , sucai_name)
*/
INSERT
OVERWRITE
TABLE
app.c_app_course_daily_baidusucai_dashboard
PARTITION
(
dt = '${datebuf}'
)
SELECT d_date
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
     , roi_D4
     ,pay_sum_D7
     , pay_sum_D8
     , pay_sum_D10
     , pay_sum_D14
     , pay_user_num
     , pay_num
     , pay_sum
     , roi
     , conv_rate
     , cac
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
     , wx_active_num
     , collect_active_num
     , ifcome0
     , ifok0
     , ifcome1
     , ifok1
     , ifcome2
     , ifok2
     , ifcome3
     , ifok3
     , ifcome4
     , ifok4
     , ifcome5
     , ifok5
     , impression
     , click
     , ctr
     , cpc
     , ocpcTargetTrans
     , ocpcTargetTransCPC
     , completePlayCount
     , completePlayCost
     , completePlayRatio
     , playCount1
     , playCount2
     , playCount3
     , playCount4
     , avgPlayTime
     , manualPlayCount
     , autoPlayCount
     , effectivePlayCount
FROM r1
WHERE pos IN ('百度搜索', '百度信息流','百度信息流北京');

DFS -touchz /dw/app/c_app_course_daily_baidusucai_dashboard/dt=${datebuf}/_SUCCESS;