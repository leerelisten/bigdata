SET mapred.job.name="c_app_course_daily_tencentsucai_dashboard#${datebuf}";
USE app;
CREATE  TABLE IF NOT EXISTS app.c_app_course_daily_tencentsucai_dashboard
(
    d_date                      string COMMENT '日期',
    cat                         string COMMENT '品类',
    ad_department               string COMMENT '投放部门',
    platform_name               string COMMENT '渠道',
    pos                         string COMMENT '版位',
    price                       string COMMENT '价格',
    link_type_v2                string COMMENT '链路类型(新)',
    mobile                      string COMMENT '收集手机号',
    agent                       string COMMENT '代理',
    cost_id                     string COMMENT '账户id',
    ad_id                       string COMMENT '计划id',
    sucai_id                    string COMMENT '素材id',
    sucai_name                  string COMMENT '素材名称',
    sucai_type                  string COMMENT '素材类型',
    cost                        float COMMENT '账面消耗(元)',
    cost_real                   float COMMENT '实际消耗(元)',
    submit_num                  int COMMENT '表单填写例子数(个)',
    payment_num                 int COMMENT '支付成功例子数(个)',
    user_num                    int COMMENT '例子数(个)',
    wx_num                      int COMMENT '加微例子数(个)',
    collect_num                 int COMMENT '填问卷例子数(个)',
    pay_num_D4                  float COMMENT 'D4正价课订单数(单)',
    pay_sum_D4                  float COMMENT 'D4正价课GMV(元)',
    roi_D4                      float COMMENT 'D4ROI',
    pay_sum_D7                  float COMMENT 'D7正价课GMV',
    pay_sum_D8                  float COMMENT 'D8正价课GMV',
    pay_sum_D10                 float COMMENT 'D10正价课GMV',
    pay_sum_D14                 float COMMENT 'D14正价课GMV',
    pay_user_num                int COMMENT '购买正价课例子数(个)',
    pay_num                     float COMMENT '正价课订单数(单)',
    pay_sum                     float COMMENT '正价课GMV(元)',
    roi                         float COMMENT 'ROI=正价课GMV/实际支出',
    conv_rate                   float COMMENT '转化率=正价课订单数/例子数(%)',
    cac                         float COMMENT 'CAC=实际支出/例子数(元/个)',
    wx_rate                     float COMMENT '加微率=加微例子数/例子数(%)',
    wx_active_rate              float COMMENT '主动加微率=主动加微例子数/例子数(%)',
    collect_rate                float COMMENT '问卷率=填问卷例子数/例子数(%)',
    collect_active_rate         float COMMENT '主动问卷率=主动填问卷例子数/例子数(%)',
    ifcome0_rate                float COMMENT '导学课到课率(%)',
    ifok0_rate                  float COMMENT '导学课完课率(%)',
    ifcome1_rate                float COMMENT 'D1到课率(%)',
    ifok1_rate                  float COMMENT 'D1完课率(%)',
    ifcome2_rate                float COMMENT 'D2到课率(%)',
    ifok2_rate                  float COMMENT 'D2完课率(%)',
    ifcome3_rate                float COMMENT 'D3到课率(%)',
    ifok3_rate                  float COMMENT 'D3完课率(%)',
    ifcome4_rate                float COMMENT 'D4到课率(%)',
    ifok4_rate                  float COMMENT 'D4完课率(%)',
    ifcome5_rate                float COMMENT 'D5到课率(%)',
    ifok5_rate                  float COMMENT 'D5完课率(%)',
    wx_active_num               int COMMENT '主动加微例子数(个)',
    collect_active_num          int COMMENT '主动填问卷例子数(个)',
    ifcome0                     int COMMENT '导学课到课例子数(个)',
    ifok0                       int COMMENT '导学课完课例子数(个)',
    ifcome1                     int COMMENT 'D1到课例子数(个)',
    ifok1                       int COMMENT 'D1完课例子数(个)',
    ifcome2                     int COMMENT 'D2到课例子数(个)',
    ifok2                       int COMMENT 'D2完课例子数(个)',
    ifcome3                     int COMMENT 'D3到课例子数(个)',
    ifok3                       int COMMENT 'D3完课例子数(个)',
    ifcome4                     int COMMENT 'D4到课例子数(个)',
    ifok4                       int COMMENT 'D4完课例子数(个)',
    ifcome5                     int COMMENT 'D5到课例子数(个)',
    ifok5                       int COMMENT 'D5完课例子数(个)',
    view_count                  float COMMENT '曝光次数(次)',
    valid_click_count           float COMMENT '点击次数(次)',
    conversions_count           float COMMENT '目标转化量(次)',
    conversions_rate            float COMMENT '目标转化率(%)',
    conversions_cost            float COMMENT '目标转化成本(元/次)',
    video_outer_play_count      float COMMENT '有效播放数(次)',
    video_outer_play_rate       float COMMENT '有效播放率(%)',
    video_outer_play_cost       float COMMENT '有效播放成本(元/次)',
    video_outer_play_time_count float COMMENT '平均有效播放时长(S/次)',
    video_outer_play10_count    float COMMENT '10%进度播放次数(次)',
    video_outer_play25_count    float COMMENT '25%进度播放次数(次)',
    video_outer_play50_count    float COMMENT '50%进度播放次数(次)',
    video_outer_play75_count    float COMMENT '75%进度播放次数(次)',
    video_outer_play90_count    float COMMENT '90%进度播放次数(次)',
    video_outer_play95_count    float COMMENT '95%进度播放次数(次)',
    video_outer_play100_count   float COMMENT '100%进度播放次数(次)',
    video_outer_play3s_count    float COMMENT '3s播放完成次数(次)',
    video_outer_play5s_count    float COMMENT '5s播放完成次数(次)',
    video_outer_play7s_count    float COMMENT '7s播放完成次数(次)'
)
    COMMENT '培训主题数仓-腾讯素材日报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_daily_tencentsucai_dashboard';

SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;

WITH cost_api AS
    (SELECT d_date
          , cat
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
          , sucai_type
          , ad_department
          , GET_JSON_OBJECT(json_data, '$.view_count')                  AS view_count--曝光次数
          , GET_JSON_OBJECT(json_data, '$.valid_click_count')           AS valid_click_count --点击次数
          , GET_JSON_OBJECT(json_data, '$.conversions_count')           AS conversions_count --目标转化量
          , GET_JSON_OBJECT(json_data, '$.conversions_rate')            AS conversions_rate --目标转化率
          , GET_JSON_OBJECT(json_data, '$.conversions_cost')            AS conversions_cost--目标转化成本
          , GET_JSON_OBJECT(json_data, '$.video_outer_play_count')      AS video_outer_play_count--有效播放数
          , GET_JSON_OBJECT(json_data, '$.video_outer_play_rate')       AS video_outer_play_rate--有效播放率
          , GET_JSON_OBJECT(json_data, '$.video_outer_play_cost')       AS video_outer_play_cost--有效播放成本
          , GET_JSON_OBJECT(json_data, '$.video_outer_play_time_count') AS video_outer_play_time_count--平均有效播放时长
          , GET_JSON_OBJECT(json_data, '$.video_outer_play10_count')    AS video_outer_play10_count--10%进度播放次数
          , GET_JSON_OBJECT(json_data, '$.video_outer_play25_count')    AS video_outer_play25_count--25%进度播放次数
          , GET_JSON_OBJECT(json_data, '$.video_outer_play50_count')    AS video_outer_play50_count--50%进度播放次数
          , GET_JSON_OBJECT(json_data, '$.video_outer_play75_count')    AS video_outer_play75_count--75%进度播放次数
          , GET_JSON_OBJECT(json_data, '$.video_outer_play90_count')    AS video_outer_play90_count--90%进度播放次数
          , GET_JSON_OBJECT(json_data, '$.video_outer_play95_count')    AS video_outer_play95_count--95%进度播放次数
          , GET_JSON_OBJECT(json_data, '$.video_outer_play100_count')   AS video_outer_play100_count--100%进度播放次数
          , GET_JSON_OBJECT(json_data, '$.video_outer_play3s_count')    AS video_outer_play3s_count--3s播放完成次数
          , GET_JSON_OBJECT(json_data, '$.video_outer_play5s_count')    AS video_outer_play5s_count--5s播放完成次数
          , GET_JSON_OBJECT(json_data, '$.video_outer_play7s_count')    AS video_outer_play7s_count--7s播放完成次数
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
                      0))                                                                         AS pay_sum_D7         -- D7正价课GMV

             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D8,
                      0))                                                                         AS pay_sum_D8         -- D8正价课GMV
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D10,
                      0))                                                                         AS pay_sum_D10         -- D10正价课GMV
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D14,
                      0))                                                                         AS pay_sum_D14         -- D14正价课GMV

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
   -- 腾讯渠道素材id有多个，将素材id炸开
   , mid1 AS
    (SELECT *
     FROM mid
              LATERAL VIEW EXPLODE(SPLIT(sucai_id, ',')) exploded_sucaiids AS sucaiid)

   -- 炸开后会有重复维度，再聚合处理下
   , mid2 AS
    (SELECT d_date
          , ad_department
          , cost_id
          , ad_id
          , sucaiid
          , SUM(submit_num)         AS submit_num
          , SUM(payment_num)        AS payment_num
          , SUM(user_num)           AS user_num          -- 例子数
          , SUM(wx_num)             AS wx_num            -- 加微uv
          , SUM(wx_active_num)      AS wx_active_num     -- 主动加微uv
          , SUM(collect_num)        AS collect_num       -- 填问卷uv
          , SUM(collect_active_num) AS collect_active_num-- 主动问卷uv
          , SUM(pay_user_num)       AS pay_user_num      -- 购买正价课uv
          , SUM(pay_num)            AS pay_num           -- 正价课订单数
          , SUM(pay_sum)            AS pay_sum           -- 正价课GMV
          , SUM(pay_num_d4)         AS pay_num_d4        -- 正价课订单数
          , SUM(pay_sum_d4)         AS pay_sum_d4        -- 正价课GMV
          , SUM(pay_sum_D7)         AS pay_sum_D7
          , SUM(pay_sum_D8)         AS pay_sum_D8
          , SUM(pay_sum_D10)        AS pay_sum_D10
          , SUM(pay_sum_D14)        AS pay_sum_D14
          , SUM(ifcome0)            AS ifcome0           -- 导学课到课
          , SUM(ifok0)              AS ifok0             -- 导学课完课
          , SUM(ifcome1)            AS ifcome1           -- D1到课
          , SUM(ifok1)              AS ifok1             -- D1完课
          , SUM(ifcome2)            AS ifcome2
          , SUM(ifok2)              AS ifok2
          , SUM(ifcome3)            AS ifcome3
          , SUM(ifok3)              AS ifok3
          , SUM(ifcome4)            AS ifcome4
          , SUM(ifok4)              AS ifok4
          , SUM(ifcome5)            AS ifcome5
          , SUM(ifok5)              AS ifok5
     FROM mid1
     GROUP BY d_date
            , ad_department
            , cost_id
            , ad_id
            , sucaiid)

--关联消耗和聚合的用户数
   , r1 AS
    (SELECT COALESCE(a.d_date, b.d_date)                            AS d_date
          , COALESCE(a.cat, '无')                                   AS cat
          , if(COALESCE(b.ad_department,COALESCE(a.ad_department,'信息流一部'),'无')='信息流四部','信息流一部',COALESCE(b.ad_department,COALESCE(a.ad_department,'信息流一部'),'无'))  AS ad_department
          , COALESCE(a.platform, '无')                              AS platform_name
          , COALESCE(a.pos, '无')                                   AS pos
          , COALESCE(a.price, '无')                                 AS price
          , COALESCE(a.link_type_v2, '无')                          AS link_type_v2
          , COALESCE(a.mobile, '无')                                AS mobile
          , COALESCE(a.agent, '无')                                 AS agent
          , CONCAT('id_', COALESCE(a.cost_id, 'other'))             AS cost_id
          , CONCAT('id_', COALESCE(a.ad_id, 'other'))               AS ad_id
          , CONCAT('id_', COALESCE(a.sucai_id, b.sucaiid, 'other')) AS sucai_id
          , COALESCE(a.sucai_name, '无')                            AS sucai_name
          , a.sucai_type
          , NVL(cost, 0)                                            AS cost
          , NVL(cost_real, 0)                                       AS cost_real
          , NVL(submit_num, 0)                                      AS submit_num
          , NVL(payment_num, 0)                                     AS payment_num
          , NVL(user_num, 0)                                        AS user_num
          , NVL(wx_num, 0)                                          AS wx_num
          --, NVL(wx_active_num, 0)                                   AS wx_active_num
          , NVL(collect_num, 0)                                     AS collect_num
          --, NVL(collect_active_num, 0)                              AS collect_active_num
          , NVL(pay_num_D4, 0)                                      AS pay_num_D4
          , NVL(pay_sum_D4, 0)                                      AS pay_sum_D4
          , NVL(pay_sum_D4 / cost_real, 0)                          AS roi_D4
          , NVL(pay_sum_D7, 0)                                      AS pay_sum_D7
          , NVL(pay_sum_D8, 0)                                      AS pay_sum_D8
          , NVL(pay_sum_D10, 0)                                      AS pay_sum_D10
          , NVL(pay_sum_D14, 0)                                      AS pay_sum_D14
          , NVL(pay_user_num, 0)                                    AS pay_user_num
          , NVL(pay_num, 0)                                         AS pay_num
          , NVL(pay_sum, 0)                                         AS pay_sum
          , NVL(pay_sum / cost_real, 0)                             AS roi
          , NVL(pay_num / user_num * 100, 0)                        AS conv_rate
          , NVL(cost_real / user_num, 0)                            AS cac
          , NVL(wx_num / payment_num * 100, 0)                      AS wx_rate
          , NVL(wx_active_num / payment_num * 100, 0)               AS wx_active_rate
          , NVL(collect_num / user_num * 100, 0)                    AS collect_rate
          , NVL(collect_active_num / user_num * 100, 0)             AS collect_active_rate
          , NVL(ifcome0 / user_num * 100, 0)                           ifcome0_rate
          , NVL(ifok0 / user_num * 100, 0)                             ifok0_rate
          , NVL(ifcome1 / user_num * 100, 0)                           ifcome1_rate
          , NVL(ifok1 / user_num * 100, 0)                             ifok1_rate
          , NVL(ifcome2 / user_num * 100, 0)                           ifcome2_rate
          , NVL(ifok2 / user_num * 100, 0)                             ifok2_rate
          , NVL(ifcome3 / user_num * 100, 0)                           ifcome3_rate
          , NVL(ifok3 / user_num * 100, 0)                             ifok3_rate
          , NVL(ifcome4 / user_num * 100, 0)                           ifcome4_rate
          , NVL(ifok4 / user_num * 100, 0)                             ifok4_rate
          , NVL(ifcome5 / user_num * 100, 0)                           ifcome5_rate
          , NVL(ifok5 / user_num * 100, 0)                             ifok5_rate
          , NVL(wx_active_num, 0)                                   AS wx_active_num
          , NVL(collect_active_num, 0)                              AS collect_active_num
          , NVL(ifcome0, 0)                                         AS ifcome0
          , NVL(ifok0, 0)                                           AS ifok0
          , NVL(ifcome1, 0)                                         AS ifcome1
          , NVL(ifok1, 0)                                           AS ifok1
          , NVL(ifcome2, 0)                                         AS ifcome2
          , NVL(ifok2, 0)                                           AS ifok2
          , NVL(ifcome3, 0)                                         AS ifcome3
          , NVL(ifok3, 0)                                           AS ifok3
          , NVL(ifcome4, 0)                                         AS ifcome4
          , NVL(ifok4, 0)                                           AS ifok4
          , NVL(ifcome5, 0)                                         AS ifcome5
          , NVL(ifok5, 0)                                           AS ifok5
          , NVL(view_count, 0)                                      AS view_count--曝光次数
          , NVL(valid_click_count, 0)                               AS valid_click_count --点击次数
          , NVL(conversions_count, 0)                               AS conversions_count --目标转化量
          , NVL(conversions_rate, 0)                                AS conversions_rate --目标转化率
          , NVL(conversions_cost, 0)                                AS conversions_cost--目标转化成本
          , NVL(video_outer_play_count, 0)                          AS video_outer_play_count--有效播放数
          , NVL(video_outer_play_rate, 0)                           AS video_outer_play_rate--有效播放率
          , NVL(video_outer_play_cost, 0)                           AS video_outer_play_cost--有效播放成本
          , NVL(video_outer_play_time_count, 0)                     AS video_outer_play_time_count--平均有效播放时长
          , NVL(video_outer_play10_count, 0)                        AS video_outer_play10_count--10%进度播放次数
          , NVL(video_outer_play25_count, 0)                        AS video_outer_play25_count--25%进度播放次数
          , NVL(video_outer_play50_count, 0)                        AS video_outer_play50_count--50%进度播放次数
          , NVL(video_outer_play75_count, 0)                        AS video_outer_play75_count--75%进度播放次数
          , NVL(video_outer_play90_count, 0)                        AS video_outer_play90_count--90%进度播放次数
          , NVL(video_outer_play95_count, 0)                        AS video_outer_play95_count--95%进度播放次数
          , NVL(video_outer_play100_count, 0)                       AS video_outer_play100_count--100%进度播放次数
          , NVL(video_outer_play3s_count, 0)                        AS video_outer_play3s_count--3s播放完成次数
          , NVL(video_outer_play5s_count, 0)                        AS video_outer_play5s_count--5s播放完成次数
          , NVL(video_outer_play7s_count, 0)                        AS video_outer_play7s_count--7s播放完成次数
     FROM cost_api a
              LEFT JOIN mid2 b
                        ON a.d_date = b.d_date
                            -- and a.cat = b.cat
-- and a.platform = b.platform_name
-- and a.pos = b.pos
-- and a.price = b.price
-- and a.link_type_v2 = b.link_type_v2
-- and a.mobile = b.mobile
                            AND a.cost_id = b.cost_id
                            AND a.ad_id = b.ad_id
                            AND a.sucai_id = b.sucaiid)
INSERT
OVERWRITE
TABLE
app.c_app_course_daily_tencentsucai_dashboard
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
     , sucai_type
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
     , pay_sum_D7
     ,pay_sum_D8
     ,pay_sum_D10
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
     , view_count
     , valid_click_count
     , conversions_count
     , conversions_rate
     , conversions_cost
     , video_outer_play_count
     , video_outer_play_rate
     , video_outer_play_cost
     , video_outer_play_time_count
     , video_outer_play10_count
     , video_outer_play25_count
     , video_outer_play50_count
     , video_outer_play75_count
     , video_outer_play90_count
     , video_outer_play95_count
     , video_outer_play100_count
     , video_outer_play3s_count
     , video_outer_play5s_count
     , video_outer_play7s_count
FROM r1
WHERE platform_name IN ('腾讯')
  AND pos IN ('腾讯公众号', '腾讯朋友圈', '腾讯视频号信息流','腾讯视频号信息流(北京)','腾讯朋友圈(北京)');

DFS -touchz /dw/app/c_app_course_daily_tencentsucai_dashboard/dt=${datebuf}/_SUCCESS;

