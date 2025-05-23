-- tittle
-- 投放日期 分组类型 
-- 品类 渠道 价格 新链路 手机号 版位 代理 账户id 计划id 
-- 账面支出 实际支出
-- 表单填写uv 支付成功uv 例子数(分配销售uv) 加微uv 问卷填写uv 正价课订单数 正价课金额 ROI=正价课金额/实际支出 转化率=正价课订单数/分配销售uv 实际CAC=实际支出/分配销售uv 
-- 加微率 主动加微率 问卷率 主动问卷填写率 导学课到课率 导学课完课率 D1-D5到课完课率
-- 主动加微uv 主动问卷填写uv 导学课到课uv 导学课完课uv D1-D5到课完课uv
-- #账面CAC=账面支出/分配销售uv #加微CAC=实际支出/加微uv #主动加微uv #主动问卷填写uv  #D1-D7到课完课作业uv #素材id #购买正价课uv
-- 回传预留: 提交表单回传 支付成功回传 加微成功回传 问卷填写回传 导学课到课回传 D1到课回传

-- 1_品类
-- 2_品类x渠道
-- 3_品类x渠道x版位
-- 4_品类x价格
-- 5_品类x渠道x版位x价格
-- 6_品类x渠道x版位x价格x新链路
-- 7_品类x渠道x版位x价格x新链路x手机号
-- 8_品类x渠道x版位x价格x新链路x手机号x代理
-- 9_品类x渠道x版位x价格x新链路x手机号x代理x账户
-- 10_品类x渠道x版位x价格x新链路x手机号x代理x账户x计划

-- 建表思路：消耗和用户通过渠道版位等维度，最细到计划进行full join，按天统计例子数、加微、转化、到课等指标
SET mapred.job.name="c_app_course_daily_ad_dashboard#${datebuf}";
USE app;
CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_course_daily_ad_dashboard
(
    d_date              string COMMENT '日期',
    grouptype           string COMMENT '分组类型',
    is_abroad           varchar(20) COMMENT '海内外',
    cat                 STRING COMMENT '品类',
    ad_department       STRING COMMENT '投放部门',
    platform_name       string COMMENT '渠道 ',
    pos                 string COMMENT '版位 ',
    price               string COMMENT '价格 ',
    link_type_v2        string COMMENT '链路类型(新) ',
    mobile              string COMMENT '收集手机号',
    agent               string COMMENT '代理',
    cost_id             string COMMENT '账户id',
    ad_id               string COMMENT '计划id',
    cost                float COMMENT '账面消耗(元)',
    cost_real           float COMMENT '实际消耗(元)',
    submit_num          int COMMENT '表单填写例子数(个)',
    payment_num         int COMMENT '支付成功例子数(个)',
    user_num            int COMMENT '例子数(个)',
    wx_num              int COMMENT '加微例子数(个)',
    collect_num         int COMMENT '填问卷例子数(个)',
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
    COMMENT '培训主题数仓-投放数据日报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_daily_ad_dashboard';

SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;

-- api抓取的消耗数据
WITH cost_api AS
    (SELECT d_date          -- 投放日期
          , cat             -- 品类
          , platform-- 渠道
          , price           -- 价格
          , mobile-- 收集手机号
          , link_type_v2    -- 链路类型(新),获客助手等
          , pos             -- 版位
          , cost_id         -- 账户id
          , advertiser_name -- 账户名称
          , agent
          , ad_id           -- 计划id
          , cost            -- 账面消耗
          , cost_real       -- 实际消耗
         ,  ad_department
     FROM da.da_course_daily_cost_by_adid
     WHERE dt = '${datebuf}' -- 20240821改为单pt记录所有数据
--     and dt >= '2024-07-06'
       AND d_date <= '${datebuf}'
--     and d_date >= '2024-07-06'
    )

   , mid AS
    ( -- 按日+adid维度聚合用户
        SELECT TO_DATE(created_at)                                                                AS d_date
             , cat
             , is_abroad
             , ad_department
             , platform
             , platform_name
             , price
             , link_type_v2
             , mobile
             , pos
             , cost_id
             , ad_id
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
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, cac,
                      0))                                                                         AS cost
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, cac_real,
                      0))                                                                         AS cost_real
        FROM dws.dws_sale_camping_user_day
        WHERE dt = '${datebuf}'
          AND (platform_name != '小糖私域' OR pos != '私域群活码') -- 20241113 剔除私域群活码
        GROUP BY TO_DATE(created_at)
               , cat
               , is_abroad
               , ad_department
               , platform
               , platform_name
               , price
               , link_type_v2
               , mobile
               , pos
               , cost_id
               , ad_id)
--关联消耗和聚合的用户数
   , r1 AS
    (SELECT COALESCE(a.d_date, b.d_date)                                                                    AS d_date
          , COALESCE(b.cat, a.cat, '无')                                                                    AS cat
          , COALESCE(b.is_abroad, IF(a.pos IN ('脸书'), '海外', '国内'))                                    AS is_abroad
          , coalesce(b.ad_department,a.ad_department)                                                    AS ad_department
          , COALESCE(b.platform_name, a.platform, '无')                                                     AS platform_name
          , COALESCE(b.price, a.price, '无')                                                                AS price
          , COALESCE(b.link_type_v2, a.link_type_v2, '无')                                                  AS link_type_v2
          , COALESCE(b.mobile, a.mobile, '无')                                                              AS mobile
          , COALESCE(b.pos, a.pos, '无')                                                                    AS pos
          , COALESCE(a.agent, '无')                                                                         AS agent
          , CONCAT('id_', COALESCE(a.cost_id, b.cost_id, 'other'))                                          AS cost_id
          , COALESCE(a.advertiser_name, '')                                                                 AS cost_name
          , CONCAT('id_', COALESCE(a.ad_id, b.ad_id, 'other'))                                              AS ad_id
          , CASE WHEN b.platform_name IN ('非标', '小糖私域') THEN b.cost ELSE NVL(a.cost, 0) END           AS cost
          , CASE WHEN b.platform_name IN ('非标', '小糖私域') THEN b.cost_real ELSE NVL(a.cost_real, 0) END AS cost_real
          , NVL(submit_num, 0)                                                                              AS submit_num
          , NVL(payment_num, 0)                                                                             AS payment_num
          , NVL(user_num, 0)                                                                                AS user_num
          , NVL(wx_num, 0)                                                                                  AS wx_num
          , NVL(wx_active_num, 0)                                                                           AS wx_active_num
          , NVL(collect_num, 0)                                                                             AS collect_num
          , NVL(collect_active_num, 0)                                                                      AS collect_active_num
          , NVL(pay_user_num, 0)                                                                            AS pay_user_num
          , NVL(pay_num, 0)                                                                                 AS pay_num
          , NVL(pay_sum, 0)                                                                                 AS pay_sum
          , NVL(ifcome0, 0)                                                                                 AS ifcome0
          , NVL(ifok0, 0)                                                                                   AS ifok0
          , NVL(ifcome1, 0)                                                                                 AS ifcome1
          , NVL(ifok1, 0)                                                                                   AS ifok1
          , NVL(ifcome2, 0)                                                                                 AS ifcome2
          , NVL(ifok2, 0)                                                                                   AS ifok2
          , NVL(ifcome3, 0)                                                                                 AS ifcome3
          , NVL(ifok3, 0)                                                                                   AS ifok3
          , NVL(ifcome4, 0)                                                                                 AS ifcome4
          , NVL(ifok4, 0)                                                                                   AS ifok4
          , NVL(ifcome5, 0)                                                                                 AS ifcome5
          , NVL(ifok5, 0)                                                                                   AS ifok5
     FROM cost_api a
              FULL JOIN mid b
                        ON a.d_date = b.d_date
                            AND a.cat = b.cat
                            AND a.platform = b.platform_name
                            AND a.pos = b.pos
                            AND a.price = b.price
                            AND a.link_type_v2 = b.link_type_v2
                            AND a.mobile = b.mobile
                            AND a.cost_id = b.cost_id
                            AND a.ad_id = b.ad_id)

INSERT
OVERWRITE
TABLE
app.c_app_course_daily_ad_dashboard
PARTITION
(
dt = '${datebuf}'
)
SELECT d_date
     , CASE GROUPING__ID
           WHEN 1023 THEN '1_海内外'
           WHEN 511 THEN '2_海内外x品类'
           WHEN 127 THEN '3_海内外x品类x投放部门x渠道'
           WHEN 63 THEN '4_海内外x品类x投放部门x渠道x版位'
           WHEN 479 THEN '5_海内外x品类x价格'
           WHEN 31 THEN '6_海内外x品类x投放部门x渠道x版位x价格'
           WHEN 15 THEN '7_海内外x品类x投放部门x渠道x版位x价格x新链路'
           WHEN 7 THEN '8_海内外x品类x投放部门x渠道x版位x价格x新链路x手机号'
           WHEN 3 THEN '9_海内外x品类x投放部门x渠道x版位x价格x新链路x手机号x代理'
           WHEN 1 THEN '10_海内外x品类x投放部门x渠道x版位x价格x新链路x手机号x代理x账户'
           WHEN 0 THEN '11_海内外x品类x投放部门x渠道x版位x价格x新链路x手机号x代理x账户x计划'
    END                                                      AS grouptype
     , is_abroad
     , NVL(cat, 'ALL-汇总')                                  AS cat
     , NVL(ad_department, 'ALL-汇总')                        AS ad_department
     , NVL(platform_name, 'ALL-汇总')                        AS platform_name
     , NVL(pos, 'ALL-汇总')                                  AS pos
     , NVL(price, 'ALL-汇总')                                AS price
     , NVL(link_type_v2, 'ALL-汇总')                         AS link_type_v2
     , NVL(mobile, 'ALL-汇总')                               AS mobile
     , NVL(agent, 'ALL-汇总')                                AS agent
     , NVL(cost_id, 'ALL-汇总')                              AS cost_id
     , NVL(ad_id, 'ALL-汇总')                                AS ad_id
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
       , is_abroad
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
    GROUPING SETS (
       (d_date, is_abroad)                                                                               -- 110000000000
       , (d_date, is_abroad, cat)                                                                        -- 111000000000
       , (d_date, is_abroad, cat, ad_department, platform_name)                                          -- 111110000000
       , (d_date, is_abroad, cat, ad_department, platform_name, pos)                                     -- 111111000000
       , (d_date, is_abroad, cat, price)                                                                 -- 111000100000
       , (d_date, is_abroad, cat, ad_department, platform_name, pos, price)                              -- 111111100000
       , (d_date, is_abroad, cat, ad_department, platform_name, pos, price, link_type_v2)                -- 111111110000
       , (d_date, is_abroad, cat, ad_department, platform_name, pos, price, link_type_v2, mobile)        -- 111111111000
       , (d_date, is_abroad, cat, ad_department, platform_name, pos, price, link_type_v2, mobile, agent) -- 111111111100
       , (d_date, is_abroad, cat, ad_department, platform_name, pos, price, link_type_v2, mobile, agent
       , cost_id)                                                                                        -- 111111111110
       , (d_date, is_abroad, cat, ad_department, platform_name, pos, price, link_type_v2, mobile, agent, cost_id
       , ad_id)                                                                                          -- 111111111111
    )
;