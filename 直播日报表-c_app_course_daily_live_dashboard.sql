-- drop table app.c_app_course_roi_behavior_add_olduser_h5id;
SET mapred.job.name="c_app_course_daily_live_dashboard#${datebuf}";
USE app;
CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_course_daily_live_dashboard
(
    d_date              string COMMENT '日期',
    grouptype           string COMMENT '分组',
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
    pay_num             int COMMENT '正价课订单数(单)',
    pay_sum             float COMMENT '正价课GMV(元)',
    roi                 float COMMENT 'ROI=正价课GMV/实际支出',
    cac                 float COMMENT '账面CAC',
    cac_real            float COMMENT '实际CAC',
    conv_rate           float COMMENT '转化率=正价课订单数/例子数(%)',
    wx_rate             float COMMENT '加微率=加微例子数/例子数(%)',
    wx_active_rate      float COMMENT '主动加微率=主动加微例子数/例子数(%)',
    collect_rate        float COMMENT '问卷率=填问卷例子数/例子数(%)',
    collect_active_rate float COMMENT '主动问卷率=主动填问卷例子数/例子数(%)',
    d0_come_rate        float COMMENT '导学课到课率',
    d0_ok_rate          float COMMENT '导学课完课率',
    d1_come_rate        float COMMENT 'D1到课率',
    d1_ok_rate          float COMMENT 'D1完课率',
    d2_come_rate        float COMMENT 'D2到课率',
    d2_ok_rate          float COMMENT 'D2完课率',
    d3_come_rate        float COMMENT 'D3到课率',
    d3_ok_rate          float COMMENT 'D3完课率',
    d4_come_rate        float COMMENT 'D4到课率',
    d4_ok_rate          float COMMENT 'D4完课率',
    d5_come_rate        float COMMENT 'D5到课率',
    d5_ok_rate          float COMMENT 'D5完课率',
    m_olduv_rate        float COMMENT '重复例子占比(%)',
    m_olduv_rate30      float COMMENT '重复例子占比(30天)(%)'
)
    COMMENT '培训主题数仓-app层-直播日报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_daily_live_dashboard';


SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=2048;
SET mapreduce.reduce.memory.mb=2048;
-- SET hive.execution.engine=tez;


-- api抓取的消耗数据
CREATE TEMPORARY TABLE cost_api AS
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
     FROM da.da_course_daily_cost_by_adid
     WHERE dt = '${datebuf}'
       AND d_date <= '${datebuf}');


CREATE TEMPORARY TABLE olduser AS
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
     GROUP BY member_id, goods_name);


CREATE TEMPORARY TABLE olduser30 AS
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
     GROUP BY member_id, goods_name);


CREATE TEMPORARY TABLE mid AS
    ( -- 按日+adid维度聚合用户
        SELECT TO_DATE(created_at)                                                                AS d_date
             , cat
             , ad_department
             , platform
             , platform_name
             , price
             , link_type_v1
             , link_type_v2
             , mobile
             , pos
             , h5_id                                                                                                    -- 多了h5_id
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
             , COUNT(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, olduser.oldid,
                        NULL))                                                                    AS olduser_num        -- 老用户例子数
             , COUNT(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, olduser30.oldid,
                        NULL))                                                                    AS olduser_num30      -- 老用户例子数30
             , SUM(COUNT(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, member_id,
                            NULL)))
                   OVER (PARTITION BY TO_DATE(created_at)
                       , cat
                       , platform
                       , platform_name
                       , price
                       , link_type_v1
                       , link_type_v2
                       , mobile
                       , pos,cost_id,ad_id)                                                       AS user_num_all       -- 日+计划粒度的总用户 -- 250117 增加维度，解决消耗和1416不一致问题
             , SUM(COUNT(member_id))
                   OVER (PARTITION BY TO_DATE(created_at)
                       , cat
                       , platform
                       , platform_name
                       , price
                       , link_type_v1
                       , link_type_v2
                       , mobile
                       , pos,cost_id,ad_id)                                                       AS submit_num_all     -- 250120 日+计划粒度的总提交表单用户

        FROM (SELECT * FROM dws.dws_sale_camping_user_day WHERE dt = '${datebuf}') user_info
                 LEFT JOIN olduser
                           ON user_info.member_id = olduser.oldid AND user_info.goods_name = olduser.goods_name
                 LEFT JOIN olduser30
                           ON user_info.member_id = olduser30.oldid AND user_info.goods_name = olduser30.goods_name
        GROUP BY TO_DATE(created_at)
               , cat
               , ad_department
               , platform
               , platform_name
               , price
               , link_type_v1
               , link_type_v2
               , mobile
               , pos
               , h5_id -- 多了h5_id
               , cost_id
               , ad_id -- 2012 5 1234    2012 6 1234

    );
--这里用户的度量是按天、H5ID、adid的粒度算的

--关联消耗和聚合的用户数
CREATE TEMPORARY TABLE r1 AS
    (SELECT COALESCE(a.d_date, b.d_date)                           AS d_date
          , COALESCE(b.cat, a.cat, '无')                           AS cat
          , b.ad_department
          , COALESCE(b.platform_name, a.platform, '无')            AS platform_name
          , COALESCE(b.price, a.price, '无')                       AS price
          -- , COALESCE(CONCAT(a.price, a.mobile), '无')           AS link_type_v1
          , COALESCE(b.link_type_v2, a.link_type_v2, '无')         AS link_type_v2
          , COALESCE(b.mobile, a.mobile, '无')                     AS mobile
          , COALESCE(b.pos, a.pos, '无')                           AS pos
          , COALESCE(b.h5_id, '无')                                AS h5_id
          , COALESCE(a.agent, '无')                                AS agent
          , CONCAT('id_', COALESCE(a.cost_id, b.cost_id, 'other')) AS cost_id
          , COALESCE(a.advertiser_name, '')                        AS cost_name
          , CONCAT('id_', COALESCE(a.ad_id, b.ad_id, 'other'))     AS ad_id
          , COALESCE(cost * user_num / user_num_all, cost * submit_num / submit_num_all,
                     cost)                                         AS cost      -- 20240823 tangwenqi 关联不上的消耗，取消耗，不做拆分处理
          , COALESCE(cost_real * user_num / user_num_all, cost_real * submit_num / submit_num_all,
                     cost_real)                                    AS cost_real -- 20240823 tangwenqi 关联不上的消耗，取消耗，不做拆分处理
          , NVL(submit_num, 0)                                     AS submit_num
          , NVL(payment_num, 0)                                    AS payment_num
          , NVL(user_num, 0)                                       AS user_num
          , NVL(olduser_num, 0)                                    AS olduser_num
          , NVL(olduser_num30, 0)                                  AS olduser_num30
          , NVL(wx_num, 0)                                         AS wx_num
          , NVL(wx_active_num, 0)                                  AS wx_active_num
          , NVL(collect_num, 0)                                    AS collect_num
          , NVL(collect_active_num, 0)                             AS collect_active_num
          , NVL(pay_user_num, 0)                                   AS pay_user_num
          , NVL(pay_num, 0)                                        AS pay_num
          , NVL(pay_sum, 0)                                        AS pay_sum
          , NVL(ifcome0, 0)                                        AS ifcome0
          , NVL(ifok0, 0)                                          AS ifok0
          , NVL(ifcome1, 0)                                        AS ifcome1
          , NVL(ifok1, 0)                                          AS ifok1
          , NVL(ifcome2, 0)                                        AS ifcome2
          , NVL(ifok2, 0)                                          AS ifok2
          , NVL(ifcome3, 0)                                        AS ifcome3
          , NVL(ifok3, 0)                                          AS ifok3
          , NVL(ifcome4, 0)                                        AS ifcome4
          , NVL(ifok4, 0)                                          AS ifok4
          , NVL(ifcome5, 0)                                        AS ifcome5
          , NVL(ifok5, 0)                                          AS ifok5
     FROM cost_api a
              FULL JOIN mid b
                        ON a.d_date = b.d_date
                            AND a.cat = b.cat
                            AND a.platform = b.platform_name
                            AND a.price = b.price
--and a.link_type_v1 = b.link_type_v1
                            AND a.link_type_v2 = b.link_type_v2
                            AND a.mobile = b.mobile
                            AND a.pos = b.pos
                            AND a.cost_id = b.cost_id
                            AND a.ad_id = b.ad_id);



CREATE TEMPORARY TABLE result_adid AS
    (SELECT d_date
          , CASE
                WHEN GROUPING__ID = 7 THEN '0_H5ID'
                WHEN GROUPING__ID = 1 THEN '1_账户'
                WHEN GROUPING__ID = 0 THEN '2_计划'
            END                                                      grouptype
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
          , ad_id
          , SUM(cost)                                             AS cost                -- 账面消耗
          , SUM(cost_real)                                        AS cost_real           --实际消耗
          , SUM(submit_num)                                       AS submit_num          --提交
          , SUM(payment_num)                                      AS payment_num         --支付
          , SUM(user_num)                                         AS user_num            --例子数
          , SUM(wx_num)                                           AS wx_num              --加微数
          --,sum(collect_num) as collect_num --问卷数
          -- ,sum(pay_user_num) as pay_user_num --购买正价课例子数
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
     FROM r1
     --where cat is not null
--and cat <> '无'
     GROUP BY d_date
            , cat
            , ad_department
            , platform_name
            , pos
            , h5_id -- add
            , price
            , link_type_v2
            , mobile
            , agent
            , cost_id
            , ad_id
         GROUPING SETS (
            (d_date, cat, ad_department, platform_name, pos, h5_id, price
            , link_type_v2, mobile)
            , (d_date, cat, ad_department, platform_name, pos, h5_id, price
            , link_type_v2, mobile, agent
            , cost_id)
            , (d_date, cat, ad_department, platform_name, pos, h5_id, price
            , link_type_v2, mobile, agent
            , cost_id
            , ad_id)
         ));



INSERT
    OVERWRITE
    TABLE app.c_app_course_daily_live_dashboard
    PARTITION
    (dt = '${datebuf}')
SELECT d_date
     , grouptype
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
     , 'ALL-汇总'
     , 'ALL-汇总'
     , cost
     , cost_real
     , submit_num
     , payment_num
     , user_num
     , wx_num
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
--and link_type_v1!='无'
--and h5_id!='未知'
--and d_date>='2024-08-01'
ORDER BY grouptype
       , cat
       , ad_department
       , platform_name
       , pos
       , h5_id
       , price
       , link_type_v2
       , mobile
       , name
       , costid
       , adid;