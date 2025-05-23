-- tittle
-- 期次 分组类型
-- 部门 组 销售
-- 账面支出 实际支出
-- 例子数 加微例子数 填问卷例子数 D4订单数 D4GMV D4ROI 正价课例子数 正价课订单数 正价课金额 ROI=正价课金额/实际支出 转化率=正价课订单数/例子数 实际CAC=实际支出/例子数
-- 主动加微率 主动问卷填写率 导学课到课率 导学课完课率 D1-D5到课完课率
-- 主动加微例子数 主动问卷填写例子数 导学课到课例子数 导学课完课例子数 D1-D5到课完课例子数

-- 分组类型:
-- 1_期次
-- 2_期次x部门
-- 3_期次x部门x组
-- 4_期次x部门x组x销售
-- 建表思路：训练营学员关联正价课购买情况，分摊到销售身上。（空耗分不到销售身上）
-- 已按照期次报表兼容人工导入的消耗
SET mapred.job.name="c_app_course_period_sales_dashboard#${datebuf}";
USE app;
CREATE TABLE IF NOT EXISTS app.c_app_course_period_sales_dashboard
(
    d_date              STRING COMMENT '日期',
    is_abroad           STRING COMMENT '海外/国内',
    cat                 STRING COMMENT '品类',
    goods_name          STRING COMMENT '期次',
    grouptype           STRING COMMENT '分组类型',
    department          STRING COMMENT '部门',
    user_group          STRING COMMENT '组',
    sales_name          STRING COMMENT '销售',
    sales_sop_type      STRING COMMENT 'AI销售SOP类型',
    user_num            int COMMENT '例子数(个)',
    wx_num              int COMMENT '加微例子数(个)',
    collect_num         int COMMENT '填问卷例子数(个)',
    pay_user_num_D4     int COMMENT 'D4正价课购买例子数（个）',
    pay_num_D4          float COMMENT 'D4正价课订单数(单)',
    pay_sum_D4          float COMMENT 'D4正价课GMV(元)',
    pay_num_D4_24h      float COMMENT 'D4正价课订单数(单)(24H)',
    pay_sum_D4_24h      float COMMENT 'D4正价课GMV(24H)',
    pay_num_D5          float COMMENT 'D5正价课订单数(单)',
    pay_sum_D5          float COMMENT 'D5正价课GMV(元)',
    pay_num_D6          float COMMENT 'D6正价课订单数(单)',
    pay_sum_D6          float COMMENT 'D6正价课GMV(元)',
    roi_D4              float COMMENT 'D4ROI',
    roi_D5              float COMMENT 'D5ROI',
    roi_D6              float COMMENT 'D6ROI',
    pay_num_D7          float COMMENT 'D7正价课订单数(单)',
    pay_sum_D7          float COMMENT 'D7正价课GMV(元)',
    pay_user_num_D8     int COMMENT 'D8正价课购买例子数（个）',
    pay_num_D8          float COMMENT 'D8正价课订单数(单)',
    pay_sum_D8          float COMMENT 'D8正价课GMV(元)',
    pay_num_D14         float COMMENT 'D14正价课订单数(单)',
    pay_sum_D14         float COMMENT 'D14正价课GMV(元)',
    pay_user_num        int COMMENT '购买正价课例子数(个)',
    pay999_user_num     int COMMENT '购买999正价课例子数(个)',
    pay1980_user_num    int COMMENT '购买1980正价课例子数(个)',
    pay_num             float COMMENT '正价课订单数(单)',
    pay_sum             float COMMENT '正价课GMV(元)',
    roi_D7              float COMMENT 'D7ROI',
    roi_D8              float COMMENT 'D8ROI',
    roi_D14             float COMMENT 'D14ROI',
    roi                 float COMMENT 'ROI',
    conv_rate           float COMMENT '转化率(%)',
    cac                 float COMMENT 'CAC(元/个)',
    wx_rate             float COMMENT '加微率(%)',
    wx_active_rate      float COMMENT '主动加微率(%)',
    collect_rate        float COMMENT '问卷率(%)',
    collect_active_rate float COMMENT '主动问卷率(%)',
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
    permission          STRING COMMENT '权限用户'
)
    COMMENT '培训主题数仓-销售数据期次报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_period_sales_dashboard';

SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;


-- 用账户日CAC进行计算
WITH cost AS
    (SELECT cost_id
          , d_date
          , SUM(cost)      AS cost
          , SUM(cost_real) AS cost_real
     FROM da.da_course_daily_cost_by_adid
     WHERE dt = '${datebuf}'
     GROUP BY cost_id
            , d_date)
   , users AS -- 20240914 本地推直播:直播一组,腾讯视频号直播付费流:直播二组,千川直播:直播三组
    (SELECT a.member_id
          , a.cat
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
          , a.sales_sop_type
          , a.ifbuy
          , a.first_pay_time
          , a.pay_num
          , a.pay_sum
          , a.pay_user_num_D4
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
          , a.pay_user_num_D8
          , a.pay_num_D8
          , a.pay_sum_D8
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
          , a.is_abroad
     FROM dws.dws_sale_camping_user_day a
     WHERE a.dt = '${datebuf}'
       AND TO_DATE(CONCAT('20', SUBSTR(a.goods_name, 2, 2), '-', SUBSTR(a.goods_name, 4, 2), '-',
                          SUBSTR(a.goods_name, 6, 2))) >= '2024-05-01'
       AND a.created_at >= '2024-05-01'
       --这里先注释掉 背景：20250403 发现业务方有 期次包含“测试”二字 但是数据是需要的
--        AND a.goods_name NOT LIKE '%测试%'
       AND (a.platform_name != '小糖私域' OR a.pos != '私域群活码') -- 20241113 剔除私域群活码
    )
   , user_num AS
    (SELECT cost_id2
          , d_date
          , COUNT(*) num
     FROM users
     WHERE dt = '${datebuf}'
       AND member_status = 1
       AND trade_state IN ('SUCCESS', 'PREPARE')
       AND sales_id > 0
     -- and to_date(created_at) >= '2024-07-01'
     GROUP BY cost_id2
            , d_date)
   , cac AS
    (SELECT a.cost_id
          , a.d_date
          , NVL(a.cost / b.num, 0)      AS cac
          , NVL(a.cost_real / b.num, 0) AS cac_real
     FROM cost a
              LEFT JOIN user_num b
                        ON a.cost_id = b.cost_id2
                            AND a.d_date = b.d_date)
   , user_info AS
    ( -- 所有例子的投放属性&加微、问卷状态&购买正价课状态
        SELECT a.member_id
             , a.cat
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
             , a.department
             , a.user_group
             , SPLIT(REGEXP_REPLACE(TRIM(a.sales_name), '[0-9]号|[0-9]', ''), '（')[0]        AS sales_name
             , a.sales_sop_type
             , a.cost_id2
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
             , COALESCE(a.cac, d.cac, 0)                                                     AS cac
             , COALESCE(a.cac_real, d.cac_real, 0)                                           AS cac_real
             , a.pay_user_num_D4
             , a.pay_num_D4
             , a.pay_sum_D4
             -- 24.09.25汪娇需求新增截止到D4，24点的数据
             , a.pay_num_D4_24h
             , a.pay_sum_D4_24h
             , a.pay_num_D5
             , a.pay_sum_D5
             , a.pay_num_D6
             , a.pay_sum_D6
             , a.pay_num_D7
             , a.pay_sum_D7
             , a.pay_user_num_D8
             , a.pay_num_D8
             , a.pay_sum_D8
             , a.pay_num_D14
             , a.pay_sum_D14
             , a.is_abroad
        FROM users a
                 LEFT JOIN cac d -- 消耗数据
                           ON d.cost_id = a.cost_id2
                               AND d.d_date = a.d_date)
   , mid AS
    ( -- 按期次+投放属性聚合用户
        SELECT TO_DATE(created_at)                                                                AS d_date
             , is_abroad
             , cat
             , goods_name
             , department
             , user_group
             , NVL(sales_name, '无')                                                              AS sales_name
             , sales_sop_type
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


             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_user_num_D4,
                      0))                                                                         AS pay_user_num_D4    -- D4正价课例子数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D4,
                      0))                                                                         AS pay_num_D4         -- 正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D4,
                      0))                                                                         AS pay_sum_D4         -- 正价课GMV
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D4_24h,
                      0))                                                                         AS pay_num_D4_24h     -- 截止到D4的24点点正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D4_24h,
                      0))                                                                         AS pay_sum_D4_24h     -- 截止到D4的24点点正价课GMV


             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D5,
                      0))                                                                         AS pay_num_D5         -- D5正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D5,
                      0))                                                                         AS pay_sum_D5         -- D5正价课GMV

             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D6,
                      0))                                                                         AS pay_num_D6         -- D6正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D6,
                      0))                                                                         AS pay_sum_D6         -- D6正价课GMV


             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D7,
                      0))                                                                         AS pay_num_D7         -- D7正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D7,
                      0))                                                                         AS pay_sum_D7         -- D7正价课GMV

             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_user_num_D8,
                      0))                                                                         AS pay_user_num_D8    -- D8正价课例子数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D8,
                      0))                                                                         AS pay_num_D8         -- D8正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D8,
                      0))                                                                         AS pay_sum_D8         -- D8正价课GMV

             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D14,
                      0))                                                                         AS pay_num_D14        -- D14正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D14,
                      0))                                                                         AS pay_sum_D14        -- D14正价课GMV


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
        GROUP BY TO_DATE(created_at)
               , is_abroad
               , cat
               , goods_name
               , department
               , user_group
               , sales_name
               , sales_sop_type)

   , r1 AS
    (SELECT '${datebuf}'                                          AS d_date
          , is_abroad
          , cat
          , goods_name
          , CASE GROUPING__ID
                WHEN 15 THEN '1_海内外x品类x期次'
                WHEN 7 THEN '2_海内外x品类x期次x部门'
                WHEN 3 THEN '3_海内外x品类x期次x部门x组'
                WHEN 0 THEN '4_海内外x品类x期次x部门x组x销售xSOP'
            END                                                   AS grouptype
          -- ,GROUPING__ID
          , NVL(department, 'ALL-汇总')                           AS department
          , NVL(user_group, 'ALL-汇总')                           AS user_group
          , NVL(sales_name, 'ALL-汇总')                           AS sales_name
          , NVL(sales_sop_type, 'ALL-汇总')                       AS sales_sop_type
          , SUM(cost)                                             AS cost
          , SUM(cost_real)                                        AS cost_real
          , SUM(user_num)                                         AS user_num
          , SUM(wx_num)                                           AS wx_num
          , SUM(collect_num)                                      AS collect_num

          , SUM(pay_user_num_D4)                                  AS pay_user_num_D4
          , SUM(pay_num_D4)                                       AS pay_num_D4
          , SUM(pay_sum_D4)                                       AS pay_sum_D4
          , SUM(pay_num_D4_24h)                                   AS pay_num_D4_24h
          , SUM(pay_sum_D4_24h)                                   AS pay_sum_D4_24h

          , SUM(pay_num_D5)                                       AS pay_num_D5
          , SUM(pay_sum_D5)                                       AS pay_sum_D5

          , SUM(pay_num_D6)                                       AS pay_num_D6
          , SUM(pay_sum_D6)                                       AS pay_sum_D6
          , NVL(SUM(pay_sum_D4) / SUM(cost_real), 0)              AS roi_D4
          , NVL(SUM(pay_sum_D5) / SUM(cost_real), 0)              AS roi_D5
          , NVL(SUM(pay_sum_D6) / SUM(cost_real), 0)              AS roi_D6
          , SUM(pay_num_D7)                                       AS pay_num_D7
          , SUM(pay_sum_D7)                                       AS pay_sum_D7
          , SUM(pay_user_num_D8)                                  AS pay_user_num_D8
          , SUM(pay_num_D8)                                       AS pay_num_D8
          , SUM(pay_sum_D8)                                       AS pay_sum_D8
          , SUM(pay_num_D14)                                      AS pay_num_D14
          , SUM(pay_sum_D14)                                      AS pay_sum_D14
          , SUM(pay_user_num)                                     AS pay_user_num
          , SUM(pay999_user_num)                                  AS pay999_user_num
          , SUM(pay1980_user_num)                                 AS pay1980_user_num
          , SUM(pay_num)                                          AS pay_num
          , SUM(pay_sum)                                          AS pay_sum
          , NVL(SUM(pay_sum_D7) / SUM(cost_real), 0)              AS roi_D7
          , NVL(SUM(pay_sum_D8) / SUM(cost_real), 0)              AS roi_D8
          , NVL(SUM(pay_sum_D14) / SUM(cost_real), 0)             AS roi_D14
          , NVL(SUM(pay_sum) / SUM(cost_real), 0)                 AS roi
          , NVL(SUM(pay_num) / SUM(user_num) * 100, 0)            AS conv_rate
          , NVL(SUM(cost_real) / SUM(user_num), 0)                AS cac
          , NVL(SUM(wx_num) / SUM(payment_num) * 100, 0)          AS wx_rate
          , NVL(SUM(wx_active_num) / SUM(payment_num) * 100, 0)   AS wx_active_rate
          , NVL(SUM(collect_num) / SUM(user_num) * 100, 0)        AS collect_rate
          , NVL(SUM(collect_active_num) / SUM(user_num) * 100, 0) AS collect_active_rate
          , NVL(SUM(ifcome0) / SUM(user_num) * 100, 0)            AS ifcome0_rate
          , NVL(SUM(ifok0) / SUM(user_num) * 100, 0)              AS ifok0_rate
          , NVL(SUM(ifcome1) / SUM(user_num) * 100, 0)            AS ifcome1_rate
          , NVL(SUM(ifok1) / SUM(user_num) * 100, 0)              AS ifok1_rate
          , NVL(SUM(ifcome2) / SUM(user_num) * 100, 0)            AS ifcome2_rate
          , NVL(SUM(ifok2) / SUM(user_num) * 100, 0)              AS ifok2_rate
          , NVL(SUM(ifcome3) / SUM(user_num) * 100, 0)            AS ifcome3_rate
          , NVL(SUM(ifok3) / SUM(user_num) * 100, 0)              AS ifok3_rate
          , NVL(SUM(ifcome4) / SUM(user_num) * 100, 0)            AS ifcome4_rate
          , NVL(SUM(ifok4) / SUM(user_num) * 100, 0)              AS ifok4_rate
          , NVL(SUM(ifcome5) / SUM(user_num) * 100, 0)            AS ifcome5_rate
          , NVL(SUM(ifok5) / SUM(user_num) * 100, 0)              AS ifok5_rate
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
     FROM mid
     GROUP BY is_abroad
            , cat
            , goods_name
            , department
            , user_group
            , sales_name
            , sales_sop_type
         GROUPING SETS (
            (is_abroad, cat, goods_name)                                                       -- 0001111 15
            , (is_abroad, cat, goods_name, department)                                         -- 0000111 7
            , (is_abroad, cat, goods_name, department, user_group)                             -- 0000011 3
            , (is_abroad, cat, goods_name, department, user_group, sales_name, sales_sop_type) -- 0000000 0
         ))


INSERT
OVERWRITE
TABLE
app.c_app_course_period_sales_dashboard
PARTITION
(
dt = '${datebuf}'
)
SELECT a.d_date
     , a.is_abroad
     , a.cat
     , a.goods_name
     , a.grouptype
     , a.department
     , a.user_group
     , a.sales_name
     , a.sales_sop_type
     , a.user_num
     , a.wx_num
     , a.collect_num
     , a.pay_user_num_D4
     , a.pay_num_D4
     , a.pay_sum_D4
     , a.pay_num_D4_24h
     , a.pay_sum_D4_24h
     , a.pay_num_D5
     , a.pay_sum_D5
     , a.pay_num_D6
     , a.pay_sum_D6
     , a.roi_D4
     , a.roi_D5
     , a.roi_D6
     , a.pay_num_D7
     , a.pay_sum_D7
     , a.pay_user_num_D8
     , a.pay_num_D8
     , a.pay_sum_D8
     , a.pay_num_D14
     , a.pay_sum_D14
     , a.pay_user_num
     , a.pay999_user_num
     , a.pay1980_user_num
     , a.pay_num
     , a.pay_sum
     , a.roi_D7
     , a.roi_D8
     , a.roi_D14
     , a.roi
     , a.conv_rate
     , a.cac
     , a.wx_rate
     , a.wx_active_rate
     , a.collect_rate
     , a.collect_active_rate
     , a.ifcome0_rate
     , a.ifok0_rate
     , a.ifcome1_rate
     , a.ifok1_rate
     , a.ifcome2_rate
     , a.ifok2_rate
     , a.ifcome3_rate
     , a.ifok3_rate
     , a.ifcome4_rate
     , a.ifok4_rate
     , a.ifcome5_rate
     , a.ifok5_rate
     , a.wx_active_num
     , a.collect_active_num
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
     , CONCAT_WS(',', COLLECT_SET(pm.emails)) AS permission
FROM r1 a
         LEFT JOIN (SELECT is_abroad
                         , cat
                         , platform_name
                         , pos
                         , ad_department
                         , sale_department
                         , sop_type
                         , emails
                    FROM dws.dws_report_permission_day
                    WHERE dt = '${datebuf}'
                      AND report_id = '1423') pm
                   ON (pm.is_abroad = '全部' OR a.is_abroad = pm.is_abroad)
                       AND (pm.cat = '全部' OR a.cat = pm.cat)
                       AND (pm.sop_type = '全部' OR a.sales_sop_type = pm.sop_type)
                       AND (pm.sale_department = '全部' OR a.department = pm.sale_department)
GROUP BY a.d_date
       , a.is_abroad
       , a.cat
       , a.goods_name
       , a.grouptype
       , a.department
       , a.user_group
       , a.sales_name
       , a.sales_sop_type
       , a.user_num
       , a.wx_num
       , a.collect_num
       , a.pay_user_num_D4
       , a.pay_num_D4
       , a.pay_sum_D4
       , a.pay_num_D4_24h
       , a.pay_sum_D4_24h
       , a.pay_num_D5
       , a.pay_sum_D5
       , a.pay_num_D6
       , a.pay_sum_D6
       , a.roi_D4
       , a.roi_D5
       , a.roi_D6
       , a.pay_num_D7
       , a.pay_sum_D7
       , a.pay_user_num_D8
       , a.pay_num_D8
       , a.pay_sum_D8
       , a.pay_num_D14
       , a.pay_sum_D14
       , a.pay_user_num
       , a.pay999_user_num
       , a.pay1980_user_num
       , a.pay_num
       , a.pay_sum
       , a.roi_D7
       , a.roi_D8
       , a.roi_D14
       , a.roi
       , a.conv_rate
       , a.cac
       , a.wx_rate
       , a.wx_active_rate
       , a.collect_rate
       , a.collect_active_rate
       , a.ifcome0_rate
       , a.ifok0_rate
       , a.ifcome1_rate
       , a.ifok1_rate
       , a.ifcome2_rate
       , a.ifok2_rate
       , a.ifcome3_rate
       , a.ifok3_rate
       , a.ifcome4_rate
       , a.ifok4_rate
       , a.ifcome5_rate
       , a.ifok5_rate
       , a.wx_active_num
       , a.collect_active_num
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
       , a.ifok5;


DFS -touchz /dw/app/c_app_course_period_sales_dashboard/dt=${datebuf}/_SUCCESS;