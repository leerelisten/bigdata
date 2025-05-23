-- tittle
-- 投放日期 分组类型 
-- 品类 渠道 链路 版位 代理 账户id 计划id 
-- 账面支出 实际支出
-- 表单填写uv 支付成功uv 例子数(分配销售uv) 加微uv 问卷填写uv 正价课订单数 正价课金额 ROI=正价课金额/实际支出 转化率=正价课订单数/分配销售uv 实际CAC=实际支出/分配销售uv 
-- 主动加微率 主动问卷填写率 导学课到课率 导学课完课率 D1-D5到课完课率
-- 主动加微uv 主动问卷填写uv 导学课到课uv 导学课完课uv D1-D5到课完课uv
-- #账面CAC=账面支出/分配销售uv #加微CAC=实际支出/加微uv #主动加微uv #主动问卷填写uv  #D1-D7到课完课作业uv #素材id #购买正价课uv
-- #回传预留: 提交表单回传 支付成功回传 加微成功回传 问卷填写回传 导学课到课回传 D1到课回传

-- 1_期次
-- 2_期次x渠道
-- 3_期次x渠道x版位
-- 4_期次x价格
-- 5_期次x渠道x版位x价格
-- 6_期次x渠道x版位x价格x新链路
-- 7_期次x渠道x版位x价格x新链路x手机号
-- 8_期次x渠道x版位x价格x新链路x手机号x账户

-- 已兼容人工导入的消耗

SET mapred.job.name="c_app_course_period_ad_dashboard#${datebuf}";
USE app;
CREATE TABLE IF NOT EXISTS app.c_app_course_period_ad_dashboard
(
    d_date               string COMMENT '日期',
    is_abroad            STRING COMMENT '海外/国内',
    goods_name           string COMMENT '期次',
    grouptype            string COMMENT '分组类型',
    cat                  string COMMENT '品类',         --20241125新增品类
    ad_department        string COMMENT '投放部门',     --20250313新增投放部门
    platform_name        string COMMENT '渠道',
    pos                  string COMMENT '版位',
    h5_id                string COMMENT 'h5id',
    price                string COMMENT '价格',
    link_type_v2         string COMMENT '链路类型(新)',
    mobile               string COMMENT '收集手机号',
    cost_id              string COMMENT '账户',
    cost                 float COMMENT '账面消耗(元)',
    cost_real            float COMMENT '实际消耗(元)',
    submit_num           int COMMENT '表单填写例子数(个)',
    payment_num          int COMMENT '支付成功例子数(个)',
    user_num             int COMMENT '例子数(个)',
    wx_num               int COMMENT '加微例子数(个)',
    collect_num          int COMMENT '填问卷例子数(个)',
    get_ticket_num       int COMMENT '领劵例子数(个)',  --新增
    pay_num_D4           float COMMENT 'D4正价课订单数(单)',
    pay_sum_D4           float COMMENT 'D4正价课GMV(元)',
    roi_D4               float COMMENT 'D4ROI',
    pay_num_D5           float COMMENT 'D5正价课订单数(单)',
    pay_sum_D5           float COMMENT 'D5正价课GMV(元)',
    pay_num_D7           float COMMENT 'D7正价课订单数(单)',
    pay_sum_D7           float COMMENT 'D7正价课GMV(元)',
    pay_num_D8           float COMMENT 'D8正价课订单数(单)',
    pay_sum_D8           float COMMENT 'D8正价课GMV(元)',
    pay_num_D9           float COMMENT 'D9正价课订单数(单)',
    pay_sum_D9           float COMMENT 'D9正价课GMV(元)',
    pay_num_D14          float COMMENT 'D14正价课订单数(单)',
    pay_sum_D14          float COMMENT 'D14正价课GMV(元)',
    pay_user_num         int COMMENT '购买正价课例子数(个)',
    pay999_user_num      int COMMENT '购买999正价课例子数(个)',
    pay1980_user_num     int COMMENT '购买1980正价课例子数(个)',
    pay_num              float COMMENT '正价课订单数(单)',
    pay_sum              float COMMENT '正价课GMV(元)',
    roi_D5               float COMMENT 'D5ROI',
    roi_D7               float COMMENT 'D7ROI',
    roi_D8               float COMMENT 'D8ROI',
    roi_D9               float COMMENT 'D9ROI',
    roi_D14              float COMMENT 'D14ROI',
    roi                  float COMMENT 'ROI',
    conv_rate            float COMMENT '转化率(%)',
    cac                  float COMMENT 'CAC(元/个)',
    wx_rate              float COMMENT '加微率(%)',
    wx_active_rate       float COMMENT '主动加微率(%)',
    collect_rate         float COMMENT '问卷率(%)',
    collect_active_rate  float COMMENT '主动问卷率(%)',
    get_ticket_rate      float COMMENT '领劵率(%)',     --新增
    get_ticket_conv_rate float COMMENT '领劵转化率(%)', --新增
    ifcome0_rate         float COMMENT '导学课到课率(%)',
    ifok0_rate           float COMMENT '导学课完课率(%)',
    ifcome1_rate         float COMMENT 'D1到课率(%)',
    ifok1_rate           float COMMENT 'D1完课率(%)',
    ifcome2_rate         float COMMENT 'D2到课率(%)',
    ifok2_rate           float COMMENT 'D2完课率(%)',
    ifcome3_rate         float COMMENT 'D3到课率(%)',
    ifok3_rate           float COMMENT 'D3完课率(%)',
    ifcome4_rate         float COMMENT 'D4到课率(%)',
    ifok4_rate           float COMMENT 'D4完课率(%)',
    ifcome5_rate         float COMMENT 'D5到课率(%)',
    ifok5_rate           float COMMENT 'D5完课率(%)',
    wx_active_num        int COMMENT '主动加微例子数(个)',
    collect_active_num   int COMMENT '主动填问卷例子数(个)',
    ifcome0              int COMMENT '导学课到课例子数(个)',
    ifok0                int COMMENT '导学课完课例子数(个)',
    ifcome1              int COMMENT 'D1到课例子数(个)',
    ifok1                int COMMENT 'D1完课例子数(个)',
    ifcome2              int COMMENT 'D2到课例子数(个)',
    ifok2                int COMMENT 'D2完课例子数(个)',
    ifcome3              int COMMENT 'D3到课例子数(个)',
    ifok3                int COMMENT 'D3完课例子数(个)',
    ifcome4              int COMMENT 'D4到课例子数(个)',
    ifok4                int COMMENT 'D4完课例子数(个)',
    ifcome5              int COMMENT 'D5到课例子数(个)',
    ifok5                int COMMENT 'D5完课例子数(个)',
    m_olduv_rate         float COMMENT '重复例子占比(%)',
    m_olduv_rate30       float COMMENT '重复例子占比(30天)(%)',
    pay_num_d3           int COMMENT 'D3正价课订单数(单)',
    pay_sum_d3           int COMMENT 'D3正价课GMV(元)',
    roi_d3               decimal(10,6) COMMENT 'D3ROI',
    pay_num_d6           int COMMENT 'D6正价课订单数(单)',
    pay_sum_d6           int COMMENT 'D6正价课GMV(元)',
    roi_d6               decimal(10,6) COMMENT 'D6ROI',
    pay_num_d10          int COMMENT 'D10正价课订单数(单)',
    pay_sum_d10          int COMMENT 'D10正价课GMV(元)',
    roi_d10              decimal(10,6) COMMENT 'D10ROI',
    permission           string COMMENT '权限用户'
)
    COMMENT '培训主题数仓-投放数据期次报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_period_ad_dashboard';

SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;


-- 改用账户日CAC进行计算
CREATE TEMPORARY TABLE cost AS
    (SELECT cost_id
          , d_date
          , SUM(cost)      AS cost
          , SUM(cost_real) AS cost_real
     FROM da.da_course_daily_cost_by_adid
     WHERE dt = '${datebuf}'
     GROUP BY cost_id
            , d_date);



CREATE TEMPORARY TABLE users AS -- 20240914 本地推直播:直播一组,腾讯视频号直播付费流:直播二组,千川直播:直播三组
    (SELECT a.member_id
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
          , a.pay_num_D8
          , a.pay_sum_D8
          , a.pay_num_D9
          , a.pay_sum_D9
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
          , a.is_get_ticket
          , a.is_abroad
          , a.pay_num_D1
          , a.pay_sum_D1
          , a.pay_num_D2
          , a.pay_sum_D2
          , a.pay_num_D3
          , a.pay_sum_D3
          , a.pay_num_D10
          , a.pay_sum_D10
     FROM dws.dws_sale_camping_user_day a
     WHERE a.dt = '${datebuf}'
       AND TO_DATE(CONCAT('20', SUBSTR(a.goods_name, 2, 2), '-', SUBSTR(a.goods_name, 4, 2), '-',
                          SUBSTR(a.goods_name, 6, 2))) >= '2024-05-01'
       AND a.created_at >= '2024-05-01'
       --这里先注释掉 背景：20250403 发现业务方有 期次包含“测试”二字 但是数据是需要的
--        AND a.goods_name NOT LIKE '%测试%'
       AND (a.platform_name != '小糖私域' OR a.pos != '私域群活码') -- 20241113 剔除私域群活码
    );


CREATE TEMPORARY TABLE
    user_num AS
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
            , d_date);



CREATE TEMPORARY TABLE cac AS
    (SELECT a.cost_id
          , a.d_date
          , NVL(a.cost / b.num, 0)      AS cac
          , NVL(a.cost_real / b.num, 0) AS cac_real
     FROM cost a
              LEFT JOIN user_num b
                        ON a.cost_id = b.cost_id2
                            AND a.d_date = b.d_date);



CREATE TEMPORARY TABLE user_info AS
    ( -- 所有例子的投放属性&加微、问卷状态&购买正价课状态
        SELECT a.member_id
             , a.cat
             , a.ad_department
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
             , a.cost_id2                                                                    AS cost_id -- 20240828新增账户粒度 20240914 使用转译后账户id避免误解
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
             -- 人为定义非标等渠道的消耗
             , COALESCE(a.cac, d.cac, 0)                                                     AS cac
             , COALESCE(a.cac_real, d.cac_real, 0)                                           AS cac_real
             , a.pay_num_D4
             , a.pay_sum_D4
             , a.pay_num_D5
             , a.pay_sum_D5
             , a.pay_num_D7
             , a.pay_sum_D7
             , a.pay_num_D8
             , a.pay_sum_D8
             , a.pay_num_D9
             , a.pay_sum_D9
             , a.pay_num_D14
             , a.pay_sum_D14
             , a.is_get_ticket
             , a.is_abroad
             , a.pay_num_D1
             , a.pay_sum_D1
             , a.pay_num_D2
             , a.pay_sum_D2
             , a.pay_num_D3
             , a.pay_sum_D3
             , a.pay_num_D6
             , a.pay_sum_D6
             , a.pay_num_D10
             , a.pay_sum_D10
        FROM users a
                 LEFT JOIN cac d -- 消耗数据
                           ON d.cost_id = a.cost_id2
                               AND d.d_date = a.d_date);


-- 25.04.14，新增重复用户选项，口径：太极和八段锦一起剃重。
CREATE TEMPORARY TABLE olduser AS
    (SELECT member_id AS oldid
          , goods_name
     FROM (SELECT member_id
                , goods_name
                , created_at
                , cat
                , LAG(created_at) OVER (PARTITION BY member_id,IF(cat = '道门八段锦', '太极', cat) ORDER BY created_at ASC) AS lasttime
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
                  UNIX_TIMESTAMP(LAG(created_at)
                                     OVER (PARTITION BY member_id,IF(cat = '道门八段锦', '太极', cat) ORDER BY created_at ASC)) AS timediff
           FROM dw.dwd_xt_user
           WHERE dt = '${datebuf}'
             AND TO_DATE(created_at) <= '${datebuf}'
             AND member_status = 1
             AND trade_state IN ('SUCCESS', 'PREPARE')
             AND sales_id > 0) t
     WHERE timediff <= 30 * 24 * 3600
     GROUP BY member_id, goods_name);



CREATE TEMPORARY TABLE mid AS
    ( -- 按期次+投放属性聚合用户
        SELECT TO_DATE(user_info.created_at)                                                      AS d_date
             , user_info.is_abroad
             , user_info.goods_name
             , user_info.cat
             , user_info.ad_department
             , user_info.platform_name
             , user_info.price
             , user_info.link_type_v2
             , user_info.mobile
             , user_info.pos
             , user_info.h5_id
             , CONCAT('id_', cost_id)                                                             AS cost_id
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
             , COUNT(IF(
                member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0 AND is_get_ticket = '2是',
                member_id,
                NULL))                                                                            AS get_ticket_num     -- 领券uv

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
             , SUM(IF(
                member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0 AND is_get_ticket = '2是',
                pay_num,
                0))                                                                               AS get_ticket_pay_num -- 领券订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum,
                      0))                                                                         AS pay_sum            -- 正价课GMV
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D4,
                      0))                                                                         AS pay_num_D4         -- 正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D4,
                      0))                                                                         AS pay_sum_D4         -- 正价课GMV
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D5,
                      0))                                                                         AS pay_num_D5         -- D5正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D5,
                      0))                                                                         AS pay_sum_D5         -- D5正价课GMV
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D7,
                      0))                                                                         AS pay_num_D7         -- D7正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D7,
                      0))                                                                         AS pay_sum_D7         -- D7正价课GMV
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D8,
                      0))                                                                         AS pay_num_D8         -- D8正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D8,
                      0))                                                                         AS pay_sum_D8         -- D8正价课GMV
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D9,
                      0))                                                                         AS pay_num_D9         -- D9正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D9,
                      0))                                                                         AS pay_sum_D9         -- D9正价课GMV
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

             , COUNT(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, olduser.oldid,
                        NULL))                                                                    AS olduser_num        -- 老用户例子数
             , COUNT(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, olduser30.oldid,
                        NULL))                                                                    AS olduser_num30     -- 老用户例子数30
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D3,
                      0))                                                                         AS pay_num_D3       -- D3正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D3,
                      0))                                                                         AS pay_sum_D3       -- D3正价课GMV
            , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D6,
                      0))                                                                         AS pay_num_D6       -- D6正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D6,
                      0))                                                                         AS pay_sum_D6       -- D6正价课GMV
            , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num_D10,
                      0))                                                                         AS pay_num_D10       -- D10正价课订单数
             , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum_D10,
                      0))                                                                         AS pay_sum_D10       -- D10正价课GMV
        FROM user_info
                 LEFT JOIN olduser
                           ON user_info.member_id = olduser.oldid AND user_info.goods_name = olduser.goods_name
                 LEFT JOIN olduser30
                           ON user_info.member_id = olduser30.oldid AND user_info.goods_name = olduser30.goods_name
        GROUP BY TO_DATE(user_info.created_at)
               , user_info.is_abroad
               , user_info.goods_name
               , user_info.cat
               , user_info.ad_department
               , user_info.platform_name
               , user_info.price
               , user_info.link_type_v2
               , user_info.mobile
               , user_info.pos
               , user_info.h5_id
               , CONCAT('id_', cost_id));



CREATE TEMPORARY TABLE c_app_course_period_ad_dashboard_temp AS
    (SELECT '${datebuf}'                                                AS d_date
          , is_abroad
          , goods_name
          , CASE GROUPING__ID
                WHEN 255 THEN '1_海内外x期次x品类'
                WHEN 127 THEN '2_海内外x期次x品类X投放部门'
                WHEN 63  THEN '3_海内外x期次x品类x投放部门x渠道'
                WHEN 31  THEN '4_海内外x期次x品类x投放部门x渠道x版位'
                WHEN 15  THEN '5_海内外x期次x品类x投放部门x渠道x版位xh5_id'
                WHEN 247 THEN '6_海内外x期次x品类x价格'
                WHEN 23  THEN '7_海内外x期次x品类x投放部门x渠道x版位x价格'
                WHEN 19  THEN '8_海内外x期次x品类x投放部门x渠道x版位x价格x新链路'
                WHEN 17  THEN '9_海内外x期次x品类x投放部门x渠道x版位x价格x新链路x手机号'
                WHEN 16  THEN '10_海内外x期次x品类x投放部门x渠道x版位x价格x新链路x手机号x账户'
                WHEN 0  THEN '11_海内外x期次x品类x投放部门x渠道x版位x价格x新链路x手机号x账户xh5_id'
            END                                                         AS grouptype
          , NVL(cat, 'ALL-汇总')                                        AS cat           --20241125新增品类
          , NVL(ad_department, 'ALL-汇总')                              AS ad_department
          , NVL(platform_name, 'ALL-汇总')                              AS platform_name
          , NVL(pos, 'ALL-汇总')                                        AS pos
          , NVL(h5_id, 'ALL-汇总')                                      AS h5_id
          , NVL(price, 'ALL-汇总')                                      AS price
          , NVL(link_type_v2, 'ALL-汇总')                               AS link_type_v2
          , NVL(mobile, 'ALL-汇总')                                     AS mobile
          , NVL(cost_id, 'ALL-汇总')                                    AS cost_id
          , SUM(cost)                                                   AS cost
          , SUM(cost_real)                                              AS cost_real
          , SUM(submit_num)                                             AS submit_num
          , SUM(payment_num)                                            AS payment_num
          , SUM(user_num)                                               AS user_num
          , SUM(wx_num)                                                 AS wx_num
          , SUM(collect_num)                                            AS collect_num
          , SUM(get_ticket_num)                                         AS get_ticket_num
          , SUM(pay_num_D3)                                             AS pay_num_D3
          , SUM(pay_sum_D3)                                             AS pay_sum_D3
          , SUM(pay_num_D4)                                             AS pay_num_D4
          , SUM(pay_sum_D4)                                             AS pay_sum_D4
          , NVL(SUM(pay_sum_D4) / SUM(cost_real), 0)                    AS roi_D4
          , SUM(pay_num_D5)                                             AS pay_num_D5
          , SUM(pay_sum_D5)                                             AS pay_sum_D5
          , SUM(pay_num_D6)                                             AS pay_num_D6
          , SUM(pay_sum_D6)                                             AS pay_sum_D6
          , SUM(pay_num_D7)                                             AS pay_num_D7
          , SUM(pay_sum_D7)                                             AS pay_sum_D7
          , SUM(pay_num_D8)                                             AS pay_num_D8
          , SUM(pay_sum_D8)                                             AS pay_sum_D8
          , SUM(pay_num_D9)                                             AS pay_num_D9
          , SUM(pay_sum_D9)                                             AS pay_sum_D9
          , SUM(pay_num_D10)                                            AS pay_num_D10
          , SUM(pay_sum_D10)                                            AS pay_sum_D10
          , SUM(pay_num_D14)                                            AS pay_num_D14
          , SUM(pay_sum_D14)                                            AS pay_sum_D14
          , SUM(pay_user_num)                                           AS pay_user_num
          , SUM(pay999_user_num)                                        AS pay999_user_num
          , SUM(pay1980_user_num)                                       AS pay1980_user_num
          , SUM(pay_num)                                                AS pay_num
          , SUM(pay_sum)                                                AS pay_sum
          , NVL(SUM(pay_sum_D3) / SUM(cost_real), 0)                    AS roi_D3
          , NVL(SUM(pay_sum_D5) / SUM(cost_real), 0)                    AS roi_D5
          , NVL(SUM(pay_sum_D6) / SUM(cost_real), 0)                    AS roi_D6

          , NVL(SUM(pay_sum_D7) / SUM(cost_real), 0)                    AS roi_D7
          , NVL(SUM(pay_sum_D8) / SUM(cost_real), 0)                    AS roi_D8
          , NVL(SUM(pay_sum_D9) / SUM(cost_real), 0)                    AS roi_D9

          , NVL(SUM(pay_sum_D10) / SUM(cost_real), 0)                    AS roi_D10
          , NVL(SUM(pay_sum_D14) / SUM(cost_real), 0)                   AS roi_D14
          , NVL(SUM(pay_sum) / SUM(cost_real), 0)                       AS roi
          , NVL(SUM(pay_num) / SUM(user_num) * 100, 0)                  AS conv_rate
          , NVL(SUM(cost_real) / SUM(user_num), 0)                      AS cac
          , NVL(SUM(wx_num) / SUM(payment_num) * 100, 0)                AS wx_rate
          , NVL(SUM(wx_active_num) / SUM(payment_num) * 100, 0)         AS wx_active_rate
          , NVL(SUM(collect_num) / SUM(user_num) * 100, 0)              AS collect_rate
          , NVL(SUM(collect_active_num) / SUM(user_num) * 100, 0)       AS collect_active_rate
          , NVL(SUM(get_ticket_num) / SUM(user_num) * 100, 0)           AS get_ticket_rate
          , NVL(SUM(get_ticket_pay_num) / SUM(get_ticket_num) * 100, 0) AS get_ticket_conv_rate
          , NVL(SUM(ifcome0) / SUM(user_num) * 100, 0)                  AS ifcome0_rate
          , NVL(SUM(ifok0) / SUM(user_num) * 100, 0)                    AS ifok0_rate
          , NVL(SUM(ifcome1) / SUM(user_num) * 100, 0)                  AS ifcome1_rate
          , NVL(SUM(ifok1) / SUM(user_num) * 100, 0)                    AS ifok1_rate
          , NVL(SUM(ifcome2) / SUM(user_num) * 100, 0)                  AS ifcome2_rate
          , NVL(SUM(ifok2) / SUM(user_num) * 100, 0)                    AS ifok2_rate
          , NVL(SUM(ifcome3) / SUM(user_num) * 100, 0)                  AS ifcome3_rate
          , NVL(SUM(ifok3) / SUM(user_num) * 100, 0)                    AS ifok3_rate
          , NVL(SUM(ifcome4) / SUM(user_num) * 100, 0)                  AS ifcome4_rate
          , NVL(SUM(ifok4) / SUM(user_num) * 100, 0)                    AS ifok4_rate
          , NVL(SUM(ifcome5) / SUM(user_num) * 100, 0)                  AS ifcome5_rate
          , NVL(SUM(ifok5) / SUM(user_num) * 100, 0)                    AS ifok5_rate
          , SUM(wx_active_num)                                          AS wx_active_num
          , SUM(collect_active_num)                                     AS collect_active_num
          , SUM(ifcome0)                                                AS ifcome0
          , SUM(ifok0)                                                  AS ifok0
          , SUM(ifcome1)                                                AS ifcome1
          , SUM(ifok1)                                                  AS ifok1
          , SUM(ifcome2)                                                AS ifcome2
          , SUM(ifok2)                                                  AS ifok2
          , SUM(ifcome3)                                                AS ifcome3
          , SUM(ifok3)                                                  AS ifok3
          , SUM(ifcome4)                                                AS ifcome4
          , SUM(ifok4)                                                  AS ifok4
          , SUM(ifcome5)                                                AS ifcome5
          , SUM(ifok5)                                                  AS ifok5
          , NVL(SUM(olduser_num) / SUM(user_num) * 100, 0)              AS m_olduv_rate-- 老用户例子占比
          , NVL(SUM(olduser_num30) / SUM(user_num) * 100, 0)            AS m_olduv_rate30-- 老用户例子占比30
     FROM mid
     GROUP BY is_abroad
            , goods_name
            , cat
            , ad_department
            , platform_name
            , pos
            , h5_id
            , price
            , link_type_v2
            , mobile
            , cost_id
         GROUPING SETS (
              (is_abroad, goods_name, cat)                                                         -- 00011111111
            , (is_abroad, goods_name, cat, ad_department)                                          -- 00001111111
            , (is_abroad, goods_name, cat, ad_department, platform_name)                           -- 00000111111
            , (is_abroad, goods_name, cat, ad_department, platform_name, pos)                      -- 00000011111
            , (is_abroad, goods_name, cat, ad_department, platform_name, pos, h5_id)               -- 00000001111
            , (is_abroad, goods_name, cat, price)                                                  -- 00011110111
            , (is_abroad, goods_name, cat, ad_department, platform_name, pos, price)               -- 00000010111
            , (is_abroad, goods_name, cat, ad_department, platform_name, pos, price, link_type_v2) -- 00000010011
            , (is_abroad, goods_name, cat, ad_department, platform_name, pos, price, link_type_v2
            , mobile)                                                                              -- 00000010001
            , (is_abroad, goods_name, cat, ad_department, platform_name, pos, price, link_type_v2, mobile
            , cost_id)                                                                             -- 00000010000
            , (is_abroad, goods_name, cat, ad_department, platform_name, pos, price, link_type_v2, mobile
            , cost_id, h5_id)                                                                      -- 00000000000
         ));


INSERT
    OVERWRITE
    TABLE app.c_app_course_period_ad_dashboard
    PARTITION
    (dt = '${datebuf}')
SELECT a.d_date
     , a.is_abroad
     , a.goods_name
     , a.grouptype
     , a.cat
     , a.ad_department
     , a.platform_name
     , a.pos
     , a.h5_id
     , a.price
     , a.link_type_v2
     , a.mobile
     , a.cost_id
     , a.cost
     , a.cost_real
     , a.submit_num
     , a.payment_num
     , a.user_num
     , a.wx_num
     , a.collect_num
     , a.get_ticket_num
     , a.pay_num_D4
     , a.pay_sum_D4
     , a.roi_D4
     , a.pay_num_D5
     , a.pay_sum_D5
     , a.pay_num_D7
     , a.pay_sum_D7
     , a.pay_num_D8
     , a.pay_sum_D8
     , a.pay_num_D9
     , a.pay_sum_D9
     , a.pay_num_D14
     , a.pay_sum_D14
     , a.pay_user_num
     , a.pay999_user_num
     , a.pay1980_user_num
     , a.pay_num
     , a.pay_sum
     , a.roi_D5
     , a.roi_D7
     , a.roi_D8
     , a.roi_D9
     , a.roi_D14
     , a.roi
     , a.conv_rate
     , a.cac
     , a.wx_rate
     , a.wx_active_rate
     , a.collect_rate
     , a.collect_active_rate
     , a.get_ticket_rate
     , a.get_ticket_conv_rate
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
     , a.m_olduv_rate
     , a.m_olduv_rate30
     , a.pay_num_d3
     , a.pay_sum_d3
     , a.roi_d3
     , a.pay_num_d6
     , a.pay_sum_d6
     , a.roi_d6
     , a.pay_num_d10
     , a.pay_sum_d10
     , a.roi_d10
     , CONCAT_WS(',', COLLECT_SET(pm.emails)) AS permission
FROM c_app_course_period_ad_dashboard_temp a
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
                      AND report_id = '1417') pm
                   ON (pm.is_abroad = '全部' OR a.is_abroad = pm.is_abroad)
                       AND (pm.cat = '全部' OR a.cat = pm.cat)
                       AND (pm.platform_name = '全部' OR a.platform_name = pm.platform_name)
                       AND (pm.pos = '全部' OR a.pos = pm.pos)
                       AND (pm.ad_department = '全部' OR a.ad_department = pm.ad_department)

GROUP BY a.d_date
     , a.is_abroad
     , a.goods_name
     , a.grouptype
     , a.cat
     , a.ad_department
     , a.platform_name
     , a.pos
     , a.h5_id
     , a.price
     , a.link_type_v2
     , a.mobile
     , a.cost_id
     , a.cost
     , a.cost_real
     , a.submit_num
     , a.payment_num
     , a.user_num
     , a.wx_num
     , a.collect_num
     , a.get_ticket_num
     , a.pay_num_D4
     , a.pay_sum_D4
     , a.roi_D4
     , a.pay_num_D5
     , a.pay_sum_D5
     , a.pay_num_D7
     , a.pay_sum_D7
     , a.pay_num_D8
     , a.pay_sum_D8
     , a.pay_num_D9
     , a.pay_sum_D9
     , a.pay_num_D14
     , a.pay_sum_D14
     , a.pay_user_num
     , a.pay999_user_num
     , a.pay1980_user_num
     , a.pay_num
     , a.pay_sum
     , a.roi_D5
     , a.roi_D7
     , a.roi_D8
     , a.roi_D9
     , a.roi_D14
     , a.roi
     , a.conv_rate
     , a.cac
     , a.wx_rate
     , a.wx_active_rate
     , a.collect_rate
     , a.collect_active_rate
     , a.get_ticket_rate
     , a.get_ticket_conv_rate
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
     , a.m_olduv_rate
     , a.m_olduv_rate30
     , a.pay_num_d3
     , a.pay_sum_d3
     , a.roi_d3
     , a.pay_num_d6
     , a.pay_sum_d6
     , a.roi_d6
     , a.pay_num_d10
     , a.pay_sum_d10
     , a.roi_d10
