CREATE TABLE IF NOT EXISTS app.c_app_course_channel_forecast_indicators
(
    cat               string COMMENT '品类',
    goods_name_period string COMMENT '期次',
    platform_name     string COMMENT '渠道',
    pos               string COMMENT '版位',
    pos_type          string COMMENT '版位类型',
    price             string COMMENT '价格',
    h5_id             string COMMENT 'h5_id',
    cost_id2          string COMMENT '账户ID',
    cost              float COMMENT '账面消耗(元)',
    cost_real         float COMMENT '实际消耗(元)',
    cac_real          float COMMENT '实际CAC(元/个)',
    user_num          int COMMENT '例子数',
    pay_num_D4        float COMMENT 'D4订单数',
    conv_D4           float COMMENT 'D4转化率',
    pay_sum_D4        float COMMENT 'D4GMV(元)',
    roi_D4            float COMMENT 'D4ROI',
    pay_num_D7        float COMMENT 'D7订单数',
    conv_D7           float COMMENT 'D7转化率',
    pay_sum_D7        float COMMENT 'D7GMV(元)',
    roi_D7            float COMMENT 'D7ROI',
    pay_num_D8        float COMMENT 'D8订单数',
    pay_sum_D8        float COMMENT 'D8GMV(元)',
    pay_num           float COMMENT '总订单数',
    conv              float COMMENT '总转化率',
    pay_sum           float COMMENT '正价课GMV(元)',
    roi               float COMMENT 'ROI',
    wx_num            int COMMENT '加微例子数',
    wx_active         int COMMENT '主动加微例子数',
    ifcollect         int COMMENT '填问卷例子数',
    collect_active    int COMMENT '主动填问卷例子数',
    interest          int COMMENT '兴趣度例子数',
    influence         int COMMENT '紧迫度例子数',
    sex               int COMMENT '女性例子数',
    age_45            int COMMENT '45岁以上例子数',
    age_50            int COMMENT '50岁以上例子数',
    city              int COMMENT '二线及以上城市例子数',
    wx_rel_status_new int COMMENT '加微例子数_新',
    wx_active_new     int COMMENT '主动加微例子数_新',
    exp_user_num      int COMMENT '支付成功例子数',
    old_user          int COMMENT '重复例子(历史至今)',
    old_user_30       int COMMENT '重复例子(30天)',
    ifcome0           int COMMENT '导学课是否到课',
    ifok0             int COMMENT '导学课是否完课',
    ifcome1           int COMMENT 'D1是否到课',
    ifok1             int COMMENT 'D1是否完课',
    ifcome2           int COMMENT 'D2是否到课',
    ifok2             int COMMENT 'D2是否完课',
    ifcome3           int COMMENT 'D3是否到课',
    ifok3             int COMMENT 'D3是否完课',
    ifcome4           int COMMENT 'D4是否到课',
    ifok4             int COMMENT 'D4是否完课'
)
    COMMENT '培训主题数仓-渠道预测指标'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE;


SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;


SET hive.execution.engine = tez;
WITH tmp1 AS (SELECT m.member_id
              FROM ods.ods_xiaoe_special_member_relation m
                       LEFT JOIN ods.ods_xiaoe_special xs
                                 ON m.special_xe_id = xs.xe_id
              WHERE xs.goods_name LIKE '%训练营%'
                AND xs.goods_name NOT LIKE '%老学员%'
                AND xs.goods_name LIKE '%太极%'
--                 AND xs.goods_name NOT LIKE '%测试%'
                --  背景：20250403 发现业务方有 期次包含“测试”二字 但是数据是需要的
                AND (xs.goods_name RLIKE '武当秘传太极养生功•5天训练营' OR (xs.goods_name NOT LIKE '%测试%'))
                AND m.member_sales_id NOT IN (0, 303)
              GROUP BY m.member_id
              HAVING COUNT(1) > 1)
   , cost AS
    (SELECT cost_id        AS cost_id
          , d_date
          , SUM(cost)      AS cost
          , SUM(cost_real) AS cost_real
     FROM da.da_course_daily_cost_by_adid
     WHERE dt = TO_DATE(DATE_SUB(CURRENT_TIMESTAMP(), 1))
       AND CASE
               WHEN pos = '头条直播付费流' AND price = '1元' THEN d_date >= '2024-07-05'
               ELSE d_date >= '2024-05-28' END
     GROUP BY cost_id
            , d_date)
   , users_1 AS -- 20240914 本地推直播:直播一组,腾讯视频号直播付费流:直播二组,千川直播:直播三组
    (SELECT *
     FROM dws.dws_sale_camping_user_day
     WHERE dt = TO_DATE(DATE_SUB(CURRENT_TIMESTAMP(), 1))
       AND is_abroad = '国内'
       AND (platform_name != '小糖私域'
         OR pos != '私域群活码') -- 20241113 剔除私域群活码
    )
   , user_num AS
    (SELECT cost_id2
          , d_date
          , COUNT(*) num
     FROM users_1
     WHERE member_status = 1
       AND trade_state IN ('SUCCESS', 'PREPARE')
       AND sales_id > 0
     GROUP BY cost_id2
            , d_date)
   , cac AS
    (SELECT a.cost_id
          , a.d_date
          , NVL(a.cost / b.num, 0)      AS cac
          , NVL(a.cost_real / b.num, 0) AS cac_real
     FROM COST a
              LEFT JOIN user_num b
                        ON a.cost_id = b.cost_id2
                            AND a.d_date = b.d_date)
   , mid AS
    (SELECT DISTINCT a.goods_name
                   , a.member_id
                   , a.xe_id
                   , a.platform_name
                   , a.pos
                   , a.price
                   , a.mobile
                   , a.link_type_v2
                   , a.h5_id
                   , a.cost_id2
                   , a.sucai_id
                   , SPLIT(REGEXP_REPLACE(TRIM(a.sales_name), '[0-9]', ''), '（')[0] AS    sales_name
                   , a.department
                   , a.user_group
                   , a.created_at
                   , a.is_get_ticket
                   , NVL(a.first_pay_time, '')                                            pay_time
                   , NVL(a.pay_num, 0)                                                    pay_num
                   , NVL(a.pay_sum, 0)                                                    pay_sum
                   , NVL(a.cac, c.cac)                                              AS    cac
                   , NVL(a.cac_real, c.cac_real)                                    AS    cac_real
                   , 1                                                              AS    user_num
                   , a.pay_num_D4
                   , a.pay_sum_D4
                   , a.pay_num_d7
                   , a.pay_sum_d7
                   , a.pay_num_d8
                   , a.pay_sum_d8
                   , CASE
                         WHEN a.goods_name RLIKE '八段锦' THEN '八段锦'
                         WHEN a.goods_name RLIKE '晨课' THEN '晨课'
                         WHEN a.goods_name RLIKE '5天训练营' THEN '5天体验课'
                         WHEN a.goods_name RLIKE '线上训练营' THEN '线上训练营'
                         ELSE '老体验课' END                                        AS    cat
                   , SUBSTR(a.goods_name, 2, 7)                                     AS    goods_name_period

                   , IF(a.trade_state IN ('SUCCESS', 'PREPARE') AND a.sales_id > 0, 1, 0) exp_user_status
                   , IF(a.wx_rel_status IN (2, 3, 4), 1, 0)                               wx_rel_status_new
                   , IF(
                a.wx_rel_status IN (2, 3, 4) AND UNIX_TIMESTAMP(a.wx_add_time) - UNIX_TIMESTAMP(a.created_at) <= 300, 1,
                0)                                                                        wx_active_new
                   , a.member_status -- 东雷20241024 23点注：重要，后面处理要用


                   , IF(a.wx_rel_status IN (2, 3), 1, 0)                                  wx_rel_status
                   , IF(
                a.wx_rel_status IN (2, 3) AND UNIX_TIMESTAMP(a.wx_add_time) - UNIX_TIMESTAMP(a.created_at) <= 300, 1,
                0)                                                                        wx_active
                   , a.ifcollect
                   , a.collect_active
                   , NVL(a.ifcome0, 0)                                                    ifcome0
                   , NVL(a.ifok0, 0)                                                      ifok0
                   , NVL(a.ifcome1, 0)                                                    ifcome1
                   , NVL(a.ifok1, 0)                                                      ifok1
                   , NVL(a.ifcome2, 0)                                                    ifcome2
                   , NVL(a.ifok2, 0)                                                      ifok2
                   , NVL(a.ifcome3, 0)                                                    ifcome3
                   , NVL(a.ifok3, 0)                                                      ifok3
                   , NVL(a.ifcome4, 0)                                                    ifcome4
                   , NVL(a.ifok4, 0)                                                      ifok4
                   , NVL(a.ifcome5, 0)                                                    ifcome5
                   , NVL(a.ifok5, 0)                                                      ifok5
                   , IF(e.member_id IS NULL, '新用户', '老用户')                          old_user
                   , IF(f.ex_unionid IS NOT NULL, 1, 0)                                   wx_rel_status_fix
                   , IF(f.ex_unionid IS NOT NULL AND UNIX_TIMESTAMP(f.created_at) - UNIX_TIMESTAMP(a.created_at) <= 300,
                        1,
                        0)                                                                wx_active_fix
                   , NVL(g.interest, 0)                                                   interest
                   , NVL(g.influence, 0)                                                  influence
                   , NVL(g.sex, 0)                                                        sex
                   , NVL(g.age_45, 0)                                                     age_45
                   , NVL(g.age_50, 0)                                                     age_50
                   , NVL(g.city, 0)                                                       city
                   , a.d_date
     FROM (SELECT *
           FROM dws.dws_sale_camping_user_day
           WHERE dt = TO_DATE(DATE_SUB(CURRENT_TIMESTAMP(), 1))
             AND is_abroad = '国内'
             --这里先注释掉 背景：20250403 发现业务方有 期次包含“测试”二字 但是数据是需要的
--              AND (goods_name NOT LIKE '%测试%')
             AND (platform_name != '小糖私域' OR pos != '私域群活码')) a
              LEFT JOIN cac c -- 消耗数据
                        ON c.cost_id = a.cost_id2 AND c.d_date = a.d_date
              LEFT JOIN (SELECT member_id
                              , special_id
                         FROM (SELECT m.special_xe_id                                                     AS special_id
                                    , m.member_id
                                    , ROW_NUMBER() OVER (PARTITION BY m.member_id ORDER BY m.addtime ASC) AS rnum
                               FROM ods.ods_xiaoe_special_member_relation m -- 学员和专栏关系
                                        LEFT JOIN ods.ods_xiaoe_special xs
                                                  ON m.special_xe_id = xs.xe_id
                               WHERE xs.goods_name LIKE '%训练营%'
                                 AND xs.goods_name NOT LIKE '%老学员%'
                                 AND xs.goods_name LIKE '%太极%'
--                                  AND xs.goods_name NOT LIKE '%测试%'
                                 --  背景：20250403 发现业务方有 期次包含“测试”二字 但是数据是需要的
                                 AND (xs.goods_name RLIKE '武当秘传太极养生功•5天训练营' OR (xs.goods_name NOT LIKE '%测试%'))
                                 AND m.member_sales_id NOT IN (0, 303)
                                 AND m.member_id IN (SELECT * FROM tmp1)) t
                         WHERE rnum > 1) e -- 判断是否重复
                        ON e.member_id = a.member_id
                            AND e.special_id = a.special_id
              LEFT JOIN (SELECT a.ex_unionid
                              , MIN(a.created_at) AS created_at -- 取用户微信的首次加微时间(旧逻辑)
                         FROM ods.ods_place_contact a
                                  LEFT JOIN ods.ods_place_sales b
                                            ON a.userid = b.corp_userid
                         WHERE a.ex_unionid IS NOT NULL
                           AND a.ex_unionid <> ''
                         GROUP BY a.ex_unionid) f
                        ON a.unionid = f.ex_unionid
              LEFT JOIN (SELECT member_id
                              , goods_name
                              , IF(
                 taiji_interest IN ('曾经线上或线下学习过太极，计划提高', '知道太极养生对健康的帮助，想学习太极'), 1,
                 0)                                                                                                       interest
                              , IF(taiji_influence IN ('有时影响生活，需要调理改善', '影响不大，或已经找到改善方式'), 1, 0) influence
                              , IF(sex = '女', 1, 0)                                                                      sex
                              , IF(age_level IN ('46-50岁', '56-60岁', '66-70岁', '71岁及以上', '51-55岁', '61-65岁'),
                                   1, 0)                                                                                  age_45
                              , IF(age_level IN ('56-60岁', '66-70岁', '71岁及以上', '51-55岁', '61-65岁'), 1, 0)         age_50
                              , IF(city_level IN ('一线城市', '新一线城市', '二线城市'), 1, 0)                            city
                         FROM app.c_app_course_xt_user_profile
                         WHERE dt = TO_DATE(DATE_SUB(CURRENT_TIMESTAMP(), 1))
                         GROUP BY member_id
                                , goods_name
                                , IF(
                                 taiji_interest IN
                                 ('曾经线上或线下学习过太极，计划提高', '知道太极养生对健康的帮助，想学习太极'), 1,
                                 0)
                                , IF(taiji_influence IN ('有时影响生活，需要调理改善', '影响不大，或已经找到改善方式'), 1, 0)
                                , IF(sex = '女', 1, 0)
                                , IF(age_level IN ('46-50岁', '56-60岁', '66-70岁', '71岁及以上', '51-55岁', '61-65岁'),
                                     1, 0)
                                , IF(age_level IN ('56-60岁', '66-70岁', '71岁及以上', '51-55岁', '61-65岁'), 1, 0)
                                , IF(city_level IN ('一线城市', '新一线城市', '二线城市'), 1, 0)) g --问卷
                        ON a.member_id = g.member_id
                            AND a.goods_name = g.goods_name
     WHERE a.dt = TO_DATE(DATE_SUB(CURRENT_TIMESTAMP(), 1))
       -- AND a.member_status = 1
       AND a.trade_state IN ('SUCCESS', 'PREPARE')
       AND a.sales_id > 0
       AND SUBSTR(a.goods_name, 2, 6) >= DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP(), 60), 'yyMMdd')
       AND SUBSTR(a.goods_name, 2, 6) <= DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP(), 0), 'yyMMdd')
       AND a.goods_name RLIKE '太极')
   , old_user_30 AS (SELECT member_id AS oldid
                          , goods_name
                     FROM (SELECT member_id
                                , goods_name
                                , created_at
                                , cat
                                , UNIX_TIMESTAMP(created_at) -
                                  UNIX_TIMESTAMP(LAG(created_at)
                                                     OVER (PARTITION BY member_id,cat ORDER BY created_at ASC)) AS timediff
                           FROM dw.dwd_xt_user
                           WHERE dt = DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP(), 1), 'yyMMdd')
                             AND TO_DATE(created_at) <= DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP(), 1), 'yyMMdd')
                             AND member_status = 1
                             AND trade_state IN ('SUCCESS', 'PREPARE')
                             AND sales_id > 0) t
                     WHERE timediff <= 30 * 24 * 3600
                     GROUP BY member_id, goods_name)
   , old_user AS (SELECT member_id AS oldid
                       , goods_name
                  FROM (SELECT member_id
                             , goods_name
                             , created_at
                             , cat
                             , UNIX_TIMESTAMP(created_at) -
                               UNIX_TIMESTAMP(LAG(created_at)
                                                  OVER (PARTITION BY member_id,cat ORDER BY created_at ASC)) AS timediff
                        FROM dw.dwd_xt_user
                        WHERE dt = DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP(), 1), 'yyMMdd')
                          AND TO_DATE(created_at) <= DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP(), 1), 'yyMMdd')
                          AND member_status = 1
                          AND trade_state IN ('SUCCESS', 'PREPARE')
                          AND sales_id > 0) t
                  WHERE timediff > 0
                  GROUP BY member_id, goods_name)

-- 按h5/cost_id统计头条直播付费流1元

INSERT
OVERWRITE
TABLE
app.c_app_course_channel_forecast_indicators
PARTITION
(
dt = '${datebuf}'
)
SELECT t1.cat
     , t1.goods_name_period
     , t1.platform_name
     , t1.pos
     , t2.pos_1 AS pos_type
     , t1.price
     , t1.h5_id
     , t1.cost_id2
     , t1.cost
     , t1.cost_real
     , t1.cac_real
     , t1.user_num
     , t1.pay_num_D4
     , t1.conv_D4
     , t1.pay_sum_D4
     , t1.roi_D4
     , t1.pay_num_D7
     , t1.conv_D7
     , t1.pay_sum_D7
     , t1.roi_D7
     , t1.pay_num_D8
     , t1.pay_sum_D8
     , t1.pay_num
     , t1.conv
     , t1.pay_sum
     , t1.roi
     , t1.wx_num
     , t1.wx_active
     , t1.ifcollect
     , t1.collect_active
     , t1.interest
     , t1.influence
     , t1.sex
     , t1.age_45
     , t1.age_50
     , t1.city
     , t1.wx_rel_status_new
     , t1.wx_active_new
     , t1.exp_user_num
     , t1.old_user
     , t1.old_user_30
     , t1.ifcome0
     , t1.ifok0
     , t1.ifcome1
     , t1.ifok1
     , t1.ifcome2
     , t1.ifok2
     , t1.ifcome3
     , t1.ifok3
     , t1.ifcome4
     , t1.ifok4
FROM (SELECT cat
           , goods_name_period
           , platform_name
           , pos
           , price
           , h5_id
           , cost_id2
           , SUM(CASE WHEN member_status = 1 THEN NVL(cac, 0) ELSE 0 END)              AS cost
           , SUM(CASE WHEN member_status = 1 THEN NVL(cac_real, 0) ELSE 0 END)         AS cost_real
           , NVL(SUM(CASE WHEN member_status = 1 THEN NVL(cac_real, 0) ELSE 0 END) /
                 SUM(CASE WHEN member_status = 1 THEN NVL(user_num, 0) ELSE 0 END), 0) AS cac_real
           , SUM(CASE WHEN member_status = 1 THEN NVL(user_num, 0) ELSE 0 END)         AS user_num
           , SUM(CASE WHEN member_status = 1 THEN NVL(pay_num_D4, 0) ELSE 0 END)       AS pay_num_D4
           , NVL(SUM(CASE WHEN member_status = 1 THEN NVL(pay_num_D4, 0) ELSE 0 END) /
                 SUM(CASE WHEN member_status = 1 THEN NVL(user_num, 0) ELSE 0 END), 0) AS conv_D4
           , SUM(CASE WHEN member_status = 1 THEN NVL(pay_sum_D4, 0) ELSE 0 END)       AS pay_sum_D4
           , NVL(SUM(CASE WHEN member_status = 1 THEN pay_sum_D4 ELSE 0 END) /
                 SUM(CASE WHEN member_status = 1 THEN NVL(cac_real, 0) ELSE 0 END), 0) AS roi_D4

           , SUM(CASE WHEN member_status = 1 THEN NVL(pay_num_D7, 0) ELSE 0 END)       AS pay_num_D7
           , NVL(SUM(CASE WHEN member_status = 1 THEN NVL(pay_num_D7, 0) ELSE 0 END) /
                 SUM(CASE WHEN member_status = 1 THEN NVL(user_num, 0) ELSE 0 END), 0) AS conv_D7
           , SUM(CASE WHEN member_status = 1 THEN NVL(pay_sum_D7, 0) ELSE 0 END)       AS pay_sum_D7
           , NVL(SUM(CASE WHEN member_status = 1 THEN pay_sum_D7 ELSE 0 END) /
                 SUM(CASE WHEN member_status = 1 THEN NVL(cac_real, 0) ELSE 0 END), 0) AS roi_D7

           , SUM(CASE WHEN member_status = 1 THEN NVL(pay_num_D8, 0) ELSE 0 END)       AS pay_num_D8
           , SUM(CASE WHEN member_status = 1 THEN NVL(pay_sum_D8, 0) ELSE 0 END)       AS pay_sum_D8


           , SUM(CASE WHEN member_status = 1 THEN pay_num ELSE 0 END)                  AS pay_num
           , NVL(SUM(CASE WHEN member_status = 1 THEN pay_num ELSE 0 END) /
                 SUM(CASE WHEN member_status = 1 THEN user_num ELSE 0 END), 0)         AS conv
           , SUM(CASE WHEN member_status = 1 THEN pay_sum ELSE 0 END)                  AS pay_sum
           , NVL(SUM(CASE WHEN member_status = 1 THEN pay_sum ELSE 0 END) /
                 SUM(CASE WHEN member_status = 1 THEN NVL(cac_real, 0) ELSE 0 END), 0) AS roi
           , SUM(CASE WHEN member_status = 1 THEN wx_rel_status ELSE 0 END)            AS wx_num            -- 老加微
           , SUM(CASE WHEN member_status = 1 THEN wx_active ELSE 0 END)                AS wx_active         -- 老主动加微
           , SUM(CASE WHEN member_status = 1 THEN ifcollect ELSE 0 END)                AS ifcollect
           , SUM(CASE WHEN member_status = 1 THEN collect_active ELSE 0 END)           AS collect_active
           , SUM(CASE WHEN member_status = 1 THEN interest ELSE 0 END)                 AS interest
           , SUM(CASE WHEN member_status = 1 THEN influence ELSE 0 END)                AS influence
           , SUM(CASE WHEN member_status = 1 THEN sex ELSE 0 END)                      AS sex
           , SUM(CASE WHEN member_status = 1 THEN age_45 ELSE 0 END)                   AS age_45
           , SUM(CASE WHEN member_status = 1 THEN age_50 ELSE 0 END)                   AS age_50
           , SUM(CASE WHEN member_status = 1 THEN city ELSE 0 END)                     AS city
           , SUM(CASE WHEN member_status = 1 THEN wx_rel_status_new ELSE 0 END)        AS wx_rel_status_new -- 新加微
           , SUM(CASE WHEN member_status = 1 THEN wx_active_new ELSE 0 END)            AS wx_active_new     -- 新主动加微
           , SUM(exp_user_status)                                                      AS exp_user_num      -- 支付成功
           , SUM(IF(b.oldid IS NOT NULL, 1, 0))                                        AS old_user
           , SUM(IF(c.oldid IS NOT NULL, 1, 0))                                        AS old_user_30
           , SUM(CASE WHEN member_status = 1 THEN ifcome0 ELSE 0 END)                  AS ifcome0
           , SUM(CASE WHEN member_status = 1 THEN ifok0 ELSE 0 END)                    AS ifok0
           , SUM(CASE WHEN member_status = 1 THEN ifcome1 ELSE 0 END)                  AS ifcome1
           , SUM(CASE WHEN member_status = 1 THEN ifok1 ELSE 0 END)                    AS ifok1
           , SUM(CASE WHEN member_status = 1 THEN ifcome2 ELSE 0 END)                  AS ifcome2
           , SUM(CASE WHEN member_status = 1 THEN ifok2 ELSE 0 END)                    AS ifok2
           , SUM(CASE WHEN member_status = 1 THEN ifcome3 ELSE 0 END)                  AS ifcome3
           , SUM(CASE WHEN member_status = 1 THEN ifok3 ELSE 0 END)                    AS ifok3
           , SUM(CASE WHEN member_status = 1 THEN ifcome4 ELSE 0 END)                  AS ifcome4
           , SUM(CASE WHEN member_status = 1 THEN ifok4 ELSE 0 END)                    AS ifok4

      FROM mid a
               LEFT JOIN old_user b
                         ON a.member_id = b.oldid
                             AND a.goods_name = b.goods_name
               LEFT JOIN old_user_30 c
                         ON a.member_id = c.oldid
                             AND a.goods_name = c.goods_name
      WHERE SUBSTR(goods_name_period, 1, 6) >= DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP(), 60), 'yyMMdd')
        AND SUBSTR(goods_name_period, 1, 6) NOT IN ('240624', '240710')
      GROUP BY cat
             , goods_name_period
             , platform_name
             , pos
             , price
             , h5_id
             , cost_id2) t1
         JOIN
     (SELECT DISTINCT pos
                    , pos_1
      FROM ods.ods_pos_transform
      WHERE dt = DATE_SUB(CURRENT_DATE, 1)
        AND pos_1 IN
            (
             '抖音信息流', '抖音直播', '视频号直播'
                )) t2
     ON t1.pos = t2.pos
;


