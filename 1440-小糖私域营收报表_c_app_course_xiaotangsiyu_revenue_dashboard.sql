SET mapred.job.name="c_app_course_xiaotangsiyu_revenue_dashboard#${datebuf}";
USE app;
CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_course_xiaotangsiyu_revenue_dashboard
(
    d_date          string COMMENT '分区日期',
    dims            string COMMENT '时间维度(月/周)',
    month_week      string COMMENT '支付时间区间',
    h5_id           string COMMENT 'h5_id',
    title           string COMMENT '渠道来源',
    pos             string COMMENT '版位',
    course          string COMMENT '课程',
    pay_num         float COMMENT '订单数(单)',
    pay_sum         float COMMENT '营收(元)',
    refund_sum      float COMMENT '退款(元)'
)
    COMMENT '培训主题数仓-小糖私域营收报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_xiaotangsiyu_revenue_dashboard';

SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;
SET hive.execution.engine = tez;
DROP TABLE pay_data;
CREATE TEMPORARY TABLE pay_data AS
WITH user_info AS
    ( -- 所有例子的投放属性&加微、问卷状态&购买正价课状态
        SELECT a.member_id
             , a.cat
             , a.platform
             , a.platform_name
             , a.h5_id
             , c.title
             , a.p_source
             , a.pos
             , a.price
             , a.mobile
             , a.link_type_v1
             , CASE WHEN a.link_type_v2 = '新一键' THEN '新一键授权' ELSE a.link_type_v2 END AS link_type_v2
             , a.cost_id
             , a.ad_id
             , a.sucai_id
             , a.created_at
             , SUBSTR(b.first_pay_time, 1, 10)                                       AS pay_time --支付时间
             , a.wx_rel_status
             , a.goods_name
             , a.xe_id
             , a.ifcollect
             , a.trade_state
             , a.member_status
             , a.sales_id
             , IF(b.user_id IS NOT NULL, 1, 0)                                          ifbuy
             , NVL(b.pay_num, 0)                                                        pay_num
             , NVL(b.pay_sum, 0)                                                        pay_sum
             , IF(wx_rel_status IN (2, 3) AND UNIX_TIMESTAMP(a.wx_add_time) - UNIX_TIMESTAMP(a.created_at) <= 300, 1,
                  0)                                                                    wx_active
             , IF(ifcollect = 1 AND UNIX_TIMESTAMP(a.collect_time) - UNIX_TIMESTAMP(a.wx_add_time) <= 3600, 1,
                  0)                                                                    collect_active
        FROM dw.dwd_xt_user a
                 LEFT JOIN (SELECT * FROM dws.dws_sale_buy_course_day WHERE dt = '${datebuf}') b
                           ON a.xe_id = b.user_id
                               AND a.special_id = b.owner_class
                 LEFT JOIN (SELECT h5_id
                                 , title
                            FROM (SELECT h5_id
                                       , title
                                       , ROW_NUMBER() OVER (PARTITION BY h5_id ORDER BY updated_at DESC) row_1 --取更新后的最新一条
                                  FROM dim.dim_place_h5
                                  WHERE TO_DATE(updated_at) <= '${datebuf}') tt
                            WHERE row_1 = 1) c
                           ON a.h5_id = c.h5_id
        WHERE a.dt = '${datebuf}'
          AND TO_DATE(a.created_at) <= '${datebuf}'
          AND a.platform_name = '小糖私域'
          AND pos NOT IN ('TMK往期召回')
          --AND a.goods_name RLIKE '太极|柔骨活血'
          AND a.h5_id <> 1
    )
   , mid AS
    (SELECT TO_DATE(pay_time) AS d_date  --取支付时间
          , pos
          , h5_id
          , title
          , case when goods_name like '%武当秘传太极养生功%' then '武当秘传太极养生功训练营'
                 when goods_name like '%气血能量瑜伽养生%' then '气血能量瑜伽养生训练营'
                 when goods_name like '%中医脏腑清毒%' then '中医脏腑清毒训练营'
                 when goods_name like '%知否知否·舞蹈训练营%' then '知否知否·舞蹈训练营'
                 when goods_name like '%6天经络瑜伽%' then '6天经络瑜伽体验营'
                 when goods_name like '%4天面部三维驻颜体验营%' then '4天面部三维驻颜体验营'
                 when goods_name like '%道门八段锦%' then '道门八段锦训练营'
                 when goods_name like '%太极复学%' then '太极复学训练营'
                 when goods_name like '%女儿情·舞蹈训练营%' then '女儿情·舞蹈训练营'
            end AS course
          , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_num,
                   0))        AS pay_num -- 正价课订单数
          , SUM(IF(member_status = 1 AND trade_state IN ('SUCCESS', 'PREPARE') AND sales_id > 0, pay_sum,
                   0))        AS pay_sum
     FROM user_info
     WHERE pay_time <> ''
     GROUP BY TO_DATE(pay_time)
            , pos
            , h5_id
            , title
            , case when goods_name like '%武当秘传太极养生功%' then '武当秘传太极养生功训练营'
                   when goods_name like '%气血能量瑜伽养生%' then '气血能量瑜伽养生训练营'
                   when goods_name like '%中医脏腑清毒%' then '中医脏腑清毒训练营'
                   when goods_name like '%知否知否·舞蹈训练营%' then '知否知否·舞蹈训练营'
                   when goods_name like '%6天经络瑜伽%' then '6天经络瑜伽体验营'
                   when goods_name like '%4天面部三维驻颜体验营%' then '4天面部三维驻颜体验营'
                   when goods_name like '%道门八段锦%' then '道门八段锦训练营'
                   when goods_name like '%太极复学%' then '太极复学训练营'
                   when goods_name like '%女儿情·舞蹈训练营%' then '女儿情·舞蹈训练营'
              end
    )
SELECT *
FROM mid
;

--退款GMV计算
DROP TABLE refund_data;
CREATE TEMPORARY TABLE refund_data AS
WITH user_info AS
    ( -- 所有例子的投放属性&加微、问卷状态&购买正价课状态
        SELECT a.member_id
             , a.cat
             , a.platform
             , a.platform_name
             , a.h5_id
             , c.title
             , a.p_source
             , a.pos
             , a.price
             , a.mobile
             , a.link_type_v1
             , CASE WHEN a.link_type_v2 = '新一键' THEN '新一键授权' ELSE a.link_type_v2 END AS link_type_v2
             , a.cost_id
             , a.ad_id
             , a.sucai_id
             , a.created_at
             , SUBSTR(b.refund_time, 1, 10)                                          AS refund_time --退款时间
             , a.wx_rel_status
             , a.goods_name
             , a.xe_id
             , a.ifcollect
             , a.trade_state
             , a.member_status
             , a.sales_id
             , IF(b.user_id IS NOT NULL, 1, 0)                                          ifbuy
             , NVL(b.refund_price, 0)                                                   refund_price
             , IF(wx_rel_status IN (2, 3) AND UNIX_TIMESTAMP(a.wx_add_time) - UNIX_TIMESTAMP(a.created_at) <= 300, 1,
                  0)                                                                    wx_active
             , IF(ifcollect = 1 AND UNIX_TIMESTAMP(a.collect_time) - UNIX_TIMESTAMP(a.wx_add_time) <= 3600, 1,
                  0)                                                                    collect_active
        FROM dw.dwd_xt_user a
                 LEFT JOIN ( --退费信息
            SELECT a.user_id,
                   a.owner_class,
                   b.refund_time,
                   b.refund_price
            FROM (
                     SELECT *
                     FROM ods.ods_xiaoe_order_dt
                     WHERE dt = '${datebuf}'
                       AND resource_type IN (8, 100008)
                       AND order_state IN (1, 10, 11)
                       AND xiaoe_order_type != '开放API导入订单'
                       AND refund_money > 0
                 ) a
                     LEFT JOIN
                 (
                     SELECT xiaoe_order_xe_id
                          , MAX(refund_time)  AS refund_time
                          , SUM(refund_price) AS refund_price
                     FROM ods.ods_xiaoe_order_refund_info
                     WHERE state = 2 --退款成功
                     GROUP BY xiaoe_order_xe_id
                 ) b
                 ON a.xe_id = b.xiaoe_order_xe_id) b
                           ON a.xe_id = b.user_id
                               AND a.special_id = b.owner_class
                 LEFT JOIN (SELECT h5_id
                                 , title
                            FROM (SELECT h5_id
                                       , title
                                       , ROW_NUMBER() OVER (PARTITION BY h5_id ORDER BY updated_at DESC) row_1 --取更新后的最新一条
                                  FROM dim.dim_place_h5
                                  WHERE TO_DATE(updated_at) <= '${datebuf}') tt
                            WHERE row_1 = 1) c
                           ON a.h5_id = c.h5_id
        WHERE a.dt = '${datebuf}'
          AND TO_DATE(a.created_at) <= '${datebuf}'
          AND a.platform_name = '小糖私域'
          AND pos NOT IN ('TMK往期召回')
          --AND a.goods_name RLIKE '太极|柔骨活血'
          AND a.h5_id <> 1
    )

   , mid AS
    (SELECT TO_DATE(refund_time) AS d_date --取退款支付时间
          , pos
          , h5_id
          , title
          , case when goods_name like '%武当秘传太极养生功%' then '武当秘传太极养生功训练营'
                 when goods_name like '%气血能量瑜伽养生%' then '气血能量瑜伽养生训练营'
                 when goods_name like '%中医脏腑清毒%' then '中医脏腑清毒训练营'
                 when goods_name like '%知否知否·舞蹈训练营%' then '知否知否·舞蹈训练营'
                 when goods_name like '%6天经络瑜伽%' then '6天经络瑜伽体验营'
                 when goods_name like '%4天面部三维驻颜体验营%' then '4天面部三维驻颜体验营'
                 when goods_name like '%道门八段锦%' then '道门八段锦训练营'
                 when goods_name like '%太极复学%' then '太极复学训练营'
                 when goods_name like '%女儿情·舞蹈训练营%' then '女儿情·舞蹈训练营'
            end  AS course
          , SUM(IF(refund_price > 0 AND refund_price != 500000, refund_price / 100,
                   0))           AS refund_sum
     FROM user_info
     WHERE refund_time <> ''
     GROUP BY TO_DATE(refund_time)
            , pos
            , h5_id
            , title
            , case when goods_name like '%武当秘传太极养生功%' then '武当秘传太极养生功训练营'
                   when goods_name like '%气血能量瑜伽养生%' then '气血能量瑜伽养生训练营'
                   when goods_name like '%中医脏腑清毒%' then '中医脏腑清毒训练营'
                   when goods_name like '%知否知否·舞蹈训练营%' then '知否知否·舞蹈训练营'
                   when goods_name like '%6天经络瑜伽%' then '6天经络瑜伽体验营'
                   when goods_name like '%4天面部三维驻颜体验营%' then '4天面部三维驻颜体验营'
                   when goods_name like '%道门八段锦%' then '道门八段锦训练营'
                   when goods_name like '%太极复学%' then '太极复学训练营'
                   when goods_name like '%女儿情·舞蹈训练营%' then '女儿情·舞蹈训练营'
                end
    )
SELECT *
FROM mid
;


DROP TABLE merge_data;
CREATE TEMPORARY TABLE merge_data AS
SELECT NVL(a.d_date, rd.d_date) AS d_date
     , NVL(a.pos, rd.pos)       AS pos
     , NVL(a.h5_id, rd.h5_id)   AS h5_id
     , NVL(a.title, rd.title)   AS title
     , NVL(a.course, rd.course) AS course
     , NVL(pay_num, 0)          AS pay_num
     , NVL(pay_sum, 0)          AS pay_sum
     , NVL(rd.refund_sum, 0)    AS refund_sum
FROM pay_data a
         FULL JOIN refund_data rd ON a.title = rd.title
    AND a.d_date = rd.d_date
    AND a.pos = rd.pos
    AND a.course = rd.course
    AND a.h5_id = rd.h5_id
;

DROP TABLE result_data;
CREATE TEMPORARY TABLE result_data AS
--周维度
SELECT '${datebuf}'          AS d_date1
     , '周'                   AS dims
     , NVL(CONCAT(DATE_ADD(d_date, 1 - CASE WHEN DAYOFWEEK(d_date) = 1 THEN 7 ELSE DAYOFWEEK(d_date) - 1 END), '至',
                  DATE_ADD(d_date, 7 - CASE WHEN DAYOFWEEK(d_date) = 1 THEN 7 ELSE DAYOFWEEK(d_date) - 1 END)),
           'ALL-汇总')         AS month_week
     , NVL(h5_id, 'ALL-汇总')  AS h5_id
     , NVL(title, 'ALL-汇总')  AS title
     , NVL(pos, 'ALL-汇总')    AS pos
     , NVL(course, 'ALL-汇总') AS course
     , SUM(pay_num)          AS pay_num --正价课订单数
     , SUM(pay_sum)          AS pay_sum --正价课GMV
     , SUM(refund_sum)       AS refund_sum
FROM merge_data
GROUP BY CONCAT(DATE_ADD(d_date, 1 - CASE WHEN DAYOFWEEK(d_date) = 1 THEN 7 ELSE DAYOFWEEK(d_date) - 1 END), '至',
                DATE_ADD(d_date, 7 - CASE WHEN DAYOFWEEK(d_date) = 1 THEN 7 ELSE DAYOFWEEK(d_date) - 1 END))
       , h5_id
       , title
       , pos
       , course
    GROUPING SETS (
       (CONCAT(DATE_ADD(d_date, 1 - CASE WHEN DAYOFWEEK(d_date) = 1 THEN 7 ELSE DAYOFWEEK(d_date) - 1 END), '至',
               DATE_ADD(d_date, 7 - CASE WHEN DAYOFWEEK(d_date) = 1 THEN 7 ELSE DAYOFWEEK(d_date) - 1 END))
       , h5_id, title, pos, course)
       , (CONCAT(DATE_ADD(d_date, 1 - CASE WHEN DAYOFWEEK(d_date) = 1 THEN 7 ELSE DAYOFWEEK(d_date) - 1 END), '至',
                 DATE_ADD(d_date, 7 - CASE WHEN DAYOFWEEK(d_date) = 1 THEN 7 ELSE DAYOFWEEK(d_date) - 1 END)))
    )

UNION ALL
--月维度
SELECT '${datebuf}'                                                      AS d_date1
     , '月'                                                               AS dims
     , NVL(CONCAT(TRUNC(d_date, 'MM'), '至', LAST_DAY(d_date)), 'ALL-汇总') AS month_week -- 月份范围
     , NVL(h5_id, 'ALL-汇总')                                              AS h5_id
     , NVL(title, 'ALL-汇总')                                              AS title
     , NVL(pos, 'ALL-汇总')                                                AS pos
     , NVL(course, 'ALL-汇总')                                             AS course
     , SUM(pay_num)                                                      AS pay_num     -- 正价课订单数
     , SUM(pay_sum)                                                      AS pay_sum     -- 正价课GMV
     , SUM(refund_sum)                                                   AS refund_sum
FROM merge_data
GROUP BY CONCAT(TRUNC(d_date, 'MM'), '至', LAST_DAY(d_date)) -- 按月份分组
       , h5_id
       , title
       , pos
       , course
    GROUPING SETS (
       (CONCAT(TRUNC(d_date, 'MM'), '至', LAST_DAY(d_date)), h5_id, title, pos, course)
       , (CONCAT(TRUNC(d_date, 'MM'), '至', LAST_DAY(d_date)))
    )
;


INSERT OVERWRITE TABLE app.c_app_course_xiaotangsiyu_revenue_dashboard PARTITION(dt = '${datebuf}')
SELECT
*
from result_data