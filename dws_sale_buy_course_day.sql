CREATE TABLE IF NOT EXISTS dws.dws_sale_buy_course_day
(
    `user_id`               string COMMENT '用户id',
    `owner_class`           string COMMENT '小鹅通id',
    `pay_num`               decimal(22, 2) COMMENT '正价课订单总数',
    `pay_sum`               decimal(22, 2) COMMENT '正价课总GMV',
    `first_pay_time`        timestamp COMMENT '首次付款时间',
    `first_pay_num`         decimal(10, 2) COMMENT '首次下单订单数',
    `first_pay_sum`         decimal(22, 2) COMMENT '首次下单GMV',
    first_order_state       string COMMENT '首次下单状态',
    first_refund_money      decimal(10, 2) COMMENT '首次退款金额',
    first_refund_created_at timestamp COMMENT '首次退款时间'
)
    COMMENT '训练营学员购买正价课情况'
    PARTITIONED BY (dt STRING)
    STORED AS ORC;



CREATE TEMPORARY TABLE effective_order AS
SELECT *
FROM ods.ods_xiaoe_order_dt
WHERE dt = '${datebuf}'
  AND TO_DATE(pay_time) <= '${datebuf}'
  AND resource_type IN (8, 100008)
  AND order_state IN (1, 10, 11)
  AND xiaoe_order_type != '开放API导入订单'
  AND price > 0;


CREATE TEMPORARY TABLE total_buy AS
SELECT od.user_id
     , od.owner_class
     , SUM(pg.unit_pay_num) AS pay_num
     , SUM(pg.unit_pay_sum) AS pay_sum
FROM dw.dws_xiaoe_periods_goods_day pg
         INNER JOIN
     effective_order od
     ON od.resource_id = pg.xe_id
WHERE pg.xe_id IS NOT NULL
GROUP BY od.user_id
       , od.owner_class;



CREATE TEMPORARY TABLE first_buy AS
SELECT user_id
     , owner_class
     , pay_time                 AS first_pay_time
     , unit_pay_num             AS first_pay_num
     , unit_pay_sum             AS first_pay_sum
     , CASE order_state
           WHEN 0 THEN '未支付'
           WHEN 1 THEN '支付成功'
           WHEN 2 THEN '支付失败'
           WHEN 10 THEN '主动全部退款成功'
           WHEN 11 THEN '发起过部分退款'
           ELSE order_state END AS order_state
     , refund_money
     , refund_created_at
FROM (SELECT od.user_id
           , od.owner_class
           , od.pay_time
           , od.order_state
           , od.refund_money / 100                                                               AS refund_money
           , od.refund_created_at
           , pg.unit_pay_num
           , pg.unit_pay_sum
           , ROW_NUMBER() OVER (PARTITION BY od.user_id,od.owner_class ORDER BY od.pay_time ASC) AS rnum -- 如果同一期支付多次 只取首次支付成功时间
      FROM dw.dws_xiaoe_periods_goods_day pg
               INNER JOIN
           effective_order od
           ON od.resource_id = pg.xe_id
      WHERE pg.xe_id IS NOT NULL
        AND pg.goods_name RLIKE '筑基|变美|八段锦|中医三维驻颜|舞|瑜伽') a --古典舞 可能会有其他名称 这里调整为匹配舞
WHERE rnum = 1;


INSERT OVERWRITE TABLE dws.dws_sale_buy_course_day PARTITION (dt = '${datebuf}')
SELECT tb.user_id
     , tb.owner_class
     , tb.pay_num
     , tb.pay_sum
     , fb.first_pay_time
     , fb.first_pay_num
     , fb.first_pay_sum
     , fb.order_state       AS first_order_state
     , fb.refund_money      AS first_refund_money
     , fb.refund_created_at AS first_refund_created_at
FROM total_buy tb
         LEFT JOIN
     first_buy fb
     ON tb.user_id = fb.user_id
         AND tb.owner_class = fb.owner_class;



