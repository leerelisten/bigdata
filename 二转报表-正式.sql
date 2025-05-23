-- 逻辑3:
-- 例子：vip_member  购买课程：xiaoe_order

-- 例子=vip_member+换号

-- vip_member
DROP TABLE vip_member;
CREATE TEMPORARY TABLE vip_member AS
SELECT vm.xiaoe_order_resource_id AS special_id
     , mb.xe_id                   AS user_id
     , vm.xiaoe_member_id         AS member_id
     , vm.class_sales_id          AS sale_id
     , vm.created_at
FROM (SELECT *
      FROM ods.ods_xiaoe_class
      WHERE type IN (12, 18)
        AND title NOT RLIKE '测试|定金|占座|老学员|回放|已下架') xc
         LEFT JOIN
     ods.ods_xiaoe_vip_member vm
     ON xc.resource_id = vm.xiaoe_order_resource_id
         LEFT JOIN
     ods.ods_xiaoe_member mb
     ON vm.xiaoe_member_id = mb.id;


-- 获取换号人的情况
DROP TABLE hh_member;
CREATE TEMPORARY TABLE hh_member AS
SELECT od.owner_class AS special_id
     , od.user_id
     , mb.id          AS member_id
     , od.owner_id    AS sale_id
     , od.created_at
FROM (SELECT *
      FROM ods.ods_xiaoe_class
      WHERE type IN (12, 18)
        AND title NOT RLIKE '测试|定金|占座|老学员|回放|已下架') xc
         LEFT JOIN
     (SELECT *
      FROM ods.ods_xiaoe_order_dt
      WHERE dt = '2024-11-13'
        AND resource_type IN (8, 100008)
        AND order_state IN (1, 10, 11)
        AND xiaoe_order_type != '开放API导入订单'
        AND price > 0) od
     ON xc.resource_id = od.owner_class
         LEFT JOIN
     ods.ods_xiaoe_member mb
     ON od.user_id = mb.xe_id
         --去掉od里延期的数据
         LEFT JOIN ods.ods_crm_vip_apply_delay dl
                   ON mb.id = dl.member_id
                       AND od.owner_class = dl.curr_class
         LEFT JOIN
     ods.ods_xiaoe_vip_member vm
     ON od.owner_class = vm.xiaoe_order_resource_id AND vm.xiaoe_member_id = mb.id
WHERE dl.member_id IS NULL
  AND vm.xiaoe_member_id IS NULL
;



DROP TABLE way_3;
CREATE TEMPORARY TABLE way_3 AS
SELECT xc.type
     , xc.resource_id                    AS special_id
     , xc.title                          AS goods_name
     , vm.member_id
     , mb.xe_id                          AS user_id
     , vm.sale_id
     , ps.name                           AS sale_name
     , ps.department
     , ps.user_group
     , vm.created_at
     , IF(od.pay_time IS NOT NULL, 1, 0) AS ifbuy
     , od.pay_time
     , od.price
     , od.resource_id
     , od.refund_created_at
     , od.refund_money
FROM (SELECT *
      FROM ods.ods_xiaoe_class
      WHERE type IN (12, 18)
        AND title NOT RLIKE '测试|定金|占座|老学员|回放|已下架') xc
         LEFT JOIN
     (SELECT *
      FROM vip_member
      UNION ALL
      SELECT *
      FROM hh_member) vm
     ON xc.resource_id = vm.special_id
         LEFT JOIN
     ods.ods_xiaoe_member mb
     ON vm.member_id = mb.id
         LEFT JOIN
     -- 购买
         (SELECT *
          FROM ods.ods_xiaoe_order_dt
          WHERE dt = '2024-11-13'
            AND resource_type IN (8, 100008)
            AND order_state IN (1, 10, 11)
            AND xiaoe_order_type != '开放API导入订单'
            AND price > 0) od
     ON vm.special_id = od.owner_class
         AND mb.xe_id = od.user_id
         LEFT JOIN
     ods.ods_place_sales ps
     ON ps.id = vm.sale_id;


-- 最终清单表
CREATE TABLE dw.tdlive_erzhuan_user_list AS
SELECT SUBSTR(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTR(goods_name, 2, 6), 'yyMMdd'), 'yyyy-MM-dd'),
                       IF(type = 12, 28, 21)), 1, 7) AS belong_month
     , *
FROM way_3;





-- 当期 TODO
DROP TABLE current_period;
CREATE TEMPORARY TABLE current_period AS
SELECT sale_id
     , sale_name
     , COUNT(*)          AS user_num
     , SUM(ifbuy)        AS pay_num
     , SUM(ifbuy) * 3980 AS pay_sum
FROM dw.tdlive_erzhuan_user_list
WHERE belong_month = '2024-10'
  AND SUBSTR(pay_time, 1, 7) <= '2024-10'
GROUP BY sale_id, sale_name;


-- 往期 TODO
DROP TABLE bf_period;
CREATE TEMPORARY TABLE bf_period AS
SELECT sale_id
     , sale_name
     , SUM(ifbuy)        AS pay_num
     , SUM(ifbuy) * 3980 AS pay_sum
FROM dw.tdlive_erzhuan_user_list
WHERE belong_month < '2024-10'
  AND SUBSTR(pay_time, 1, 7) = '2024-10'
GROUP BY sale_id, sale_name;


--退费 TODO
DROP TABLE refund_list;
CREATE TEMPORARY TABLE refund_list AS
SELECT sale_id
     , sale_name
     , COUNT(1)                AS refund_cnt
     , SUM(refund_money / 100) AS refund_money
FROM dw.tdlive_erzhuan_user_list
-- 剔除优惠券退款情况
WHERE SUBSTR(refund_created_at, 1, 7) = '2024-10'
  AND (price != 8980
    OR refund_money != 500000)
GROUP BY sale_id, sale_name;


-- 生成报表
DROP TABLE result;
CREATE TEMPORARY TABLE result AS
SELECT a.sale_id
     , a.sale_name
     , a.user_num
     , a.pay_num
     , a.pay_sum
     , b.pay_num                                                                           AS before_pay_num
     , b.pay_sum                                                                           AS before_pay_sum
     , c.refund_cnt
     , c.refund_money
     , (a.pay_num + NVL(b.pay_num, 0) - NVL(c.refund_cnt, 0))                              AS total_pay_num
     , (a.pay_sum + NVL(b.pay_sum, 0) - NVL(c.refund_money, 0))                            AS total_pay_sum
     , ROUND((a.pay_num + NVL(b.pay_num, 0) - NVL(c.refund_cnt, 0)) / a.user_num, 2) * 100 AS conv_rate
FROM current_period a
         LEFT JOIN bf_period b
                   ON a.sale_id = b.sale_id
         LEFT JOIN refund_list c
                   ON a.sale_id = c.sale_id;

SELECT *
FROM result;
