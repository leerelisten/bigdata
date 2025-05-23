-- 例子：正常订单+换号（产生新price=0订单，无法剔除老号）+延期（不会产生新订单，但延期后的课程需要从delay表取数据）
-- 购买：正常购买
-- 延期：xiaoe_order会有延期前、延期后两条记录；vip_member为一条，信息仍是延期前记录；信息从delay表获取。
-- 异常换号：xiaoe_order有一条新号记录,vip_member里面没有，将该信息加入。班主任信息从哪里获得？
-- 代码逻辑：拿筑基用户(xiaoe_order)关联炼气订单(xiaoe_order)，其中筑基用户中包含延期(延期的订单信息从delay表获取)，获取二转转化率等指标。

-- TODO 1.延期前退款无法统计
--  2.换号场景无法剔除换号前的号码
--  3.异常换号(导入)场景，由于未分班，无法统计班主任


SET mapred.job.name="c_app_erzhuan_period_sale_dashboard#${datebuf}";
USE app;
CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_erzhuan_period_sale_dashboard
(
    goods_name         string COMMENT '期次名称',
    department         STRING COMMENT '部门',
    user_group         STRING COMMENT '组',
    sale_name          STRING COMMENT '班主任',
    v1_all_user_num    INT COMMENT '筑基营所有学员数(个)',
    v1_order_num       INT COMMENT '筑基营订单数(个)',
    v1_delay_user_num  INT COMMENT '筑基营延期学员数(个)',
    v1_refund_user_num INT COMMENT '筑基营退款学员数(个)',
    v1_refund_rate     FLOAT COMMENT '筑基营退费率(%)',
    v1_normal_user_num INT COMMENT '筑基营上课学员数(个)',

    v2_pay_num         INT COMMENT '二转订单个数(含退款)',
    v2_pay_sum         INT COMMENT '二转总GMV(元)(含退款)',
    v2_refund_num      INT COMMENT '二转退款订单个数',
    v2_refund_sum      INT COMMENT '二转退款GMV(元)',
    toatal_pay_num     INT COMMENT '二转订单数',
    toatal_pay_sum     INT COMMENT '二转GMV(元)',
    conv_rate          FLOAT COMMENT '转化率(%)',

    permission         STRING COMMENT '权限用户'
)
    COMMENT '二转期次销售结果报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_erzhuan_period_sale_dashboard';

SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;



--20250110  添加处理换号后数据
DROP TABLE IF EXISTS different_phase;
CREATE TEMPORARY TABLE different_phase AS
--把换号号对应的期次不同的数据拿出来
SELECT a.relate_member_id, -- 新会员ID
       a.from_xiaoe_user_id,
       a.special_id,
       b.xiaoe_order_resource_id
FROM (
         SELECT relate_member_id, -- 新会员ID
                member_id as xiaoe_member_id,  -- 老会员ID
                from_xiaoe_user_id,
                special_id
         FROM dwd.dwd_crm_vip_change_account_record -- 换号表
         WHERE dt = '${datebuf}'
           AND reason NOT LIKE '%测试%'
     ) a
         LEFT JOIN
     (
         SELECT class_sales_id  -- 班主任id
              , xiaoe_member_id -- 用户id
              , xiaoe_order_resource_id
              , class_id        -- 班级id
         FROM ods.ods_xiaoe_vip_member
     ) b
     ON a.relate_member_id = b.xiaoe_member_id
WHERE a.special_id <> b.xiaoe_order_resource_id --拿出换号后期次 换号后期次要与换号中对应的期次不同
;


--拼接上换号前后都相同的期次 找出换号后对应的期次
DROP TABLE IF EXISTS change_account_data;
CREATE TEMPORARY TABLE change_account_data AS
SELECT relate_member_id
     , from_xiaoe_user_id
     , xiaoe_order_resource_id AS special_id
FROM different_phase

UNION ALL
SELECT a.relate_member_id, -- 新会员ID
       a.from_xiaoe_user_id,
       a.special_id
FROM (
         SELECT relate_member_id, -- 新会员ID
                member_id as xiaoe_member_id,  -- 老会员ID
                from_xiaoe_user_id,
                special_id
         FROM dwd.dwd_crm_vip_change_account_record -- 换号表
         WHERE dt = '${datebuf}'
           AND reason NOT LIKE '%测试%'
     ) a
         LEFT JOIN different_phase b
                   ON a.relate_member_id = b.relate_member_id
WHERE b.relate_member_id IS NULL
;


-- 例子1
-- 正常+延期+正常换号:vip_member里不会生成新纪录
DROP TABLE IF EXISTS normal_member;
CREATE TEMPORARY TABLE normal_member AS
SELECT normal_user.user_id
     , normal_user.resource_id                       AS special_id
     , normal_user.member_id
     , NVL(dl2.new_class_sale_id, vm.class_sales_id) AS class_sales_id
     , normal_user.created_at
     , normal_user.refund_created_at
     , normal_user.refund_money
     , normal_user.order_state
     , normal_user.is_delay
FROM (SELECT od.user_id
           , xm.id AS                       member_id
           , od.resource_id
           , od.created_at
           , od.refund_created_at
           , od.refund_money
           , od.order_state
           , IF(dl.member_id IS NULL, 0, 1) is_delay
      -- 先关联delay表，获取学院是否延期信息。
      FROM (SELECT *
            FROM ods.ods_xiaoe_order_dt
            WHERE dt = '${datebuf}'
              AND resource_type IN (8, 100008)
              AND order_state IN (1, 10, 11)
              AND xiaoe_order_type != '开放API导入订单'
              AND price > 0) od
               LEFT JOIN ods.ods_xiaoe_member xm
                         ON od.user_id = xm.xe_id
          -- 关联出延期前订单
               LEFT JOIN
           ods.ods_crm_vip_apply_delay dl
           ON od.resource_id = dl.curr_class
               AND xm.id = dl.member_id
         -- 需要计算延期率，故原本那条要保留
--       WHERE dl.member_id IS NULL
     ) normal_user
         -- 再关联一次delay表，如果od表的resource_id是被延期之后的，则以delay表的延期班主任信息为主。
         LEFT JOIN
     ods.ods_crm_vip_apply_delay dl2
     ON normal_user.resource_id = dl2.new_class
         AND normal_user.member_id = dl2.member_id
         -- 正常订单关联vip_member，获取班主任信息。
         LEFT JOIN ods.ods_xiaoe_vip_member vm
                   ON normal_user.member_id = vm.xiaoe_member_id AND
                      normal_user.resource_id = vm.xiaoe_order_resource_id
--     排除掉换号后member_id  例子跟订单逻辑保持一致保留换号前做关联
         left join change_account_data hh
                   ON normal_user.member_id = hh.relate_member_id
                       and normal_user.resource_id = hh.special_id
WHERE hh.relate_member_id IS NULL
  and hh.special_id  is null
;




-- 例子2
-- -- 异常换号（直接导入的）:异常换号场景，vip_member里没有记录。取不到班主任，舍弃？
-- DROP TABLE  IF EXISTS  hh_member;
-- CREATE TEMPORARY TABLE hh_member AS
-- SELECT od.*
-- --     od.user_id
-- --      , od.resource_id AS special_id
-- --      , mb.id          AS member_id
-- --      , od.created_at
-- --      ,od.refund_created_at
-- --      ,od.refund_money
-- --      , od.order_state
-- --      , od.owner_id    AS class_sales_id
-- FROM (SELECT *
--       FROM ods.ods_xiaoe_order_dt
--       WHERE dt = '${datebuf}'
--         AND resource_type IN (8, 100008)
--         AND order_state IN (1, 10, 11)
--         -- 江哥确认换号场景，price=0
--         AND price = 0) od
--          LEFT JOIN
--      ods.ods_xiaoe_member mb
--      ON od.user_id = mb.xe_id
--          --TODO 换号+延期场景暂时无法统计
--          --去掉od里延期的数据:将xiaoe_order里面延期前的课程匹配上的，与延期后的课程匹配上的都剔除
--          LEFT JOIN ods.ods_crm_vip_apply_delay dl
--                    ON mb.id = dl.member_id
--                        AND od.resource_id = dl.curr_class
--          LEFT JOIN ods.ods_crm_vip_apply_delay dl2
--                    ON mb.id = dl2.member_id
--                        AND od.resource_id = dl2.new_class
--     -- 在vip_member里面没有记录的，才是没有走认领的异常换号
--          LEFT JOIN
--      ods.ods_xiaoe_vip_member vm
--      ON od.owner_class = vm.xiaoe_order_resource_id AND vm.xiaoe_member_id = mb.id
-- WHERE dl.member_id IS NULL
--   AND dl2.member_id IS NULL
--   AND vm.xiaoe_member_id IS NULL;


-- 例子关联购买，获得二转用户表
DROP TABLE IF EXISTS erzhuan_order;
CREATE TEMPORARY TABLE erzhuan_order AS
SELECT xc.type
     , xc.resource_id                    AS special_id
     , xc.title                          AS goods_name
     , nm.member_id
     , nm.user_id
     , nm.is_delay
     , nm.class_sales_id
     , ps.name                           AS sale_name
     , dept.name                         AS department
     , grp.name                          AS user_group
     , nm.created_at
     , nm.order_state
     , IF(od.pay_time IS NOT NULL, 1, 0) AS ifbuy
     , od.pay_time
     , od.price
     , od.resource_id
     , od.refund_created_at
     , od.refund_money
-- 1 取二筑基999-18 和1980-12例子
FROM (SELECT *
      FROM ods.ods_xiaoe_class
      WHERE type IN (12, 18)
        AND title NOT RLIKE '测试|定金|占座|老学员|回放|已下架') xc
         LEFT JOIN
     normal_member nm
     ON xc.resource_id = nm.special_id
         LEFT JOIN
     ods.ods_place_sales ps
     ON ps.id = nm.class_sales_id
         LEFT JOIN (SELECT * FROM ods.ods_crm_department_group WHERE parentid = 0) dept
                   ON ps.department = dept.id
         LEFT JOIN (SELECT * FROM ods.ods_crm_department_group WHERE parentid != 0) grp
                   ON ps.user_group = grp.id
    -- 2 购买炼气学员
         LEFT JOIN
     (
         SELECT
             DISTINCT CASE
                          WHEN hh.relate_member_id IS NOT NULL THEN hh.special_id
                          ELSE a.owner_class END AS owner_class,
                      CASE
                          WHEN hh.relate_member_id IS NOT NULL THEN hh.from_xiaoe_user_id
                          ELSE a.user_id
                          END                                                                            AS user_id,
                      a.pay_time,
                      a.price,
                      a.resource_id,
                      a.refund_created_at,
                      a.refund_money
         FROM (
                  SELECT *
                  FROM ods.ods_xiaoe_order_dt -- 订单表
                  WHERE dt = '${datebuf}'
                    AND resource_type IN (8, 100008)
                    AND order_state IN (1, 10, 11)
                    AND xiaoe_order_type != '开放API导入订单'
                    AND price > 0
              ) a
                  LEFT JOIN ods.ods_xiaoe_member b
                            ON a.user_id = b.xe_id
                  LEFT JOIN (
             SELECT relate_member_id, -- 新会员ID
                    from_xiaoe_user_id,
                    special_id
             FROM change_account_data --换号数据
         ) hh
                            ON b.id = hh.relate_member_id
                                and a.owner_class = hh.special_id
    ) od
     ON nm.special_id = od.owner_class
         AND nm.user_id = od.user_id
WHERE nm.user_id IS NOT NULL;


-- 汇总出报表   lv1-筑基   lv2-炼气
DROP TABLE IF EXISTS result;
CREATE TEMPORARY TABLE result AS
SELECT goods_name
     , department                                                              AS raw_department
     , user_group                                                              AS raw_user_group
     , SPLIT(user_group, '_')[0]                                               AS department
     , SPLIT(user_group, '_')[1]                                               AS user_group
     , sale_name
     , COUNT(user_id)                                                          AS lv1_all_user_num
     , SUM(IF(type = 12, 1, 0.5))                                              AS lv1_order_num
     , SUM(is_delay)                                                           AS lv1_delay_user_num
     , SUM(IF(order_state IN (10, 11), 1, 0))                                  AS lv1_refund_user_num
     , ROUND(SUM(IF(order_state IN (10, 11), 1, 0)) / COUNT(user_id), 4) * 100 AS lv1_refund_rate
     , COUNT(user_id) - SUM(is_delay) -
       SUM(IF(order_state IN (10, 11), 1, 0))                                  AS lv1_normal_user_num

     -- 二转订单
     , SUM(ifbuy)                                                              AS lv2_pay_num
     , SUM(ifbuy) * 3980                                                       AS lv2_pay_sum

-- 二转退款 ,把优惠券退款剔除
     , SUM(IF(refund_money > 0 AND (price != 8980 OR refund_money != 500000), 1,
              0))                                                              AS lv2_refund_num
     , SUM(IF(refund_money > 0 AND (price != 8980 OR refund_money != 500000), refund_money / 100,
              0))                                                              AS lv2_refund_sum

FROM erzhuan_order
GROUP BY goods_name
       , department
       , user_group
       , SPLIT(user_group, '_')[0]
       , SPLIT(user_group, '_')[1]
       , sale_name;



INSERT
    OVERWRITE
    TABLE app.c_app_erzhuan_period_sale_dashboard
    PARTITION
    (dt = '${datebuf}')
SELECT goods_name
     , IF(raw_department = '教学服务部', department, raw_department)        AS department
     , IF(raw_department = '教学服务部', user_group, raw_user_group)        AS user_group
     , sale_name
     , lv1_all_user_num
     , lv1_order_num
     , lv1_delay_user_num
     , lv1_refund_user_num
     , lv1_refund_rate
     , lv1_normal_user_num
     , lv2_pay_num
     , lv2_pay_sum
     , lv2_refund_num
     , lv2_refund_sum
     , lv2_pay_num - lv2_refund_num                                         AS toatal_pay_num
     , lv2_pay_sum - lv2_refund_sum                                         AS toatal_pay_sum
     , ROUND((lv2_pay_num - lv2_refund_num) / lv1_normal_user_num, 4) * 100 AS conv_rate
     , CONCAT(
        'fuchuanhong@tangdou.com,yinky@tangdou.com,zhangy@tangdou.com,lijiahang@tangdou.com,zhout@tangdou.com,wangdonglei@tangdou.com,zhengm@tangdou.com,tangwenqi@tangdou.com,liyifei@tangdou.com,kangbin@tangdou.com,shuchang@tangdou.com,wangxing@tangdou.com',
        '')                                                                 AS permission
FROM result;