

CREATE TABLE IF NOT EXISTS dwd.dwd_erzhuan_performance_refund_order
(
    order_period_name                STRING COMMENT '成单期次',
    member_id                        STRING COMMENT '用户id',
    h5_id                            STRING COMMENT 'h5id',
    user_id                          STRING COMMENT '用户小鹅通ID',
    contact_ex_nickname              STRING COMMENT '小鹅通昵称',
    mobile                           STRING COMMENT '手机号',
    created_at                       string COMMENT '下单时间',
    pay_time                         string COMMENT '支付时间',
    resource_id                      STRING COMMENT '商品ID',
    goods_name                       STRING COMMENT '商品',
    link_adress                      STRING COMMENT '专属链接',
    pay_way                          STRING COMMENT '支付方式',
    should_pay_price                 DECIMAL(18, 2) COMMENT '应收金额',
    price                            DECIMAL(18, 2) COMMENT '实收金额',
    platform                         STRING COMMENT '渠道',
    transaction_id                   STRING COMMENT '支付交易单号',
    out_order_id                     STRING COMMENT '支付商户单号',
    xe_order_id                      STRING COMMENT '订单号ID',
    sales_name                       STRING COMMENT '订单归属人',
    assistant_order_type             STRING COMMENT '助手订单类型',
    department                       STRING COMMENT '部门',
    user_group                       STRING COMMENT '组',
    refund_money                     DECIMAL(18, 2) COMMENT '退款金额',
    refund_created_at                TIMESTAMP COMMENT '退款日期',
    order_status_description         STRING COMMENT '订单状态',
    order_type                       STRING COMMENT '订单类型',
    order_period                     STRING COMMENT '筑基营期次_1',
    is_surpass_current_month         STRING COMMENT '期次是否超过本月',
    refund_date                         STRING COMMENT '支付日期',
    sales_name_2                     STRING COMMENT '订单归属人-分列',
    is_current_month_classer         STRING COMMENT '当月期次+班主任',
    current_month_classer_start_date string COMMENT '当月班主任开始日期',
    current_month_classer_end_date   string COMMENT '当月班主任结束日期',
    refund_date_compare_current_month   STRING COMMENT '支付日期比较本月',
    is_current_month_teacher         STRING COMMENT '班主任是否本月',
    is_before_month_teacher          STRING COMMENT '班主任是否上月统计过',
    refund_date_compare                 STRING COMMENT '支付日期比较3月31日',
    if_tongji                        STRING COMMENT '是否应该统计',
    goods_category                   STRING COMMENT '商品名称分类',
    order_cnt                        DECIMAL(18, 2) COMMENT '订单数',
    order_amount                     DECIMAL(18, 2) COMMENT 'GMV',
    is_current_period                STRING COMMENT '当期往期标签',
    department_comment               STRING COMMENT '备注：部门'
) COMMENT '太极二转业绩退款订单'
    PARTITIONED BY (date_month STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE ;

--太极二转数据开发
--数据总表 仅做商品期次筛选
drop TABLE IF EXISTS base_data;
CREATE TEMPORARY TABLE base_data as
SELECT
    order_period_name
     ,member_id
     ,h5_id
     ,user_id
     ,contact_ex_nickname
     ,mobile
     ,created_at
     ,pay_time
     ,resource_id
     ,goods_name
     ,link_adress
     ,pay_way
     ,should_pay_price
     ,price
     ,platform
     ,transaction_id
     ,out_order_id
     ,xe_order_id
     ,sales_name
     ,assistant_order_type
     ,department
     ,user_group
     ,refund_money
     ,refund_created_at
     ,order_status_description
     ,order_type
     ,REGEXP_REPLACE(sales_name_2,'（新）','') as sales_name_2
     ,concat(SUBSTR(order_period_name, 4, 5),REGEXP_REPLACE(sales_name_2,'（新）','')) as goods_period_sales_name
     , (price-refund_money) as gap
from  dwd.dwd_edu_service_refund_orders
where   substr(refund_created_at,1,10) between '2025-03-01'  and '2025-04-30'
  and  goods_name rlike '炼气营|养正营'
  and  goods_name not rlike '测试|会员'
  AND order_status_description <> '交易关闭'
;

--班主任维表
drop TABLE IF EXISTS teacher_schedule_dim;
CREATE TEMPORARY TABLE teacher_schedule_dim  as
SELECT
    period_sales
     ,performance_period
     , concat('25',substr(performance_period,1,5)) as goods_period
     ,teacher
     ,department
     ,time_period
     ,start_date
     ,end_date
     ,performance_month
from ods.ods_teacher_schedule_dim
;


--处理过程数据 到支付日期比较3月31日 AK列
DROP TABLE IF EXISTS process_data;
CREATE TEMPORARY TABLE process_data AS
SELECT a.order_period_name
     , a.member_id
     , a.h5_id
     , a.user_id
     , a.contact_ex_nickname
     , a.mobile
     , a.created_at
     , a.pay_time
     , a.resource_id
     , a.goods_name
     , a.link_adress
     , a.pay_way
     , a.should_pay_price
     , a.price
     , a.platform
     , a.transaction_id
     , a.out_order_id
     , a.xe_order_id
     , a.sales_name
     , a.assistant_order_type
     , a.department
     , a.user_group
     , a.refund_money
     , a.refund_created_at
     , a.order_status_description
     , a.order_type
     , SUBSTR(a.order_period_name, 2, 7)                                AS order_period                     --筑基营期次
     , IF(SUBSTR(a.order_period_name, 2, 7) > b.goods_period, 'Y', 'N') AS is_surpass_current_month         --期次是否超过本月
     , SUBSTR(a.refund_created_at, 1, 10)                                        AS refund_date              --退款时间
     , a.sales_name_2                                                                                       --订单归属人 清洗后
     , IF(c.period_sales IS NOT NULL, 1, 0)                             AS is_current_month_classer         --是否当月班主任
     , IF(d.teacher IS NOT NULL, d.start_date, '9999-01-01')            AS current_month_classer_start_date --班主任开始日期
     , IF(d.teacher IS NOT NULL, d.end_date, '0000-01-01')              AS current_month_classer_end_date   --班主任结束日期
     , IF(SUBSTR(a.refund_created_at, 1, 10) >= IF(d.teacher IS NOT NULL, d.start_date, '9999-01-01') AND
          SUBSTR(a.refund_created_at, 1, 10) <= IF(d.teacher IS NOT NULL, d.end_date, '0000-01-01'), 1,
          0)                                                            AS refund_date_compare_current_month   --退款时间比较本月
     , IF(d.teacher IS NOT NULL, 1, 0)                                  AS is_current_month_teacher         --是否当月班主任
     , IF(e.teacher IS NOT NULL, 1, 0)                                  AS is_before_month_teacher          --班主任是否上个月统计过
     , IF(SUBSTR(a.refund_created_at, 1, 10) > '2025-03-31', 1, 0)               AS refund_date_compare                 --退款日期和上月底做比较  注：每个月需调整
     , IF(c.period_sales IS NOT NULL, c.department, 0)                  AS department_comment               --部门备注
FROM base_data a
         LEFT JOIN (
    --本月统计周期最大期次
    SELECT MAX(goods_period) AS goods_period
    FROM teacher_schedule_dim
    WHERE performance_month = '2025-04' --本月 注：每个月需调整

) b ON 1 = 1
         LEFT JOIN
     --获取期次销售信息
         (
             SELECT *
             FROM teacher_schedule_dim
             WHERE performance_month = '2025-04' --本月 注：每个月需调整
         ) c
     ON a.goods_period_sales_name = c.period_sales
         LEFT JOIN
     --获取期次开始日期和期次结束日期
         (
             SELECT *
             FROM teacher_schedule_dim
             WHERE performance_month = '2025-04' --本月 注：每个月需调整
         ) d
     ON a.sales_name_2 = d.teacher
         --班主任是否上个月统计过
         LEFT JOIN
     (
         SELECT *
         FROM teacher_schedule_dim
         WHERE performance_month = '2025-03' --上个月 注：每个月需调整
     ) e
     ON a.sales_name_2 = e.teacher
WHERE  a.gap not in (3980,2580,2189)
;

--数据插入底表
INSERT OVERWRITE TABLE dwd.dwd_erzhuan_performance_refund_order PARTITION (date_month = '2025-04')
SELECT order_period_name
     , member_id
     , h5_id
     , user_id
     , contact_ex_nickname
     , mobile
     , created_at
     , pay_time
     , resource_id
     , goods_name
     , link_adress
     , pay_way
     , should_pay_price
     , price
     , platform
     , transaction_id
     , out_order_id
     , xe_order_id
     , sales_name
     , assistant_order_type
     , department
     , user_group
     , refund_money
     , refund_created_at
     , order_status_description
     , order_type
     , order_period                                                                                                               --筑基营期次
     , is_surpass_current_month                                                                                                   --期次是否超过本月
     , refund_date                                                                                                                --支付时间
     , sales_name_2                                                                                                               --订单归属人 清洗后
     , is_current_month_classer                                                                                                   --是否当月班主任
     , IF(current_month_classer_start_date = '9999-01-01', '无',
          current_month_classer_start_date)                                                   AS current_month_classer_start_date --班主任开始日期
     , IF(current_month_classer_end_date = '0000-01-01', '无',
          current_month_classer_end_date)                                                     AS current_month_classer_end_date   --班主任结束日期
     , refund_date_compare_current_month                                                                                             --支付时间比较本月
     , is_current_month_teacher                                                                                                   --是否当月班主任
     , is_before_month_teacher                                                                                                    --班主任是否上个月统计过
     , refund_date_compare                                                                                                           --支付日期和上月底做比较  注：每个月需调整
     , if_tongji                                                                                                                  --是否应该统计
     , goods_category                                                                                                             --商品名称分类
     , order_cnt                                                                                                                  --订单数量
     , order_amount                                                                                                               --退款金额
     , IF(is_surpass_current_month = 'N' AND if_tongji = 1 AND goods_name RLIKE '炼气营|精选炼气营',
          IF(is_current_month_classer <> 0, '当期', '往期'),
          0)                                                                                  AS is_current_period                --当期往期标签
     , department_comment
FROM (
         SELECT order_period_name
              , member_id
              , h5_id
              , user_id
              , contact_ex_nickname
              , mobile
              , created_at
              , pay_time
              , resource_id
              , goods_name
              , link_adress
              , pay_way
              , should_pay_price
              , price
              , platform
              , transaction_id
              , out_order_id
              , xe_order_id
              , sales_name
              , assistant_order_type
              , department
              , user_group
              , refund_money
              , refund_created_at
              , order_status_description
              , order_type
              , order_period                                                     --筑基营期次
              , is_surpass_current_month                                         --期次是否超过本月
              , refund_date                                                         --退款时间
              , sales_name_2                                                     --订单归属人 清洗后
              , is_current_month_classer                                         --是否当月班主任
              , current_month_classer_start_date                                 --班主任开始日期
              , current_month_classer_end_date                                   --班主任结束日期
              , refund_date_compare_current_month                                   --支付时间比较本月
              , is_current_month_teacher                                         --是否当月班主任
              , is_before_month_teacher                                          --班主任是否上个月统计过
              , refund_date_compare                                                 --支付日期和上月底做比较  注：每个月需调整
              , IF(is_current_month_classer <> 0, 1,
                   IF(is_current_month_teacher <> 0 AND refund_date_compare_current_month <> 0,
                      IF(refund_date_compare <> 0, 1, IF(is_before_month_teacher = 0, 1, 0)),
                      0))                                      AS if_tongji      --是否应该统计
              , CASE
                    WHEN goods_name RLIKE '国际版' AND goods_name RLIKE '炼气营' AND goods_name RLIKE '精选' THEN '国际版-精选炼气营'
                    WHEN goods_name RLIKE '国际版' AND goods_name RLIKE '炼气营' THEN '国际版-炼气营'
                    WHEN goods_name RLIKE '炼气营' AND goods_name RLIKE '精选' THEN '精选炼气营'
                    WHEN goods_name RLIKE '炼气营' THEN '炼气营' END AS goods_category --商品名称分类
              , CASE
             --1980转化情况
                    WHEN order_period_name RLIKE '28天' AND price = '3980.0' THEN 1 --1980转3980
                    WHEN order_period_name RLIKE '28天' AND price = '2580.0' THEN 0.65 --1980转2580
                    WHEN order_period_name RLIKE '28天' AND price = '2180.0' THEN 0.55 --1980转2180

             --999营转化情况

                    WHEN order_period_name RLIKE '21天' AND price = '3980.0' THEN 1.55 --999转3980
                    WHEN order_period_name RLIKE '21天' AND price = '2580.0' THEN 1 --999转2580
                    WHEN order_period_name RLIKE '21天' AND price = '2180.0' THEN 0.55 --1980转2180
             END                                               AS order_cnt      --订单数量


              , refund_money                                          AS order_amount   --退款金额
              , department_comment
         FROM process_data
     ) a
;



