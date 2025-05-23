DROP TABLE IF EXISTS dwd.dwd_edu_service_refund_orders;
CREATE TABLE IF NOT EXISTS dwd.dwd_edu_service_refund_orders
(
    order_period_name        STRING COMMENT '订单期次名称',
    member_id                STRING COMMENT '会员ID',
    h5_id                    STRING COMMENT 'H5 ID',
    user_id                  STRING COMMENT '用户ID',
    contact_ex_nickname      STRING COMMENT '用户昵称',
    mobile                   STRING COMMENT '手机号',
    created_at               STRING COMMENT '订单创建时间',
    pay_time                 STRING COMMENT '支付时间',
    resource_id              STRING COMMENT '资源ID',
    goods_name               STRING COMMENT '商品名称',
    link_adress              STRING COMMENT '专属链接',
    pay_way                  STRING COMMENT '支付方式',
    should_pay_price         string COMMENT '应付金额',
    price                    string COMMENT '实付金额',
    platform                 string COMMENT '渠道',
    transaction_id           STRING COMMENT '交易ID',
    out_order_id             STRING COMMENT '外部订单ID',
    xe_order_id              STRING COMMENT '系统订单ID',
    sales_name               STRING COMMENT '销售姓名',
    assistant_order_type     string COMMENT '助手订单类型',
    department               STRING COMMENT '部门',
    user_group               STRING COMMENT '用户组',
    refund_money             string COMMENT '退款金额',
    refund_created_at        string COMMENT '退款时间',
    order_status_description STRING COMMENT '订单状态描述',
    order_type               STRING COMMENT '订单类型',
    sales_name_2             STRING COMMENT '销售姓名(未拼接）'
)
    COMMENT '教服业绩-退款订单总表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE;

drop TABLE refund_order_data_all;
CREATE TEMPORARY TABLE refund_order_data_all as
WITH department_cte AS (
         -- 获取部门信息、组别、主题信息
         SELECT ps.id,
                dept.name AS dept_name,
                grp.name  AS grp_name,
                ps.name   AS sales_name,
                corp.name AS corp_name
         FROM ods.ods_place_sales ps
                  LEFT JOIN ods.ods_crm_department_group dept ON ps.department = dept.id
                  LEFT JOIN ods.ods_crm_department_group grp ON ps.user_group = grp.id
                  LEFT JOIN ods.ods_crm_wxwork_corps corp ON ps.corpid = corp.corpid)
SELECT
    c.goods_name as order_period_name
     , d.id  as member_id
     , a.h5_id
     , a.user_id
     , d.contact_ex_nickname
     , md5(d.mobile) as mobile
     , a.created_at  --下单时间
     , a.pay_time  --支付时间
     , a.resource_id  --商品id
     , CASE when a.resource_id RLIKE '^course' then '糖豆舞蹈私教课-月卡' else b.goods_name end as goods_name  --商品
     ,'' as link_adress   --专属链接  新增
     , CASE
           WHEN a.pay_way = 0 THEN '微信支付'
           WHEN a.pay_way = 1 THEN '支付宝支付'
           WHEN a.pay_way = 2 THEN 'IOS波币支付'
           WHEN a.pay_way = 3 THEN '安卓波币支付'
           WHEN a.pay_way = 4 THEN '线下支付'
           WHEN a.pay_way = 5 THEN '百度支付'
           WHEN a.pay_way = 6 THEN '微信收付通'
           WHEN a.pay_way = 14 THEN '支付宝-花呗分期'
           WHEN a.pay_way = 100 THEN '微信支付-豆豆直播'
           WHEN a.pay_way = 101 THEN '支付宝扫码'
           WHEN a.pay_way = 102 THEN '对公账户付款'
           WHEN a.pay_way = 103 THEN '拆单混合支付'
           WHEN a.pay_way = 203 THEN '微信h5支付'
           WHEN a.pay_way = 204 THEN '支付宝手机支付'
           WHEN a.pay_way = 205 THEN '支付宝花呗'
           WHEN a.pay_way = 206 THEN '微信公众号支付'
           WHEN a.pay_way = 207 THEN '余额支付'
           WHEN a.pay_way = 208 THEN '虚拟币支付'
           WHEN a.pay_way = 1000 THEN '空中云汇支付'
           ELSE a.pay_way
    END AS pay_way   --支付方式
     , ROUND(a.unit_price / 100, 2) as should_pay_price  --应付金额 新增
     , a.price   --实收金额
     , h5.platform  --渠道  新增
     , a.transaction_id  --交易id
     , a.out_order_id   --支付商单号
     , a.xe_order_id    --订单id
     , NVL(CONCAT(NVL(department_cte.dept_name,''), NVL(department_cte.grp_name,''), '_', NVL(department_cte.sales_name,''), '(',
                     a.owner_id, ')', '_',
                     REPLACE(NVL(department_cte.corp_name,''), '小糖乐学', '')), '未分配')  as sales_name
     , '' as assistant_order_type  --助手订单类型 新增
     , department_cte.dept_name as department
     , department_cte.grp_name  as user_group
     , (a.refund_money/100) as refund_money
     , a.refund_created_at
     ,  CASE a.order_state
            WHEN 0 THEN '未支付'
            WHEN 1 THEN '支付成功'
            WHEN 2 THEN '支付失败'
            WHEN 3 THEN '已退款'
            WHEN 4 THEN '预付款'
            WHEN 5 THEN '支付处理中'
            WHEN 6 THEN '过期自动取消'
            WHEN 7 THEN '用户手动取消'
            WHEN 8 THEN '主动全部退款中'
            WHEN 9 THEN '主动全部退款失败'
            WHEN 10 THEN '主动全部退款成功'
            WHEN 11 THEN '发起过部分退款'
            when 100 THEN '交易关闭'
            ELSE '未知状态'
     END AS order_status_description  --订单状态
     ,case when a.order_type = '1' then '系统订单'  when  a.order_type = '2' then '录入订单' when a.order_type = '3' then '创建订单' else '未知' end as order_type --订单类型
     ,  REGEXP_REPLACE(department_cte.sales_name, '[a-zA-Z0-9]', '') as sales_name_2
FROM (
         SELECT

             a.user_id
              , a.xe_order_id
              , a.out_order_id
              , a.resource_id
              , a.resource_type
              , a.transaction_id
              , a.unit_price
              , a.price
              , a.order_state
              , a.pay_time
              , a.refund_money
              , a.created_at
              , a.pay_way
              , a.refund_created_at
              , a.department
              , a.user_group
              , a.parent_orderno
              , a.parent_price
--               , a.department
              , a.h5_id
              ,a.order_type
--               , a.user_group
              , a.owner_class
              , a.owner_id
         FROM (
                  SELECT
                      user_id
                       , xe_id as xe_order_id
                       , out_order_id
                       , resource_id
                       , resource_type
                       , transaction_id
                       ,unit_price
                       , price
                       , order_state
                       , pay_time
                       , refund_money
                       , created_at
                       , pay_way
                       , refund_created_at
                       , department
                       , user_group
                       , parent_orderno
                       , parent_price
--                        , department
                       , h5_id
                       ,order_type
--                        , user_group
                       , owner_class
                       , owner_id
                  FROM ods.ods_xiaoe_order
                  WHERE
--                           refund_created_at >= '2025-03-17 00:00:00'
--                     AND refund_created_at <= '2025-04-30 23:59:59'
                        xe_app_id = 'appcafhwq5q8671' --太极
                    AND  resource_type in ('6', '100006', '8', '100008', '41')
              ) a
     ) a
         LEFT JOIN ods.ods_xiaoe_special b --购买商品
                   ON a.resource_id = b.xe_id
         left join ods.ods_xiaoe_special c --成单期次
                   on a.owner_class = c.xe_id
         left join ods.ods_xiaoe_member d --获取member_id、mobile、sales_id
                   on a.user_id = d.xe_id
--          left join  ods.ods_xiaoe_vip_member f
--                     on d.id = f.xiaoe_member_id
--                         and a.owner_class = f.xiaoe_order_resource_id
         LEFT JOIN department_cte -- 销售部门信息
                   ON department_cte.id = a.owner_id
         LEFT JOIN (select id,platform from ods.ods_place_h5) h5
                   on a.h5_id = h5.id
;
INSERT OVERWRITE TABLE dwd.dwd_edu_service_refund_orders
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
     ,sales_name_2
from refund_order_data_all