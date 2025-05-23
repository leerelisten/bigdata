CREATE TABLE IF NOT EXISTS dws.dws_erzhuan_user_info
(
    id                        int COMMENT 'ID自增',
    xe_id                     string COMMENT 'xiaoe订单id',
    member_id                 bigint COMMENT 'xiaoemember_id',
    user_id                   string COMMENT 'xiaoeuid',
    special_type              int COMMENT '商品类型，同product_type中的id',
    special_id                string COMMENT '商品id',
    goods_name                string COMMENT '商品名称',
    sale_end_date             string COMMENT '当前期次销转结束日期',
    curr_class_sale_id        bigint COMMENT '当前课程班主任',
    sale_name_real            string COMMENT '销售姓名',
    department                string COMMENT '部门',
    department_leader         string COMMENT '部门总监',
    before_delay_special_id   string COMMENT '当期次延期前special_id',
    after_delay_special_id    string COMMENT '当期次延期后special_id',
    delay_time                string COMMENT '向后延期的延期发起时间',
    resource_type             int COMMENT '商品类型',
    cou_price                 int COMMENT '优惠券抵扣金额单位分',
    channel_info              string COMMENT '渠道来源',
    price                     float COMMENT '实付价格单位元',
    order_state               int COMMENT '订单状态',
    pay_time                  string COMMENT '支付时间',
    refund_money              int COMMENT '退款金额',
    created_at                string COMMENT '创建时间',
    sync_time                 int COMMENT '同步时间',
    unit_price                int COMMENT '商品单价单位分',
    pay_way                   int COMMENT '支付方式',
    xe_app_id                 string COMMENT '小鹅店铺appid',
    out_order_id              string COMMENT '商户单号',
    transaction_id            string COMMENT '支付单号',
    refund_commit_time        string COMMENT '退款企微流程提交时间',
    refund_created_at         string COMMENT '退款创建时间',
    order_type                int COMMENT '1系统订单2录入订单',
    parent_orderno            string COMMENT '父订单号-拆单才有',
    create_uid                int COMMENT '后台创建订单的员工id',
    parent_price              float COMMENT '商品总价',
    is_agreement              tinyint COMMENT '是否同意协议',
    agree_from                string COMMENT '协议同意来源:miniapp小程序用户协议h5物流表单',
    sub_mchid                 string COMMENT '服务商号',
    xiaoe_order_type          string COMMENT '小鹅通订单类型',
    sys_version               tinyint COMMENT '系统版本=1是老系统，=2是新系统',
    per                       tinyint COMMENT '支付宝花呗分期期数',
    owner_id                  int COMMENT '订单归属人id',
    owner_class               string COMMENT '订单归属班级/训练营',
    h5_id                     int COMMENT 'h5_id',
    channel_pagestat_id       int COMMENT '渠道页面统计ID',
    sys_shop_id               string COMMENT '店铺ID',
    is_before_change          int COMMENT '当前订单是否换号而来',
    wx_add_time               string COMMENT '加微时间',
    if_buy                    int COMMENT '是否购买高价课',
    high_lv_pay_time          string COMMENT '高价课购买时间',
    high_lv_special_type      int COMMENT '高价课课程类型',
    high_lv_order_coefficient decimal(8, 2) COMMENT '高价课课程系数',
    high_lv_order_amount      decimal(8, 2) COMMENT '高价课课程单价'
) COMMENT '二转填表底层用户数据'
    STORED AS ORC;


INSERT OVERWRITE TABLE dws.dws_erzhuan_user_info
SELECT a.id
     , a.xe_id
     , a.member_id
     , a.user_id
     , a.special_type
     , a.special_id
     , a.goods_name
     , hle.sale_end_date
     , a.curr_class_sale_id
     , ps.sale_name_real
     , hls.department
     , hls.department_leader
     , a.before_delay_special_id
     , a.after_delay_special_id
     , NVL(a.delay_time, '9999-01-01 00:00:00') AS delay_time
     , a.resource_type
     , a.cou_price
     , a.channel_info
     , a.price
     , a.order_state
     , a.pay_time
     , a.refund_money
     , a.created_at
     , a.sync_time
     , a.unit_price
     , a.pay_way
     , a.xe_app_id
     , a.out_order_id
     , a.transaction_id
     , hlw.refund_commit_time
     , a.refund_created_at
     , a.order_type
     , a.parent_orderno
     , a.create_uid
     , a.parent_price
     , a.is_agreement
     , a.agree_from
     , a.sub_mchid
     , a.xiaoe_order_type
     , a.sys_version
     , a.per
     , a.owner_id
     , a.owner_class
     , a.h5_id
     , a.channel_pagestat_id
     , a.sys_shop_id
     , a.is_before_change
     , a.wx_add_time
     , IF(b.special_id IS NOT NULL, 1, 0)       AS if_buy
     , b.pay_time                               AS high_lv_pay_time
     , b.special_type                           AS high_lv_special_type
     , pt.order_coefficient                     AS high_lv_order_coefficient
     , pt.order_amount                          AS high_lv_order_amount
FROM (SELECT *
      FROM dws.dws_xiaoe_order_dt
      WHERE dt = '${datebuf}'
        AND special_type IN (12, 18)
        -- 小鹅通加权限的时候会生成的一个0元详查，需要取price>0
        AND price > 0) a
-- 关联销转完成时间
         LEFT JOIN
     (SELECT* FROM dwd.dwd_high_lv_goods_period_end_time_dt WHERE dt = '${datebuf}') hle
     ON a.goods_name = hle.goods_name
         LEFT JOIN
     (SELECT * FROM dwd.dwd_place_sales_zip WHERE end_time = '9999-01-01 00:00:00') ps
     ON ps.sales_id = a.curr_class_sale_id
         -- 二转班主任关联部门、总监等信息
         LEFT JOIN
     (SELECT * FROM dwd.dwd_high_lv_goods_sale_department_info_dt WHERE dt = '${datebuf}') hls
     ON a.goods_name = hls.goods_name
         AND ps.sale_name_real = hls.class_sale_name
         -- 关联正价课购买
         LEFT JOIN
     (SELECT *
      FROM dws.dws_xiaoe_order_dt
      WHERE dt = '${datebuf}'
        -- 去掉price=0的导入订单。
        AND price > 0
        -- 分别是  3980炼气营对应1单，2580精选炼气营对应0.65单，2180养正营对应0.55单
        AND special_type IN (108, 109, 13)) b
     ON a.special_id = b.owner_class
         AND a.user_id = b.user_id
         LEFT JOIN
     ods.ods_crm_product_types pt
     ON b.special_type = pt.id
         -- 关联退款提交时间
         LEFT JOIN
     (SELECT goods_name, user_id, MIN(refund_commit_time) AS refund_commit_time
      FROM dwd.dwd_high_lv_wx_refund_info_dt
      WHERE dt = '${datebuf}'
        AND refund_status = '已通过'
      GROUP BY goods_name, user_id) hlw
     ON a.goods_name = hlw.goods_name
         AND a.user_id = hlw.user_id
-- 如果是多次延期用户，则去掉除首尾以及购买那次订单之外的所有订单
WHERE a.before_delay_special_id IS NULL
   OR a.after_delay_special_id IS NULL;