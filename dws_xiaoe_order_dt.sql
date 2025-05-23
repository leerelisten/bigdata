-- 增加 延期 、 换号情况的特殊处理
-- 延期由于xiaoe_order里面没有单子，需要补充一笔进去
-- 换号有单子，需要剔除这条记录，保留换号后的订单，同时新增一个换号前账号的字段。
-- 增加延期前后的数据字段，和换号前后的数据字段。

CREATE TABLE IF NOT EXISTS dws.dws_xiaoe_order_dt
(
    `id`                      int COMMENT 'ID自增',
    `xe_id`                   string COMMENT 'xiaoe订单id',
    `member_id`               bigint COMMENT 'xiaoemember_id',
    `user_id`                 string COMMENT 'xiaoeuid',
    `special_type`            int COMMENT '商品类型，同product_type中的id',
    `special_id`              string COMMENT '商品id',
    `goods_name`              string COMMENT '商品名称',
    `curr_class_sale_id`      bigint COMMENT '当前课程班主任',
    `before_delay_special_id` string COMMENT '当期次延期前special_id',
    `after_delay_special_id`  string COMMENT '当期次延期后special_id',
    `delay_time`              string COMMENT '向后延期的延期发起时间',
    `resource_type`           int COMMENT '商品类型',
    `cou_price`               int COMMENT '优惠券抵扣金额单位分',
    `channel_info`            string COMMENT '渠道来源',
    `price`                   float COMMENT '实付价格单位元',
    `pay_without_coupon`      float COMMENT '实付金额，减掉优惠券退回金额之后的实际支付金额',
    `order_state`             int COMMENT '订单状态',
    `pay_time`                string COMMENT '支付时间',
    `refund_money`            int COMMENT '退款金额',
    `refund_without_coupon`   float COMMENT '实际退款金额，去掉优惠券退款',
    `created_at`              string COMMENT '创建时间',
    `sync_time`               int COMMENT '同步时间',
    `unit_price`              int COMMENT '商品单价单位分',
    `pay_way`                 int COMMENT '支付方式',
    `xe_app_id`               string COMMENT '小鹅店铺appid',
    `out_order_id`            string COMMENT '商户单号',
    `transaction_id`          string COMMENT '支付单号',
    `refund_created_at`       string COMMENT '退款创建时间',
    `department`              int COMMENT '部门',
    `user_group`              int COMMENT '用户组',
    `order_type`              int COMMENT '1系统订单2录入订单',
    `parent_orderno`          string COMMENT '父订单号-拆单才有',
    `create_uid`              int COMMENT '后台创建订单的员工id',
    `parent_price`            float COMMENT '商品总价',
    `is_agreement`            tinyint COMMENT '是否同意协议',
    `agree_from`              string COMMENT '协议同意来源:miniapp小程序用户协议h5物流表单',
    `sub_mchid`               string COMMENT '服务商号',
    `xiaoe_order_type`        string COMMENT '小鹅通订单类型',
    `sys_version`             tinyint COMMENT '系统版本=1是老系统，=2是新系统',
    `per`                     tinyint COMMENT '支付宝花呗分期期数',
    `owner_id`                int COMMENT '订单归属人id',
    `owner_class`             string COMMENT '订单归属班级/训练营',
    `h5_id`                   int COMMENT 'h5_id',
    `channel_pagestat_id`     int COMMENT '渠道页面统计ID',
    `sys_shop_id`             string COMMENT '店铺ID',
    `is_before_change`        int COMMENT '当前订单是否换号而来',
    `wx_add_time`             string COMMENT '加微时间'
) COMMENT 'DWS层xiaoe_order表'
    PARTITIONED BY (dt string)
    STORED AS ORC;



-- 延期学员计算逻辑：由于学院延期后不会生成一笔新订单，订单记录仍是延期前的记录
-- 先开窗取第一次延期的信息，关联出来xiaoe_oreder,生成一条订单记录，再开窗将最后一次延期记录的信息补充到订单里，和xiaoe_order表 union，获取到只含首尾两次延期的记录，这样就实现了将中间的延期记录删除
DROP TABLE IF EXISTS delay_user;
CREATE TEMPORARY TABLE delay_user AS
SELECT cc.id
     , cc.xe_id
     , bb.member_id
     , xm.user_id
     , bb.new_class          AS special_id
     , vm.class_sales_id     AS curr_class_sale_id
     , bb.curr_class         AS last_class
     , bb.curr_class_sale_id AS last_class_sale_id
--      , bb.new_class          AS new_class
     , CAST(NULL AS string)     delay_time
     , cc.resource_type
     , cc.cou_price
     , cc.channel_info
     , cc.price
     , cc.order_state
     , cc.pay_time
     , cc.refund_money
     , cc.created_at
     , cc.sync_time
     , cc.unit_price
     , cc.pay_way
     , cc.xe_app_id
     , cc.out_order_id
     , cc.transaction_id
     , cc.refund_created_at
     , cc.department
     , cc.user_group
     , cc.order_type
     , cc.parent_orderno
     , cc.create_uid
     , cc.parent_price
     , cc.is_agreement
     , cc.agree_from
     , cc.sub_mchid
     , cc.xiaoe_order_type
     , cc.sys_version
     , cc.per
     , cc.owner_id
     , cc.owner_class
     , cc.h5_id
     , cc.channel_pagestat_id
     , cc.sys_shop_id
     , IF(vm.class_wx_relation_status IN (2, 3, 4), vm.class_wx_created_at,
          NULL)              AS wx_add_time -- 为迎合二转填表，此处加微时间取vip_member表中的加微时间
FROM (SELECT *
      FROM (SELECT id
                 , member_id
                 , apply_type
                 , begin_class_date
                 , create_uid
                 , reason
                 , remark
                 , jiaowu
                 , order_id
                 , last_class
                 , last_class_sale_id
                 , curr_class
                 , curr_phase
                 , curr_class_sale_id
                 , apply_begin_class_at
                 , new_class
                 , new_phase
                 , new_class_sale_id
                 , created_at
                 , updated_at
                 , begin_class_state
                 , vip_member_id
                 , qw_sp_no
                 , qw_sp_status
                 , remark_img_url
                 , RANK() OVER (PARTITION BY member_id,vip_member_id ORDER BY created_at ASC) AS rn
            FROM dwd.dwd_crm_vip_apply_delay) a
      WHERE rn = 1) aa
         LEFT JOIN
     (SELECT *
      FROM (SELECT id
                 , member_id
                 , apply_type
                 , begin_class_date
                 , create_uid
                 , reason
                 , remark
                 , jiaowu
                 , order_id
                 , last_class
                 , last_class_sale_id
                 , curr_class
                 , curr_phase
                 , curr_class_sale_id
                 , apply_begin_class_at
                 , new_class
                 , new_phase
                 , new_class_sale_id
                 , created_at
                 , updated_at
                 , begin_class_state
                 , vip_member_id
                 , qw_sp_no
                 , qw_sp_status
                 , remark_img_url
                 , RANK() OVER (PARTITION BY member_id,vip_member_id ORDER BY created_at DESC) AS rn
            FROM dwd.dwd_crm_vip_apply_delay) a
      WHERE rn = 1) bb
     ON aa.vip_member_id = bb.vip_member_id
         LEFT JOIN dwd.dwd_xiaoe_member xm
                   ON bb.member_id = xm.id
         LEFT JOIN
     (SELECT *
      FROM dwd.dwd_xiaoe_order_dt
      WHERE dt = '${datebuf}') cc
     ON xm.user_id = cc.user_id
         AND aa.curr_class = cc.special_id
         LEFT JOIN
     dwd.dwd_xiaoe_vip_member vm
     ON bb.vip_member_id = vm.id
;


-- 换号，换号前后记录保留，换号前记录剔除，换号前后购买标记出来，或者单独一个字段记录是否应该删除，购买正价课正常关联。
DROP TABLE xiaoe_order;
CREATE TEMPORARY TABLE xiaoe_order AS
SELECT od.id
     , od.xe_id
     , od.member_id
     , od.user_id
     , od.special_id
     , NVL(dl2.curr_class_sale_id, vm.class_sales_id)                             AS curr_class_sale_id --优先取延期的
     , CAST(NULL AS string)                                                       AS before_delay_special_id
     , dl2.new_class                                                              AS after_delay_special_id
     , CAST(dl2.created_at AS string)                                             AS delay_time
     , od.resource_type
     , od.cou_price
     , od.channel_info
     , od.price
     , od.order_state
     , od.pay_time
     , od.refund_money
     , od.created_at
     , od.sync_time
     , od.unit_price
     , od.pay_way
     , od.xe_app_id
     , od.out_order_id
     , od.transaction_id
     , od.refund_created_at
     , od.department
     , od.user_group
     , od.order_type
     , od.parent_orderno
     , od.create_uid
     , od.parent_price
     , od.is_agreement
     , od.agree_from
     , od.sub_mchid
     , od.xiaoe_order_type
     , od.sys_version
     , od.per
     , od.owner_id
     , od.owner_class
     , od.h5_id
     , od.channel_pagestat_id
     , od.sys_shop_id
     , IF(ca.special_id IS NOT NULL, 1, 0)                                        AS is_before_change
     , IF(vm.class_wx_relation_status IN (2, 3, 4), vm.class_wx_created_at, NULL) AS wx_add_time
FROM (SELECT *
      FROM dwd.dwd_xiaoe_order_dt
      WHERE dt = '${datebuf}') od
         LEFT JOIN
     -- 去除延期订单
         dwd.dwd_crm_vip_apply_delay dl
     ON od.member_id = dl.member_id
         AND od.special_id = dl.new_class
         LEFT JOIN
     -- 关联延期前订单，如果是第一次延期的订单，则班主任信息取delay表延期前的班主任，取不到再取vip_member表班主任。
         dwd.dwd_crm_vip_apply_delay dl2
     ON od.member_id = dl2.member_id
         AND od.special_id = dl2.curr_class
         -- 给换号订单标记是否删除
         LEFT JOIN
     (SELECT * FROM dwd.dwd_crm_vip_change_account_record WHERE dt = '${datebuf}') ca
     ON od.member_id = ca.member_id
         AND od.special_id = ca.special_id
         LEFT JOIN
     dwd.dwd_xiaoe_vip_member vm
     ON od.member_id = vm.member_id
         AND od.special_id = vm.special_id
WHERE dl.member_id IS NULL
UNION ALL
-- 关联最后一次延期记录，after_delay_sepecial_id is null
SELECT id
     , xe_id
     , member_id
     , user_id
     , special_id
     , curr_class_sale_id
     , last_class           AS before_delay_special_id
     , CAST(NULL AS string) AS after_delay_special_id
     , CAST(NULL AS string)    delay_time
     , resource_type
     , cou_price
     , channel_info
     , price
     , order_state
     , pay_time
     , refund_money
     , created_at
     , sync_time
     , unit_price
     , pay_way
     , xe_app_id
     , out_order_id
     , transaction_id
     , refund_created_at
     , department
     , user_group
     , order_type
     , parent_orderno
     , create_uid
     , parent_price
     , is_agreement
     , agree_from
     , sub_mchid
     , xiaoe_order_type
     , sys_version
     , per
     , owner_id
     , owner_class
     , h5_id
     , channel_pagestat_id
     , sys_shop_id
     , CAST(NULL AS int)    AS is_before_change
     , wx_add_time
FROM delay_user a;



INSERT OVERWRITE TABLE dws.dws_xiaoe_order_dt PARTITION (dt = '${datebuf}')
SELECT t1.id
     , t1.xe_id
     , t1.member_id
     , t1.user_id
     , xc.type                                  AS special_type
     , t1.special_id
     , xs.goods_name
     , nvl(t1.curr_class_sale_id,bb.sales_id) as curr_class_sale_id
     , t1.before_delay_special_id
     , t1.after_delay_special_id
     , t1.delay_time
     , t1.resource_type
     , t1.cou_price
     , t1.channel_info
     , t1.price
     , IF(pt.id > 0, pt.order_amount, t1.price) AS pay_without_coupon
     , t1.order_state
     , t1.pay_time
     , t1.refund_money
     , IF(pt.id > 0, 0, t1.refund_money)        AS refund_without_coupon
     , t1.created_at
     , t1.sync_time
     , t1.unit_price
     , t1.pay_way
     , t1.xe_app_id
     , t1.out_order_id
     , t1.transaction_id
     , t1.refund_created_at
     , t1.department
     , t1.user_group
     , t1.order_type
     , t1.parent_orderno
     , t1.create_uid
     , t1.parent_price
     , t1.is_agreement
     , t1.agree_from
     , t1.sub_mchid
     , t1.xiaoe_order_type
     , t1.sys_version
     , t1.per
     , t1.owner_id
     , t1.owner_class
     , t1.h5_id
     , t1.channel_pagestat_id
     , t1.sys_shop_id
     , t1.is_before_change
     , t1.wx_add_time
FROM xiaoe_order t1
         -- 某些特殊场景，是匹配不到销售的，这里从操作记录表中匹配最后一次分配的销售，补充进去
         LEFT JOIN
     dwd.dwd_xiaoe_member_transfer_log bb
     ON t1.member_id = bb.member_id
         AND t1.special_id = bb.special_id
         LEFT JOIN
     dwd.dwd_xiaoe_special xs
     ON t1.special_id = xs.special_id
         LEFT JOIN
     ods.ods_xiaoe_class xc
     ON t1.special_id = xc.resource_id
         LEFT JOIN
     dwd.dwd_xiaoe_member a
     ON t1.member_id = a.id
         LEFT JOIN
     dwd.dwd_place_sales b
     ON t1.curr_class_sale_id = b.id
         LEFT JOIN
     ods.ods_crm_product_types pt
     ON xc.type = pt.id
         AND (t1.price - t1.refund_money / 100) = pt.order_amount
         AND t1.refund_money > 0;