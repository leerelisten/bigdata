-- xiaoe_order 每日定版数据 依赖 ods_xiaoe_order 每日更新当前分区数据

CREATE EXTERNAL TABLE IF NOT EXISTS `ods.ods_xiaoe_order_dt`(
  `id` int COMMENT 'ID自增', 
  `xe_id` string COMMENT 'xiaoe 订单id', 
  `user_id` string COMMENT 'xiaoe uid', 
  `resource_id` string COMMENT '商品id', 
  `resource_type` int COMMENT '商品类型', 
  `cou_price` int COMMENT '优惠券抵扣金额 单位分', 
  `channel_info` string COMMENT '渠道来源', 
  `price` float COMMENT '实付价格 单位元', 
  `order_state` int COMMENT '订单状态', 
  `pay_time` string COMMENT '支付时间', 
  `refund_money` int COMMENT '退款金额', 
  `created_at` string COMMENT '创建时间', 
  `sync_time` int COMMENT '同步时间', 
  `unit_price` int COMMENT '商品单价单位分', 
  `pay_way` int COMMENT '支付方式', 
  `xe_app_id` string COMMENT '小鹅店铺appid', 
  `out_order_id` string COMMENT '商户单号', 
  `transaction_id` string COMMENT '支付单号', 
  `refund_created_at` string COMMENT '退款创建时间', 
  `department` int COMMENT '部门', 
  `user_group` int COMMENT '用户组', 
  `order_type` int COMMENT '1系统订单2录入订单', 
  `parent_orderno` string COMMENT '父订单号-拆单才有', 
  `create_uid` int COMMENT '后台创建订单的员工id', 
  `parent_price` float COMMENT '商品总价', 
  `is_agreement` tinyint COMMENT '是否同意协议', 
  `agree_from` string COMMENT '协议同意来源:miniapp 小程序用户协议,h5 物流表单', 
  `sub_mchid` string COMMENT '服务商号', 
  `xiaoe_order_type` string COMMENT '小鹅通订单类型', 
  `sys_version` tinyint COMMENT '系统版本=1是老系统，=2是新系统', 
  `per` tinyint COMMENT '支付宝花呗分期期数', 
  `owner_id` int COMMENT '订单归属人id', 
  `owner_class` string COMMENT '订单归属班级/训练营', 
  `h5_id` int COMMENT 'h5_id', 
  `channel_pagestat_id` int COMMENT '渠道页面统计ID', 
  `sys_shop_id` string COMMENT '店铺ID', 
  `currency` string COMMENT '货币代码', 
  `umi_id` int COMMENT 'crm_user_mp_info顶级用户表，订单所属用户 id', 
  `currency_price` float COMMENT '对应货币代码价格 单位元', 
  `owner_type` tinyint COMMENT '归属类型:0=无,1=人工,2=助手人工,3=助手')
COMMENT '小鹅通订单表'
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='', 
  'serialization.format'='') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://Ucluster/wuhan_hive/warhouse/ods.db/ods_xiaoe_order_dt'
TBLPROPERTIES (
  'transient_lastDdlTime'='1747033315');

-- 执行定板插入语句
INSERT INTO ods.ods_xiaoe_order_dt PARTITION (dt = '${datebuf}')
SELECT id,
       xe_id,
       user_id,
       resource_id,
       resource_type,
       cou_price,
       channel_info,
       price,
       order_state,
       pay_time,
       refund_money,
       created_at,
       sync_time,
       unit_price,
       pay_way,
       xe_app_id,
       out_order_id,
       transaction_id,
       refund_created_at,
       department,
       user_group,
       order_type,
       parent_orderno,
       create_uid,
       parent_price,
       is_agreement,
       agree_from,
       sub_mchid,
       xiaoe_order_type,
       sys_version,
       per,
       owner_id,
       owner_class,
       h5_id,
       channel_pagestat_id,
       sys_shop_id,
       currency,
       umi_id,
       currency_price,
       owner_type
FROM ods.ods_xiaoe_order
;