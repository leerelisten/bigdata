-- place_order  每日定版数据 依赖 ods_place_order 每日更新当前分区数据

CREATE EXTERNAL TABLE IF NOT EXISTS `ods.ods_place_order_dt`
(
    `id`              int COMMENT 'ID自增',
    `member_id`       int COMMENT '用户ID',
    `out_trade_no`    string COMMENT '订单号',
    `transaction_id`  string COMMENT '微信交易号',
    `price`           int COMMENT '支付金额',
    `openid`          string COMMENT '支付openid',
    `trade_state`     string COMMENT '通知返回支付状态',
    `refunds`         int COMMENT '是否退款',
    `created_at`      string COMMENT '创建时间',
    `updated_at`      string COMMENT '更新时间',
    `mch_id`          string COMMENT '微信商户号 小糖1513678111，乐学1640062978',
    `special_id`      string COMMENT '专栏ID',
    `h5_id`           int COMMENT 'h5素材id',
    `sales_id`        int COMMENT '分配销售',
    `platform`        string COMMENT '平台',
    `report_link`     string COMMENT '转化回传',
    `ip`              string COMMENT 'ip',
    `wx_rel_status`   smallint COMMENT '1=>未添加微信,2=>已添加微信,3=>单向好友',
    `department`      int COMMENT 'sales_id部门',
    `user_group`      int COMMENT 'sales_id用户组',
    `p_source`        string COMMENT '渠道编号',
    `pay_time`        string COMMENT '支付成功时间',
    `refund_time`     string COMMENT '退款成功时间',
    `refund_price`    int COMMENT '退款金额',
    `member_status`   int COMMENT '例子订单是否有效=1有效 =0无效',
    `source_order_id` bigint COMMENT '来源订单ID',
    `wx_rel_type`     int,
    `ip_region`       string
)
    COMMENT '直播舞蹈大单课训练营支付数据'
    PARTITIONED BY (
        `dt` string)
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '',
        'serialization.format' = '')
    STORED AS INPUTFORMAT
        'org.apache.hadoop.mapred.TextInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

-- 执行定板插入语句
INSERT INTO ods.ods_place_order_dt PARTITION (dt = '${datebuf}')
SELECT id,
       member_id,
       out_trade_no,
       transaction_id,
       price,
       openid,
       trade_state,
       refunds,
       created_at,
       updated_at,
       mch_id,
       special_id,
       h5_id,
       sales_id,
       platform,
       report_link,
       ip,
       wx_rel_status,
       department,
       user_group,
       p_source,
       pay_time,
       refund_time,
       refund_price,
       member_status,
       source_order_id,
       wx_rel_type,
       ip_region
FROM ods.ods_place_order