-- 训练营学员购买正价课以及上课情况
-- 提出公共表：user_info
--20241114
--1.添加部门 小组 销售名称
--2.添加D4~D14上课情况

CREATE TABLE IF NOT EXISTS dws.dws_sale_camping_user_day
(
    member_id               int COMMENT '用户ID',
    cat                     STRING COMMENT '品类',
    ad_department           STRING COMMENT '投放部门',
    platform                STRING COMMENT '渠道',
    platform_name           STRING COMMENT '渠道(中文)',
    department              STRING COMMENT '部门',
    user_group              STRING COMMENT '组',
    h5_id                   int COMMENT 'h5id',
    pos                     STRING COMMENT '版位',
    price                   STRING COMMENT '价格',
    mobile                  STRING COMMENT '手机手机号',
    link_type_v1            STRING COMMENT '链路类型-旧',
    link_type_v2            STRING COMMENT '链路类型-新',
    cost_id                 STRING COMMENT '投放账户id',
    cost_id2                STRING COMMENT '修正账号',
    ad_id                   STRING COMMENT '投放计划id',
    sucai_id                STRING COMMENT '投放素材id',
    unionid                 STRING COMMENT '微信unionid',
    phone                   STRING COMMENT '手机号',
    created_at              STRING COMMENT '创建时间',
    wx_add_time             timestamp COMMENT '加微时间',
    wx_rel_status           STRING COMMENT '加微状态',
    special_id              STRING COMMENT '专栏id',
    goods_name              STRING COMMENT '专栏名称',
    xe_id                   STRING COMMENT '小鹅通ID',
    ifcollect               STRING COMMENT '是否填写问卷',
    trade_state             STRING COMMENT '交易状态',
    is_get_ticket           STRING COMMENT '已领券',
    member_status           int COMMENT '用户状态',
    sales_id                int COMMENT '销售id',
    sales_name              STRING COMMENT '销售',
    sales_sop_type          string COMMENT 'AI销售SOP类型',
    ifbuy                   int COMMENT '是否购买',
    first_pay_time          STRING COMMENT '首次购买时间',
    pay_num                 decimal(10, 2) COMMENT '正价课订单数',
    pay_sum                 decimal(10, 2) COMMENT '正价课GMV',

    first_order_state       string COMMENT '首次购买状态',
    first_refund_money      decimal(10, 2) COMMENT '首次退款金额',
    first_refund_created_at timestamp COMMENT '首次退款时间',

    is_old_user             int COMMENT '是否老用户',
    is_old_user_30          int COMMENT '是否老用户(30天内)',

    pay_user_num_D4         int COMMENT 'D4正价课购买人数',
    pay_num_D4              decimal(10, 2) COMMENT 'D4正价课订单数',
    pay_sum_D4              decimal(10, 2) COMMENT 'D4正价课GMV',

    pay_user_num_D4_24h     int COMMENT 'D4正价课购买人数(24H)',
    pay_num_D4_24h          decimal(10, 2) COMMENT 'D4正价课订单数(24H)',
    pay_sum_D4_24h          decimal(10, 2) COMMENT 'D4正价课GMV(24H)',

    pay_user_num_D5         int COMMENT 'D5正价课购买人数',
    pay_num_D5              decimal(10, 2) COMMENT 'D5正价课订单数',
    pay_sum_D5              decimal(10, 2) COMMENT 'D5正价课GMV',

    pay_user_num_D6         int COMMENT 'D6正价课购买人数',
    pay_num_D6              decimal(10, 2) COMMENT 'D6正价课订单数',
    pay_sum_D6              decimal(10, 2) COMMENT 'D6正价课GMV',

    pay_user_num_D7         int COMMENT 'D7正价课购买人数',
    pay_num_D7              decimal(10, 2) COMMENT 'D7正价课订单数',
    pay_sum_D7              decimal(10, 2) COMMENT 'D7正价课GMV',

    pay_user_num_D8         int COMMENT 'D8正价课购买人数',
    pay_num_D8              decimal(10, 2) COMMENT 'D8正价课订单数',
    pay_sum_D8              decimal(10, 2) COMMENT 'D8正价课GMV',

    pay_user_num_D9         int COMMENT 'D9正价课购买人数',
    pay_num_D9              decimal(10, 2) COMMENT 'D9正价课订单数',
    pay_sum_D9              decimal(10, 2) COMMENT 'D9正价课GMV',

    pay_user_num_D10        int COMMENT 'D10正价课购买人数',
    pay_num_D10             decimal(10, 2) COMMENT 'D10正价课订单数',
    pay_sum_D10             decimal(10, 2) COMMENT 'D10正价课GMV',

    pay_user_num_D11        int COMMENT 'D11正价课购买人数',
    pay_num_D11             decimal(10, 2) COMMENT 'D11正价课订单数',
    pay_sum_D11             decimal(10, 2) COMMENT 'D11正价课GMV',

    pay_user_num_D12        int COMMENT 'D12正价课购买人数',
    pay_num_D12             decimal(10, 2) COMMENT 'D12正价课订单数',
    pay_sum_D12             decimal(10, 2) COMMENT 'D12正价课GMV',

    pay_user_num_D13        int COMMENT 'D13正价课购买人数',
    pay_num_D13             decimal(10, 2) COMMENT 'D13正价课订单数',
    pay_sum_D13             decimal(10, 2) COMMENT 'D13正价课GMV',

    pay_user_num_D14        int COMMENT 'D14正价课购买人数',
    pay_num_D14             decimal(10, 2) COMMENT 'D14正价课订单数',
    pay_sum_D14             decimal(10, 2) COMMENT 'D14正价课GMV',

    ifcome0                 int COMMENT 'D0是否到课',
    ifok0                   int COMMENT 'D0是否完课',
    ifcome1                 int COMMENT 'D1是否到课',
    ifok1                   int COMMENT 'D1是否完课',
    ifcome2                 int COMMENT 'D2是否到课',
    ifok2                   int COMMENT 'D2是否完课',
    ifcome3                 int COMMENT 'D3是否到课',
    ifok3                   int COMMENT 'D3是否完课',
    ifcome4                 int COMMENT 'D4是否到课',
    ifok4                   int COMMENT 'D4是否完课',
    ifcome5                 int COMMENT 'D5是否到课',
    ifok5                   int COMMENT 'D5是否完课',
    wx_active               int COMMENT '是否主动加微',
    collect_active          int COMMENT '是否主动问卷',
    cac                     float COMMENT '例子消耗',
    cac_real                float COMMENT '例子实际消耗',
    d_date                  STRING COMMENT '消耗日期',
    is_abroad               STRING COMMENT '海外/国内',
    country_code            string COMMENT '国家编码',
    xff                     string COMMENT '用户公网IP',
    pay_user_num_D1         int COMMENT 'D1正价课购买人数',          -- 2025-04-29 新增
    pay_num_D1              decimal(10, 2) COMMENT 'D1正价课订单数', -- 2025-04-29 新增
    pay_sum_D1              decimal(10, 2) COMMENT 'D1正价课GMV',    -- 2025-04-29 新增
    pay_user_num_D2         int COMMENT 'D2正价课购买人数',          -- 2025-04-29 新增
    pay_num_D2              decimal(10, 2) COMMENT 'D2正价课订单数', -- 2025-04-29 新增
    pay_sum_D2              decimal(10, 2) COMMENT 'D2正价课GMV',    -- 2025-04-29 新增
    pay_user_num_D3         int COMMENT 'D3正价课购买人数',          -- 2025-04-29 新增
    pay_num_D3              decimal(10, 2) COMMENT 'D3正价课订单数', -- 2025-04-29 新增
    pay_sum_D3              decimal(10, 2) COMMENT 'D3正价课GMV'     -- 2025-04-29 新增
) COMMENT '训练营学员上课、购买正价课情况'
    PARTITIONED BY (dt STRING)
    STORED AS orcfile;



-- 25.05.14日新增，增加重复例子字段
-- 25.04.14，新增重复用户选项，口径：太极和八段锦一起剃重。
CREATE TEMPORARY TABLE olduser AS
    (SELECT member_id
          , goods_name
     FROM (SELECT member_id
                , goods_name
                , created_at
                , cat
                , LAG(created_at)
                      OVER (PARTITION BY member_id,IF(cat = '道门八段锦', '太极', cat) ORDER BY created_at ASC) AS lasttime
           FROM dw.dwd_xt_user
           WHERE dt = '${datebuf}'
             AND TO_DATE(created_at) <= '${datebuf}'
             AND member_status = 1
             AND trade_state IN ('SUCCESS', 'PREPARE')
             AND sales_id > 0) t
     WHERE lasttime IS NOT NULL
     GROUP BY member_id, goods_name);


CREATE TEMPORARY TABLE olduser30 AS
    (SELECT member_id
          , goods_name
     FROM (SELECT member_id
                , goods_name
                , created_at
                , cat
                , UNIX_TIMESTAMP(created_at) -
                  UNIX_TIMESTAMP(LAG(created_at)
                                     OVER (PARTITION BY member_id,IF(cat = '道门八段锦', '太极', cat) ORDER BY created_at ASC)) AS timediff
           FROM dw.dwd_xt_user
           WHERE dt = '${datebuf}'
             AND TO_DATE(created_at) <= '${datebuf}'
             AND member_status = 1
             AND trade_state IN ('SUCCESS', 'PREPARE')
             AND sales_id > 0) t
     WHERE timediff <= 30 * 24 * 3600
     GROUP BY member_id, goods_name);



INSERT OVERWRITE TABLE dws.dws_sale_camping_user_day PARTITION (dt = '${datebuf}')
SELECT a.member_id
     , a.cat
     , a.ad_department
     , a.platform
     , a.platform_name
     , a.department
     , a.user_group
     , a.h5_id
     , CASE
           WHEN a.cat = '瑜伽' AND a.pos = '腾讯视频号直播付费流' THEN '腾讯视频号直播付费流（瑜伽）'
           ELSE a.pos END                                                            AS pos       --20250116 添加区分太极和瑜伽
     , a.price
     , a.mobile
     , a.link_type_v1
     , CASE WHEN a.link_type_v2 = '新一键' THEN '新一键授权' ELSE a.link_type_v2 END AS link_type_v2
     , a.cost_id
     -- 24.11.26 lyf合并cost_id2逻辑
     , CASE
           WHEN a.pos = '本地推直播' THEN '直播一组-本地推直播'
    -- 25.4.29 新增 6个录入消耗的版位
           WHEN a.pos = '360网盟' AND ad_department = '信息流一部' THEN '信息流一部-360网盟'
           WHEN a.pos = '新浪粉丝通' AND ad_department = '信息流一部' THEN '信息流一部-新浪粉丝通'
           WHEN a.pos = 'TikTok（东南亚）' THEN '海外投放组-TikTok（东南亚）'
           WHEN a.pos = 'TikTok（北美）' THEN '海外投放组-TikTok（北美）'
           WHEN a.cat = '道门八段锦' AND a.pos = '腾讯视频号直播付费流' THEN '直播二组-视频号直播八段锦'
           WHEN a.pos = '腾讯东南亚' THEN '海外投放组-腾讯东南亚'
           WHEN a.cat = '道门八段锦' AND a.pos = '千川直播' THEN '直播二组-千川直播八段锦' --2025-05-06 新增直播二组-千川直播八段锦

    -- 25.4.8日新增，直播二组复投千川直播
           WHEN a.cat = '太极' AND a.platform_name = '抖音' AND a.pos = '千川直播' AND
                TO_DATE(a.created_at) <= '2025-03-31' THEN '直播三组-千川直播'
           WHEN a.cat = '太极' AND a.platform_name = '抖音' AND a.pos = '千川直播' AND
                TO_DATE(a.created_at) > '2025-03-31' THEN '直播二组-千川直播'
    -- 25.3.28新增9.9 元测试。
           WHEN a.cat = '太极' AND a.pos = '腾讯视频号直播付费流' AND price = '9.9元' THEN '直播二组-视频号直播测试'
           WHEN a.cat = '太极' AND a.pos = '腾讯视频号直播付费流' THEN '直播二组-视频号直播'
           WHEN a.pos = '千川直播' THEN '直播三组-千川直播'
           WHEN a.pos = '抖加' THEN '直播一组-抖加'
           WHEN a.pos = '小店随心推' THEN '直播一组-小店随心推'
           WHEN a.pos = '大屏' THEN '非标-大屏'
           WHEN a.pos = '千川直播（虚拟）' AND a.cat = '道门八段锦' THEN '直播一组-千川直播（虚拟）八段锦'
           WHEN a.pos = '千川直播（虚拟）' THEN '直播一组-千川直播（虚拟）'
           WHEN a.pos = '抖音信息流下载' AND TO_DATE(a.created_at) < '2025-03-30' THEN '非标-太极APP（抖音）'
           WHEN a.pos = '腾讯信息流下载' AND TO_DATE(a.created_at) < '2025-03-30' THEN '非标-太极APP（腾讯）'
    -- 25.4.14日新增，太极APP-百度搜索或信息流下载
    --WHEN a.pos = '百度信息流下载' AND TO_DATE(a.created_at) >= '2025-04-14' THEN '非标-太极APP（百度）'
           WHEN a.pos = '抖音信息流下载' AND TO_DATE(a.created_at) >= '2025-03-30' AND a.ad_department = '信息流四部'
               THEN '信息流四部-太极APP-抖音信息流下载'
           WHEN a.pos = '抖音信息流下载' AND TO_DATE(a.created_at) >= '2025-03-30'
               THEN '太极APP-抖音信息流下载'
           WHEN a.pos = '抖音信息流下载' AND TO_DATE(a.created_at) >= '2025-03-30' THEN '太极APP-抖音信息流下载'
           WHEN a.pos = '腾讯信息流下载' AND TO_DATE(a.created_at) >= '2025-03-30' THEN '太极APP-腾讯信息流下载'
           WHEN a.pos = '百度信息流下载' THEN '太极APP-百度信息流下载'
           WHEN a.pos = '百度搜索下载' THEN '太极APP-百度搜索下载'
           WHEN a.pos = '快手信息流下载' THEN '非标-太极APP（快手）' --20250407增加快手信息流下载
    --20250418增加太极app-应用商店下载  20250422 由 应用商店下载调整为 oppo 商店下载
           WHEN a.platform_name = '太极APP' AND a.pos = 'oppo商店下载' AND TO_DATE(a.created_at) >= '2025-04-18'
               THEN '太极APP-oppo商店下载'
    --20250422 新增  oppo信息流下载 vivo商店下载 vivo信息流下载 华为商店下载 华为ads下载
           WHEN a.platform_name = '太极APP' AND a.pos = 'oppo信息流下载' AND TO_DATE(a.created_at) >= '2025-04-22'
               THEN '太极APP-oppo信息流下载'
           WHEN a.platform_name = '太极APP' AND a.pos = 'vivo商店下载' AND TO_DATE(a.created_at) >= '2025-04-22'
               THEN '太极APP-vivo商店下载'
           WHEN a.platform_name = '太极APP' AND a.pos = 'vivo信息流下载' AND TO_DATE(a.created_at) >= '2025-04-22'
               THEN '太极APP-vivo信息流下载'
           WHEN a.platform_name = '太极APP' AND a.pos = '华为商店下载' AND TO_DATE(a.created_at) >= '2025-04-22'
               THEN '太极APP-华为商店下载'
           WHEN a.platform_name = '太极APP' AND a.pos = '华为ads下载' AND TO_DATE(a.created_at) >= '2025-04-22'
               THEN '太极APP-华为ads下载'
           WHEN a.platform_name = '太极APP' AND a.pos = '谷歌（东南亚）下载' AND ad_department = '信息流三部'
               THEN '信息流三部-谷歌（东南亚）下载'
           WHEN a.platform_name = '太极APP' AND a.pos = '脸书（东南亚）下载' AND ad_department = '信息流三部'
               THEN '信息流三部-脸书（东南亚）下载'
           WHEN a.cat = '瑜伽' AND a.pos = '腾讯视频号直播付费流' THEN '直播二组-视频号直播（瑜伽）'
           WHEN a.pos = '脸书（东南亚）' AND TO_DATE(a.created_at) < '2025-03-16' THEN '海外投放组-脸书（东南亚）'
           WHEN a.pos = '脸书（北美）' AND TO_DATE(a.created_at) < '2025-03-16' THEN '海外投放组-脸书（北美）'
           WHEN a.pos = '谷歌（东南亚）' AND TO_DATE(a.created_at) < '2025-04-19' THEN '海外投放组-谷歌（东南亚）'
           WHEN a.pos = '谷歌（北美）' AND TO_DATE(a.created_at) < '2025-04-19' THEN '海外投放组-谷歌（北美）'
           WHEN a.pos = '谷歌（东南亚）' AND TO_DATE(a.created_at) >= '2025-04-19'
               THEN CONCAT(a.cost_id, '&', a.country_code)
           WHEN a.pos = '谷歌（北美）' AND TO_DATE(a.created_at) >= '2025-04-19'
               THEN CONCAT(a.cost_id, '&', a.country_code)
    -- 由于 250309、250311、250313期次h5配置错误，导致例子解析有问题，手工处理
           WHEN a.pos = '百度信息流北京' AND TO_DATE(a.created_at) BETWEEN '2025-03-06' AND '2025-03-12' THEN '百度信息流北京'
           WHEN a.pos = '腾讯pcad(北京)' AND TO_DATE(a.created_at) BETWEEN '2025-03-14' AND '2025-03-17'
               THEN '腾讯pcad(北京)'
           WHEN a.pos = '腾讯优量汇(北京)' AND TO_DATE(a.created_at) BETWEEN '2025-03-13' AND '2025-03-17'
               THEN '腾讯优量汇(北京)'
           WHEN a.platform_name = '腾讯公众号关注' AND a.pos = '腾讯朋友圈' AND
                TO_DATE(a.created_at) BETWEEN '2025-04-01' AND '2025-04-03' THEN '腾讯公众号关注-腾讯朋友圈'
           ELSE NVL(a.cost_id, '99999') END                                          AS cost_id2
     , a.ad_id
     , a.sucai_id
     , a.unionid
     , a.phone
     , a.created_at
     , a.wx_add_time
     , a.wx_rel_status
     , a.special_id
     , a.goods_name
     , a.xe_id
     , a.ifcollect
     , a.trade_state
     , a.is_get_ticket
     , a.member_status
     , a.sales_id
     , a.sales_name
     , a.sales_sop_type
     , IF(b.user_id IS NOT NULL, 1, 0)                                               AS ifbuy
     , b.first_pay_time
     , NVL(b.pay_num, 0)                                                             AS pay_num
     , NVL(b.pay_sum, 0)                                                             AS pay_sum
     , b.first_order_state
     , b.first_refund_money
     , b.first_refund_created_at
     , IF(f.member_id IS NOT NULL, 1, 0)                                             AS is_old_user
     , IF(e.member_id IS NOT NULL, 1, 0)                                             AS is_old_user_30
     , NVL(CASE
               -- 25.4.21日修改：从250420期开始，使用D4新口径，如果有下单10分未支付订单，则消息提醒销售，若购买，则D4前单子算作人工
               WHEN (d.special_id IS NULL) OR (SUBSTR(a.goods_name, 2, 6) < '250420')
                   THEN CASE
                            WHEN a.goods_name RLIKE '线上训练营' AND SUBSTR(a.goods_name, 2, 6) >= '240930' AND
                                 b.first_pay_time IS NOT NULL
                                THEN IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
                                          TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) -
                                         3 * 24 * 3600) < 79020
                                , 1, 0) -- 9点57后30分钟，24.10.16张远要求，改成921点57分
                            WHEN a.goods_name RLIKE '八段锦|5天训练营' AND b.first_pay_time IS NOT NULL
                                THEN IF(b.first_pay_time < xs.d4_end_time, 1, 0)
                            WHEN a.goods_name NOT RLIKE '八段锦|5天训练营' AND b.first_pay_time IS NOT NULL
                                THEN IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
                                          TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 -
                                         3 * 24) < IF(SUBSTR(a.goods_name, 2, 6) >= '240903', 21, 20 + 5 / 6)
                                , 1, 0)
                            WHEN a.goods_name RLIKE '晨课' AND b.first_pay_time IS NOT NULL
                                THEN IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
                                          TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 -
                                         3 * 24) < 8.5
                                , 1, 0) -- 0910起有晨课班,时间节点为早上8.30
                            ELSE 0 END
               ELSE 0 END, 0)                                                        AS pay_user_num_D4

     , NVL(CASE
               WHEN (d.special_id IS NULL) OR (SUBSTR(a.goods_name, 2, 6) < '250420')
                   THEN
                   CASE
                       --25.04.24更新，海外使用 d4_end_time 计算
                       WHEN xs.is_abroad = 1 AND xs.cat = '太极' AND b.first_pay_time IS NOT NULL
                           THEN IF(b.first_pay_time < xs.d4_end_time, b.first_pay_num, 0)
                       -- 24.09.29更新，从0930期开始接下来要跑的“线上训练营”的版本d4是7点55上课，9点57下课哈
                       WHEN a.goods_name RLIKE '线上训练营' AND SUBSTR(a.goods_name, 2, 6) >= '240930' AND
                            b.first_pay_time IS NOT NULL
                           THEN IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
                                     TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) -
                                    3 * 24 * 3600) < 79020
                           , b.first_pay_num, 0) -- 9点57后30分钟，24.10.16张远要求，改成921点57分
                       WHEN a.goods_name RLIKE '八段锦|5天训练营' AND b.first_pay_time IS NOT NULL
                           THEN IF(b.first_pay_time < xs.d4_end_time, b.first_pay_num, 0)
                       WHEN a.goods_name NOT RLIKE '八段锦|5天训练营' AND b.first_pay_time IS NOT NULL
                           THEN IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
                                     TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 -
                                    3 * 24) < IF(SUBSTR(a.goods_name, 2, 6) >= '240903', 21, 20 + 5 / 6)
                           , b.first_pay_num, 0)
                       WHEN a.goods_name RLIKE '晨课' AND b.first_pay_time IS NOT NULL
                           THEN IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
                                     TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 -
                                    3 * 24) < 8.5
                           , b.first_pay_num, 0) -- 0910起有晨课班,时间节点为早上8.30
                       ELSE 0 END
               ELSE 0 END
    , 0)                                                                             AS pay_num_D4
     , NVL(CASE
               WHEN (d.special_id IS NULL) OR (SUBSTR(a.goods_name, 2, 6) < '250420')
                   THEN
                   CASE
                       --25.04.24更新，海外使用 d4_end_time 计算
                       WHEN xs.is_abroad = 1 AND xs.cat = '太极' AND b.first_pay_time IS NOT NULL
                           THEN IF(b.first_pay_time < xs.d4_end_time, b.first_pay_sum, 0)
                       WHEN a.goods_name RLIKE '线上训练营' AND SUBSTR(a.goods_name, 2, 6) >= '240930' AND
                            b.first_pay_time IS NOT NULL
                           THEN IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
                                     TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) -
                                    3 * 24 * 3600) < 79020
                           , b.first_pay_sum, 0) -- 21点57后30分钟，24.10.16张远要求，改成921点57分
                       WHEN a.goods_name RLIKE '八段锦|5天训练营' AND b.first_pay_time IS NOT NULL
                           THEN IF(b.first_pay_time < xs.d4_end_time, b.first_pay_sum, 0)
                       WHEN a.goods_name NOT RLIKE '八段锦|5天训练营' AND b.first_pay_time IS NOT NULL
                           THEN IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
                                     TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 -
                                    3 * 24) < IF(SUBSTR(a.goods_name, 2, 6) >= '240903', 21, 20 + 5 / 6)
                           , b.first_pay_sum, 0)
                       WHEN a.goods_name RLIKE '晨课' AND b.first_pay_time IS NOT NULL
                           THEN IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
                                     TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 -
                                    3 * 24) < 8.5
                           , b.first_pay_sum, 0) -- 0910起有晨课班,时间节点为早上8.30
                       ELSE 0 END
               ELSE 0 END
    , 0)                                                                             AS pay_sum_D4

     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 3 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D4_24h
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 3 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D4_24h
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 3 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D4_24h

     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 4 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D5
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 4 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D5
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 4 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D5

     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 5 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D6
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 5 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D6
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 5 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D6

     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 6 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D7
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 6 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D7
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 6 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D7

     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 7 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D8
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 7 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D8
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 7 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D8

     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 8 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D9
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 8 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D9
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 8 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D9

     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 9 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D10
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 9 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D10
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 9 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D10

     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 10 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D11
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 10 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D11
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 10 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D11

     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 11 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D12
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 11 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D12
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 11 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D12

     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 12 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D13
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 12 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D13
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 12 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D13

     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 13 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D14
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 13 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D14
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 13 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D14
     , IF(c.ifcome0 = 1, 1, 0)                                                       AS ifcome0
     , IF(c.ifok0 = 1, 1, 0)                                                         AS ifok0
     , IF(c.ifcome1 = 1, 1, 0)                                                       AS ifcome1
     , IF(c.ifok1 = 1, 1, 0)                                                         AS ifok1
     , IF(c.ifcome2 = 1, 1, 0)                                                       AS ifcome2
     , IF(c.ifok2 = 1, 1, 0)                                                         AS ifok2
     , IF(c.ifcome3 = 1, 1, 0)                                                       AS ifcome3
     , IF(c.ifok3 = 1, 1, 0)                                                         AS ifok3
     , IF(c.ifcome4 = 1, 1, 0)                                                       AS ifcome4
     , IF(c.ifok4 = 1, 1, 0)                                                         AS ifok4
     , IF(c.ifcome5 = 1, 1, 0)                                                       AS ifcome5
     , IF(c.ifok5 = 1, 1, 0)                                                         AS ifok5
     , IF(wx_rel_status IN (2, 3, 4) AND UNIX_TIMESTAMP(a.wx_add_time) - UNIX_TIMESTAMP(a.created_at) <= 300, 1,
          0)                                                                         AS wx_active
     , IF(ifcollect = 1 AND UNIX_TIMESTAMP(a.collect_time) - UNIX_TIMESTAMP(a.wx_add_time) <= 3600, 1,
          0)                                                                         AS collect_active
     -- 人为定义非标等渠道的消耗
     , CASE
    --20250326 增加百度-非标 45
           WHEN TO_DATE(a.created_at) >= '2025-03-24' AND a.platform_name = '百度' AND a.pos = '非标'
               THEN 45
    --20250221 增加TMK 30, 25.4.29日新增 TMK-海外召回，CAC也是30。
           WHEN TO_DATE(a.created_at) >= '2024-12-11' AND a.platform_name = 'TMK' THEN 30
    --20241211小糖私域CAC 45-->30
           WHEN TO_DATE(a.created_at) >= '2024-12-11' AND a.platform_name = '小糖私域' THEN 30
    --20241205小糖私域CAC 50-->45
           WHEN TO_DATE(a.created_at) >= '2024-12-05' AND a.platform_name = '小糖私域' THEN 45
           WHEN a.platform_name = '非标' AND a.pos = '大屏' THEN NULL --20241202剔除所有大屏
    --12.2日调整达人cac 50->45
           WHEN TO_DATE(a.created_at) >= '2024-12-02' AND a.platform_name = '非标' AND
                a.pos = '达人KOC' THEN 45
    -- 11.24日调整花螺40,kol直播60,达人koc50,其它40
           WHEN TO_DATE(a.created_at) >= '2024-11-24' AND a.platform_name = '非标' AND
                a.pos NOT IN ('KOL直播', '花螺直播', '达人KOC') THEN 40
           WHEN TO_DATE(a.created_at) >= '2024-11-24' AND a.platform_name = '非标' AND a.pos = '花螺直播'
               THEN 40
           WHEN TO_DATE(a.created_at) >= '2024-11-24' AND a.platform_name = '非标' AND a.pos = 'KOL直播' THEN 60
           WHEN TO_DATE(a.created_at) >= '2024-11-24' AND a.platform_name = '非标' AND
                a.pos = '达人KOC' THEN 50
    -- 11.18日调整花螺定价
           WHEN TO_DATE(a.created_at) >= '2024-11-18' AND a.platform_name = '非标' AND a.pos = '花螺直播'
               THEN 60
    -- 11.12日调整KOL定价
           WHEN TO_DATE(a.created_at) >= '2024-11-12' AND a.platform_name = '非标' AND a.pos = 'KOL直播' THEN 80
    -- 11.04日调整非标版位定价
           WHEN TO_DATE(a.created_at) >= '2024-11-04' AND a.platform_name = '非标' AND
                a.pos NOT IN ('KOL直播', '花螺直播', '达人KOC') THEN 50
           WHEN TO_DATE(a.created_at) >= '2024-11-04' AND a.platform_name = '非标' AND
                a.pos = 'KOL直播' THEN 100
           WHEN TO_DATE(a.created_at) >= '2024-11-04' AND a.platform_name = '非标' AND
                a.pos = '花螺直播' THEN 80
           WHEN TO_DATE(a.created_at) >= '2024-11-04' AND a.platform_name = '非标' AND
                a.pos = '达人KOC' THEN 60

    -- 之前逻辑
           WHEN a.platform_name = '非标' AND a.pos NOT IN ('花螺直播', 'KOL直播', '糖豆信息流', '达人KOC')
               THEN 50
           WHEN a.pos IN ('花螺直播', 'KOL直播') THEN IF(TO_DATE(a.created_at) >= '2024-10-16', 100, 120)
           WHEN a.pos = '糖豆信息流' THEN 50
           WHEN a.pos = '达人KOC' THEN 80
           WHEN a.platform_name = '小糖私域' THEN 50
    END                                                                              AS cac
     , CASE
    --20250326 增加百度-非标 45
           WHEN TO_DATE(a.created_at) >= '2025-03-24' AND a.platform_name = '百度' AND a.pos = '非标'
               THEN 45
    --20250221 增加TMK 30
           WHEN TO_DATE(a.created_at) >= '2024-12-11' AND a.platform_name = 'TMK' THEN 30
    --20241211小糖私域CAC 45-->30
           WHEN TO_DATE(a.created_at) >= '2024-12-11' AND a.platform_name = '小糖私域' THEN 30
    --20241205小糖私域CAC 50-->45
           WHEN TO_DATE(a.created_at) >= '2024-12-05' AND a.platform_name = '小糖私域' THEN 45
           WHEN a.platform_name = '非标' AND a.pos = '大屏' THEN NULL --20241202剔除所有大屏
    --12.2日调整达人cac 50->45
           WHEN TO_DATE(a.created_at) >= '2024-12-02' AND a.platform_name = '非标' AND
                a.pos = '达人KOC' THEN 45
    -- 11.24日调整花螺40,kol直播60,达人koc50,其它40
           WHEN TO_DATE(a.created_at) >= '2024-11-24' AND a.platform_name = '非标' AND
                a.pos NOT IN ('KOL直播', '花螺直播', '达人KOC') THEN 40
           WHEN TO_DATE(a.created_at) >= '2024-11-24' AND a.platform_name = '非标' AND a.pos = '花螺直播'
               THEN 40
           WHEN TO_DATE(a.created_at) >= '2024-11-24' AND a.platform_name = '非标' AND a.pos = 'KOL直播' THEN 60
           WHEN TO_DATE(a.created_at) >= '2024-11-24' AND a.platform_name = '非标' AND
                a.pos = '达人KOC' THEN 50
    -- 11.18日调整花螺定价
           WHEN TO_DATE(a.created_at) >= '2024-11-18' AND a.platform_name = '非标' AND a.pos = '花螺直播'
               THEN 60
    -- 11.12日调整KOL定价
           WHEN TO_DATE(a.created_at) >= '2024-11-12' AND a.platform_name = '非标' AND a.pos = 'KOL直播' THEN 80
    -- 11.04日调整非标版位定价
           WHEN TO_DATE(a.created_at) >= '2024-11-04' AND a.platform_name = '非标' AND
                a.pos NOT IN ('KOL直播', '花螺直播', '达人KOC') THEN 50
           WHEN TO_DATE(a.created_at) >= '2024-11-04' AND a.platform_name = '非标' AND
                a.pos = 'KOL直播' THEN 100
           WHEN TO_DATE(a.created_at) >= '2024-11-04' AND a.platform_name = '非标' AND
                a.pos = '花螺直播' THEN 80
           WHEN TO_DATE(a.created_at) >= '2024-11-04' AND a.platform_name = '非标' AND
                a.pos = '达人KOC' THEN 60

    -- 之前逻辑
           WHEN a.platform_name = '非标' AND a.pos NOT IN ('花螺直播', 'KOL直播', '糖豆信息流', '达人KOC')
               THEN 50
           WHEN a.pos IN ('花螺直播', 'KOL直播') THEN IF(TO_DATE(a.created_at) >= '2024-10-16', 100, 120)
           WHEN a.pos = '糖豆信息流' THEN 50
           WHEN a.pos = '达人KOC' THEN 80
           WHEN a.platform_name = '小糖私域' THEN 50
    END                                                                              AS cac_real
     -- 24.11.26 lyf 合并部分录入消耗 d_date逻辑，250314添加信息流二部手工处理。25.4.30 添加五个渠道版位
     , CASE
           WHEN a.pos IN ('千川直播', '腾讯视频号直播付费流', '本地推直播', '抖加', '小店随心推', '大屏',
                          '千川直播（虚拟）', '快手信息流下载', '360网盟', '新浪粉丝通', 'TikTok（东南亚）', 'TikTok（北美）',
                          '腾讯东南亚','脸书（东南亚）下载','谷歌（东南亚）下载')
               OR
                (a.pos IN ('脸书（东南亚）', '脸书（北美）') AND TO_DATE(a.created_at) < '2025-03-16')
               OR
                (a.pos IN ('谷歌（东南亚）', '谷歌（北美）') AND TO_DATE(a.created_at) < '2025-04-19')
               OR
                (a.pos IN ('抖音信息流下载', '腾讯信息流下载') AND TO_DATE(a.created_at) < '2025-03-30')
               OR (a.pos = '腾讯视频号直播付费流' AND a.cat = '道门八段锦')
               THEN CONCAT_WS(
                   '-',
                   CONCAT('20', SUBSTR(a.goods_name, 2, 2)),
                   SUBSTR(a.goods_name, 4, 2),
                   SUBSTR(a.goods_name, 6, 2))
           ELSE TO_DATE(a.created_at) END                                            AS d_date
     , IF(xs.is_abroad = 1, '海外', '国内')                                          AS is_abroad --20250122添加国内外标签
     , a.country_code
     , a.ip                                                                          AS xff
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 0 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D1
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 0 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D1
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 0 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D1
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 1 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D2
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 1 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D2
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 1 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D2
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 2 * 24) < 24
    , 1, 0)                                                                          AS pay_user_num_D3
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 2 * 24) < 24
    , b.first_pay_num, 0)                                                            AS pay_num_D3
     , IF(((TO_UNIX_TIMESTAMP(b.first_pay_time) -
            TO_UNIX_TIMESTAMP(TO_DATE(a.goods_date))) / 3600 - 2 * 24) < 24
    , b.first_pay_sum, 0)                                                            AS pay_sum_D3
FROM (SELECT *
           -- 新增过年期间几个期次起始日期为25年2月1日
           , IF(goods_name IN (
                               '【250124期】•武当秘传太极养生功•5天训练营',
                               '【250126期】•武当秘传太极养生功•5天训练营',
                               '【250128期】•武当秘传太极养生功•5天训练营',
                               '【250130期】•武当秘传太极养生功•5天训练营'
        ), '2025-02-01', CONCAT_WS('-', CONCAT('20', SUBSTR(goods_name, 2, 2)),
                                   SUBSTR(goods_name, 4, 2),
                                   SUBSTR(goods_name, 6, 2))) AS goods_date
      FROM dw.dwd_xt_user
      WHERE dt = '${datebuf}'
        AND TO_DATE(created_at) <= '${datebuf}'
         --这里先注释掉 背景：20250403 发现业务方有 期次包含“测试”二字 但是数据是需要的
--         AND goods_name NOT LIKE '%测试%' --20241114新增剔除测试数据逻辑
     ) a
         LEFT JOIN (SELECT * FROM dws.dws_sale_buy_course_day WHERE dt = '${datebuf}') b
                   ON a.xe_id = b.user_id
                       AND a.special_id = b.owner_class
         LEFT JOIN dwd.dwd_xiaoe_special xs
                   ON a.special_id = xs.special_id
    -- 25.4.18 新增逻辑：如果D4前下单未支付超过10分钟，则会通知销售追单，追到的单子，哪怕是D4前，也算作销售业绩
         LEFT JOIN dwd.dwd_crm_ai_notifications d
                   ON a.special_id = d.special_id AND a.member_id = d.member_id
         LEFT JOIN (SELECT id
                         , goods_id
                         , ifcome0
                         , ifok0
                         , ifcome1
                         , ifok1
                         , ifcome2
                         , ifok2
                         , ifcome3
                         , ifok3
                         , ifcome4
                         , ifok4
                         , ifcome5
                         , ifok5
                    FROM da.da_course_user_class_records
                    WHERE dt = '${datebuf}'
                    GROUP BY id
                           , goods_id
                           , ifcome0
                           , ifok0
                           , ifcome1
                           , ifok1
                           , ifcome2
                           , ifok2
                           , ifcome3
                           , ifok3
                           , ifcome4
                           , ifok4
                           , ifcome5
                           , ifok5) c
                   ON c.id = a.member_id
                       AND c.goods_id = a.special_id
         LEFT JOIN olduser f
                   ON a.member_id = f.member_id
                       AND a.goods_name = f.goods_name
         LEFT JOIN olduser30 e
                   ON a.member_id = e.member_id
                       AND a.goods_name = e.goods_name
;