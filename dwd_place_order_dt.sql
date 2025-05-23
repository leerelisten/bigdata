-- 解析json

-- 解析目标：qz_gdt、clickid、gdt_vid
-- 解析：promotionid、ad_id、wx_aid、weixinadinfo、aid、mastplanid、baidu_adid、adgroup_id
DROP TEMPORARY FUNCTION IF EXISTS parse_url2;
CREATE TEMPORARY FUNCTION parse_url2 AS 'com.td.bigdata.udf.UrlToJsonUDF' USING JAR 'hdfs:///user/admin/parse_url-3.0-SNAPSHOT.jar';


CREATE TABLE IF NOT EXISTS dwd.dwd_place_order_dt
(
    `id`               INT COMMENT 'ID自增',
    `member_id`        INT COMMENT '用户ID',
    `out_trade_no`     STRING COMMENT '订单号',
    `transaction_id`   STRING COMMENT '微信交易号',
    `openid`           STRING COMMENT '支付openid',
    `trade_state`      STRING COMMENT '通知返回支付状态',
    `refunds`          INT COMMENT '是否退款',
    `created_at`       STRING COMMENT '创建时间',
    `updated_at`       STRING COMMENT '更新时间',
    `mch_id`           STRING COMMENT '微信商户号 小糖1513678111，乐学1640062978',
    `special_id`       STRING COMMENT '专栏ID',
    `h5_id`            INT COMMENT 'h5素材id',
    `sales_id`         INT COMMENT '分配销售',
    `platform`         STRING COMMENT '平台',
    `report_link`      STRING COMMENT '转化回传',
    `ip`               STRING COMMENT 'ip',
    `wx_rel_status`    SMALLINT COMMENT '1=>未添加微信,2=>已添加微信,3=>单向好友',
    `department`       INT COMMENT 'sales_id部门',
    `user_group`       INT COMMENT 'sales_id用户组',
    `p_source`         STRING COMMENT '渠道编号',
    `pay_time`         STRING COMMENT '支付成功时间',
    `refund_time`      STRING COMMENT '退款成功时间',
    `refund_price`     INT COMMENT '退款金额',
    `member_status`    INT COMMENT '例子订单是否有效=1有效 =0无效',
    `source_order_id`  BIGINT COMMENT '来源订单ID',
    `report_link_json` string COMMENT 'report_link转译Json',
    `cat`              string COMMENT '品类',
    `platform_name`    string COMMENT '渠道',
    `pos`              string COMMENT '版位',
    `link_type_v2`     string COMMENT '链路',
    `price`            INT COMMENT '支付金额',
    `mobile`           string COMMENT '是否收集手机号',


    `click_id`         STRING COMMENT '点击ID',
    `ad_id`            STRING COMMENT '计划ID',
    `sucai_id`         STRING COMMENT '素材id'
)
    COMMENT 'DWD训练营订单表'
    PARTITIONED BY (
        `dt` STRING)
    STORED AS ORCFILE;

ALTER TABLE dwd.dwd_place_order_dt
    DROP IF EXISTS PARTITION (dt = '${datebuf}');
ALTER TABLE dwd.dwd_place_order_dt
    ADD IF NOT EXISTS PARTITION (dt = '${datebuf}');

DROP TABLE IF EXISTS result;
CREATE TEMPORARY TABLE result AS
SELECT id
     , member_id
     , out_trade_no
     , IF(transaction_id = '', NULL, transaction_id) AS transaction_id
     , IF(openid = '', NULL, openid)                 AS openid
     , IF(trade_state = '', NULL, trade_state)       AS trade_state
     , refunds
     , created_at
     , updated_at
     , IF(mch_id = '', NULL, mch_id)                 AS mch_id
     , special_id
     , h5_id
     , IF(sales_id = 0, NULL, sales_id)              AS sales_id
     , platform
     , report_link
     , ip
     , wx_rel_status
     , IF(department = 0, NULL, department)          AS department
     , IF(user_group = 0, NULL, user_group)          AS user_group
     , p_source
     , IF(pay_time = 'null', NULL, pay_time)         AS pay_time
     , IF(refund_time = 'null', NULL, refund_time)   AS refund_time
     , refund_price
     , member_status
     , source_order_id
     -- promotionid、ad_id、wx_aid、weixinadinfo、aid、mastplanid、baidu_adid、adgroup_id
     , PARSE_URL2(report_link)                       AS report_link_json
FROM ods.ods_place_order;

DROP TABLE IF EXISTS parse_json;
CREATE TEMPORARY TABLE parse_json AS
SELECT id
     , member_id
     , out_trade_no
     , transaction_id
     , openid
     , trade_state
     , refunds
     , created_at
     , updated_at
     , mch_id
     , special_id
     , h5_id
     , sales_id
     , IF(platform = '', NULL, platform)                                           AS platform
     , report_link
     , ip
     , wx_rel_status
     , department
     , user_group
     , p_source
     , pay_time
     , refund_time
     , refund_price
     , member_status
     , source_order_id
     , report_link_json
     -- promotionid、ad_id、wx_aid、weixinadinfo、aid、mastplanid、baidu_adid、adgroup_id
     , GET_JSON_OBJECT(report_link_json, '$.params.platform')                      AS platform2
     , GET_JSON_OBJECT(report_link_json, '$.params.h5_id')                         AS h5_id2
     , GET_JSON_OBJECT(report_link_json, '$.params.clickid')                       AS click_id

     , GET_JSON_OBJECT(report_link_json, '$.params.bd_vid')                        AS bd_vid
     , GET_JSON_OBJECT(report_link_json, '$.params.qz_gdt')                        AS qz_gdt
     , GET_JSON_OBJECT(report_link_json, '$.params.gdt_vid')                       AS gdt_vid

     , GET_JSON_OBJECT(report_link_json, '$.params.promotionid')                   AS douyin_promotionid
     , GET_JSON_OBJECT(report_link_json, '$.params.adid')                          AS douyin_adid
     , GET_JSON_OBJECT(report_link_json, '$.params.material_id')                   AS douyin_material_id

     , GET_JSON_OBJECT(report_link_json, '$.params.aid')                           AS kuaishou_aid

     , GET_JSON_OBJECT(report_link_json, '$.params.wx_aid')                        AS wx_aid
     , SPLIT(GET_JSON_OBJECT(report_link_json, '$.params.weixinadinfo'), '\\.')[0] AS wx_weixinadinfo
     , GET_JSON_OBJECT(report_link_json, '$.params.adgroup_id')                    AS wxmp_adgroup_id


     , GET_JSON_OBJECT(report_link_json, '$.params.baidu_adid')                    AS baidu_adid
     , GET_JSON_OBJECT(report_link_json, '$.params.baidu_unitid')                  AS baidu_unitid

     , GET_JSON_OBJECT(report_link_json, '$.params.mastplanid')                    AS oppo_mastplanid
     , GET_JSON_OBJECT(report_link_json, '$.params.campaign_id')                   AS fb_campaign_id
FROM result;


-- 过滤部分脏数据
DROP TABLE filter_step;
CREATE TEMPORARY TABLE filter_step AS
SELECT id
     , member_id
     , out_trade_no
     , IF(transaction_id = '', NULL, transaction_id)                                      AS transaction_id
     , IF(openid = '', NULL, openid)                                                      AS openid
     , IF(trade_state = '', NULL, trade_state)                                            AS trade_state
     , refunds
     , created_at
     , updated_at
     , IF(mch_id = '', NULL, mch_id)                                                      AS mch_id
     , IF(special_id = '', NULL, special_id)                                              AS special_id
     , IF(h5_id2 != '', h5_id2, h5_id)                                                    AS h5_id
     , IF(sales_id = 0, NULL, sales_id)                                                   AS sales_id
     , IF((platform2 IS NOT NULL) AND (platform2 != ''), platform2, platform)             AS platform
     , report_link
     , ip
     , wx_rel_status
     , IF(department = 0, NULL, department)                                               AS department
     , IF(user_group = 0, NULL, user_group)                                               AS user_group
     , p_source
     , IF(pay_time = 'null', NULL, pay_time)                                              AS pay_time
     , IF(refund_time = 'null', NULL, refund_time)                                        AS refund_time
     , refund_price
     , member_status
     , source_order_id
     , report_link_json

     , IF(click_id = '__CLICKID__', NULL, click_id)                                       AS click_id
     , bd_vid
     , IF(qz_gdt IN ('__tracestring__', 'This_Is_A_Example_Trace-String!'), NULL, qz_gdt) AS qz_gdt
     , IF(gdt_vid = 'xxx', NULL, gdt_vid)                                                 AS gdt_vid


     , IF(LENGTH(douyin_promotionid) = 19, douyin_promotionid, NULL)                      AS douyin_promotionid
     , IF(LENGTH(douyin_adid) = 16, douyin_adid, NULL)                                    AS douyin_adid
     , IF(LENGTH(douyin_material_id) = 19, douyin_material_id, NULL)                      AS douyin_material_id

     , IF(LENGTH(kuaishou_aid) = 10, kuaishou_aid, NULL)                                  AS kuaishou_aid

     , IF(LENGTH(wx_aid) = 11, wx_aid, NULL)                                              AS wx_aid
     , IF(LENGTH(wx_weixinadinfo) IN (10, 11), wx_weixinadinfo, NULL)                     AS wx_weixinadinfo
     , IF(LENGTH(wxmp_adgroup_id) = 11, wxmp_adgroup_id, NULL)                            AS wxmp_adgroup_id

     , IF(LENGTH(baidu_adid) = 9, baidu_adid, NULL)                                       AS baidu_adid
     , IF(LENGTH(baidu_unitid) IN (10, 11), baidu_unitid, NULL)                           AS baidu_unitid

     , IF(LENGTH(oppo_mastplanid) = 9, oppo_mastplanid, NULL)                             AS oppo_mastplanid
     , IF(LENGTH(fb_campaign_id) = 18, fb_campaign_id, NULL)                              AS fb_campaign_id
FROM parse_json;



INSERT
    OVERWRITE
    TABLE dwd.dwd_place_order_dt PARTITION (dt = '${datebuf}')
SELECT t1.id
     , t1.member_id
     , t1.out_trade_no
     , t1.transaction_id
     , t1.openid
     , t1.trade_state
     , t1.refunds
     , t1.created_at
     , t1.updated_at
     , t1.mch_id
     , t1.special_id
     , t1.h5_id
     , t1.sales_id
     , t1.platform
     , t1.report_link
     , t1.ip
     , t1.wx_rel_status
     , t1.department
     , t1.user_group
     , t1.p_source
     , t1.pay_time
     , t1.refund_time
     , t1.refund_price
     , t1.member_status
     , t1.source_order_id
     , t1.report_link_json
     , t2.cat
     , t2.platform_name
     , t2.pos
     , t2.link_type_v2
     , t2.price
     , t2.mobile
     -- 只有抖音叫click_id，其他渠道不叫click_id
     , CASE
           WHEN t1.platform = 'baidu' THEN t1.bd_vid
           WHEN t1.platform IN
                ('wx', 'wxmini', 'wxpcad', 'manual', 'wxmp', 'tencent', 'wxyoulianghui', 'wxyoulianhui')
               THEN t1.gdt_vid
           ELSE t1.click_id
    END AS click_id

     , CASE t1.platform
    -- 抖音优先订单表的report_link,其次report_link带的点击，其次回传，其次回传的点击
           WHEN 'douyin' THEN NVL(t1.douyin_promotionid, t1.douyin_adid)
           WHEN 'wx' THEN NVL(t1.wx_aid, t1.wx_weixinadinfo)
           WHEN 'wxmini' THEN t1.wx_weixinadinfo
           WHEN 'wxmp' THEN t1.wxmp_adgroup_id
           WHEN 'kuaishou' THEN t1.kuaishou_aid
           WHEN 'oppo' THEN t1.oppo_mastplanid
    -- 百度优先订单表的点击，其次report_link
           WHEN 'baidu' THEN t1.baidu_adid
           WHEN 'facebook' THEN t1.fb_campaign_id
    END AS ad_id
     , CASE
    -- 抖音优先订单表的report_link,其次report_link带的点击，其次回传，其次回传的点击
           WHEN t1.platform = 'douyin' THEN t1.douyin_material_id
           WHEN t1.platform = 'baidu' AND t2.pos = '百度搜索' THEN t1.baidu_unitid
    END AS sucai_id
FROM filter_step t1
         LEFT JOIN
     (SELECT h5_id
           , category AS cat --品类信息
           , ad_department
           , pos             --版位信息
           , link_type_v2    --链路类型-新
           , platform_name
           , mobile
           , price
      FROM (SELECT h5_id
                 , category
                 , ad_department
                 , platform
                 , NVL(IF(pltf.long_name = '腾讯pcad', '腾讯', pltf.long_name), platform)           platform_name -- 20240808 添加腾讯公众号关注渠道，与肖傲沟通和腾讯区分开来
                 , CASE
                       WHEN platform_section = '腾讯视频号付费流' THEN '腾讯视频号直播付费流'
                       WHEN platform_section = '腾讯视频号免费流' THEN '腾讯视频号直播免费流'
                       WHEN platform_section = '腾讯视频号直播' THEN '腾讯视频号直播免费流'
                       WHEN platform_section = '腾讯视频号' AND
                            platform IN ('wx', 'wxpcad', 'wxyoulianghui', 'wxyoulianhui')
                           THEN '腾讯视频号信息流'
              -- 24.12.30 数据标准化暂时添加到此
                       ELSE IF(platform_section IS NULL OR platform_section IN ('其它', ''), '其他',
                               platform_section) END                                                pos           -- 20240808 腾讯公众号关注渠道的腾讯视频号不做映射处理，与肖傲确认，和腾讯渠道下面的腾讯视频号是两码事
                 , NVL(lt.long_name, '无')                                                          link_type_v2
                 , IF(mobile_show IN (1, 2), '有手机号', '无手机号')                                mobile
                 , CONCAT(CAST(CASE
                                   WHEN PMOD(price, 100) = 0 THEN CAST(ROUND(price / 100, 0) AS int)
                                   ELSE CAST(ROUND(price / 100, 2) AS string) END AS string), '元') price
                 , ROW_NUMBER() OVER (PARTITION BY h5_id ORDER BY updated_at DESC)                  rn            --取更新后的最新一条
            FROM dim.dim_place_h5 h5
                     LEFT JOIN (SELECT full_name, long_name
                                FROM ods.ods_place_config
                                WHERE type = 'platform'
                                  AND parent_name = '') pltf
                               ON h5.platform = pltf.full_name
                     LEFT JOIN (SELECT full_name, long_name
                                FROM ods.ods_place_config
                                WHERE type = 'link_type') lt
                               ON h5.link_type = lt.full_name
            WHERE TO_DATE(updated_at) <= '${datebuf}') tt
      WHERE rn = 1) t2
     ON t1.h5_id = t2.h5_id;

