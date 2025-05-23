CREATE TABLE IF NOT EXISTS dws.dws_xt_user_dt
(
    id               int COMMENT 'place_order自增ID',
    member_id        INT COMMENT '用户ID',
    user_id          string COMMENT 'user_id',
    platform         STRING COMMENT '渠道',
    h5_id            int COMMENT 'h5id',
    p_source         string COMMENT '渠道编号',
    openid           string COMMENT '支付小程序的openid',
    unionid          string COMMENT '微信unionid',
    phone            string COMMENT '手机号',
    sales_id         int COMMENT '销售id',
    sales_name       string COMMENT '销售姓名',
    department       string COMMENT '部门',
    user_group       string COMMENT '组',
    created_at       string COMMENT '创建时间',
    pay_time         string COMMENT '支付时间',
    price            int COMMENT '价格',
    transaction_id   string COMMENT '交易单号',
    out_trade_no     string COMMENT '内部订单号',
    refund_time      string COMMENT '退款时间',
    refund_price     int COMMENT '退款金额',
    updated_at       string COMMENT '更新时间',
    trade_state      string COMMENT '交易状态',
    refunds          int COMMENT '退款状态',
    member_status    int COMMENT '用户状态',
    ip               string COMMENT '下单的ip地址',
    special_id       string COMMENT '专栏id',
    goods_name       string COMMENT '专栏名称',
    mch_id           string COMMENT '交易商户号',
    corp_userid      string COMMENT '企微ID',
    wx_rel_status    int COMMENT '加微状态',
    wx_add_time      string COMMENT '添加微信时间',
    report_link_json string COMMENT '回传链接json',
    cat              string COMMENT '品类',
    platform_name    string COMMENT '渠道(中文)',
    pos              string COMMENT '版位',
    link_type_v2     string COMMENT '链路类型-新',
    mobile           string COMMENT '收集手机号',
    click_id         string COMMENT '填写表单对应点击ID',
    cost_id          STRING COMMENT '投放账户id',
    ad_id            string COMMENT '投放计划id',
    sucai_id         string COMMENT '投放素材id'
)
    COMMENT 'DWS层用户表（原xt_user）'
    PARTITIONED BY (dt string COMMENT '分区日期')
    STORED AS ORC;



DROP TABLE IF EXISTS place_order;
CREATE TEMPORARY TABLE place_order AS
SELECT t1.id
     , t1.member_id
     , t5.xe_id                     AS user_Id
     , t1.platform
     , t1.h5_id
     , t1.p_source
     , t1.openid
     , t4.unionid                            -- 微信从place_member中取
     , t4.mobile                    AS phone -- 手机号从place_member中取
     , t1.sales_id
     , SPLIT(ps.clean_name, '_')[0] AS sales_real_name
     -- 25年调整架构期间，使用销售的最新架构作为部门、组
     , CASE
           WHEN SUBSTR(t3.goods_name, 2, 6) BETWEEN '250309' AND '250313'
               THEN NVL(dept2.name, '其他')
           ELSE NVL(dept.name, '其他')
    END                             AS department
     , CASE
           WHEN SUBSTR(t3.goods_name, 2, 6) BETWEEN '250309' AND '250313'
               THEN NVL(grp2.name, '其他')
           ELSE NVL(grp.name, '其他')
    END                             AS user_group
     , t1.created_at
     , t1.pay_time
     , t1.price
     , t1.transaction_id
     , t1.out_trade_no
     , t1.refund_time
     , t1.refund_price
     , t1.updated_at
     , t1.trade_state
     , t1.refunds
     , t1.member_status
     , t1.ip
     , t1.wx_rel_status
     , t1.special_id
     , t3.goods_name
     , t1.mch_id
     , ps.corp_userid
     , t1.report_link_json
     , t1.cat
     , t1.click_id                           -- 新增从place_order中解析出的广告信息
     , t1.ad_id
     , t1.sucai_id
FROM (SELECT *
      FROM dwd.dwd_place_order_dt
      WHERE dt = '${datebuf}') t1
         LEFT JOIN dwd.dwd_place_sales ps
                   ON t1.sales_id = ps.id
         INNER JOIN dwd.dwd_xiaoe_special t3
                    ON t1.special_id = t3.special_id
         LEFT JOIN ods.ods_place_member t4
                   ON t1.member_id = t4.id
         LEFT JOIN ods.ods_xiaoe_member t5
                   ON t1.member_id = t5.id
         LEFT JOIN (SELECT * FROM ods.ods_crm_department_group WHERE parentid = 0) dept
                   ON t1.department = dept.id
         LEFT JOIN (SELECT * FROM ods.ods_crm_department_group WHERE parentid != 0) grp
                   ON t1.user_group = grp.id
         LEFT JOIN (SELECT * FROM ods.ods_crm_department_group WHERE parentid = 0) dept2 -- 0309 ~0313 销售使用最新的部门
                   ON ps.department = dept2.id
         LEFT JOIN (SELECT * FROM ods.ods_crm_department_group WHERE parentid != 0) grp2
                   ON ps.user_group = grp2.id;


-- 处理封号状态下的销售加微时间
DROP TABLE IF EXISTS change_count;
CREATE TEMPORARY TABLE change_count AS
SELECT a.ex_unionid
     --, a.sales_id
     , a.special_id
     , MIN(b.created_at) AS created_at
FROM (SELECT ex_unionid, member_id, special_id, sales_id FROM dw.dws_place_order_firstsale) a
         LEFT JOIN
     ods.ods_place_sales c
     ON a.sales_id = c.id
         LEFT JOIN
     (SELECT *
      FROM ods.ods_place_contact
      WHERE ex_unionid IS NOT NULL
        AND ex_unionid <> '') b
     ON a.ex_unionid = b.ex_unionid
         AND c.corp_userid = b.userid
GROUP BY a.ex_unionid
       --, a.sales_id
       , a.special_id;


DROP TABLE IF EXISTS base_user_add_wx;
CREATE TEMPORARY TABLE base_user_add_wx AS
    -- 添加加微时间
SELECT t1.id
     , t1.member_id
     , t1.user_id
     , t1.platform
     , t1.h5_id
     , t1.p_source
     , t1.openid
     , t1.unionid
     , t1.phone
     , t1.sales_id
     , t1.sales_real_name                                              AS sales_name
     , NVL(ai.ai_type, '无')                                           AS sales_sop_type
     , CASE
    -- AI销售测试期间，使用陈雪萍提供的维表对销售进行打标 3月20日更改：更改AI的打标逻辑，0309期-0321期按照陈雪萍提供的维表进行打标。3月21日之后按照系统配置进行打标。
           WHEN SUBSTR(t1.goods_name, 2, 6) BETWEEN '250309' AND '250321' AND tmp.is_ai = 1
               THEN CONCAT('AI-', t1.department)
           WHEN SUBSTR(t1.goods_name, 2, 6) BETWEEN '250309' AND '250321' AND
                (tmp.is_ai = 0 OR tmp.goods_name IS NULL)
               THEN CONCAT('人工-', t1.department)
           WHEN SUBSTR(t1.goods_name, 2, 6) > '250321' AND (ai.goods_name IS NOT NULL) AND
                (ai.sales_id IS NOT NULL) THEN CONCAT('AI-', t1.department)
           WHEN SUBSTR(t1.goods_name, 2, 6) > '250321'
               AND ai.sales_id IS NULL
               AND t1.sales_id > 0 THEN CONCAT('人工-', t1.department)
           ELSE t1.department
    END                                                                AS department
     , t1.user_group
     , t1.created_at
     , t1.pay_time
     , t1.price
     , t1.transaction_id
     , t1.out_trade_no
     , t1.refund_time
     , t1.refund_price
     , t1.updated_at
     , t1.trade_state
     , t1.refunds
     , t1.member_status
     , t1.ip
     --,t1.wx_rel_status
     , t1.special_id
     , t1.goods_name
     , t1.mch_id
     , t1.corp_userid
     , CASE
           WHEN TO_DATE(t1.created_at) >= '2024-08-05' THEN t1.wx_rel_status -- 20240912 汤文奇 修改两套逻辑分割时间为0805
           ELSE IF(t3.ex_unionid IS NOT NULL, t3.wx_rel_status, 1) END AS wx_rel_status
     , COALESCE(t11.created_at, t2.created_at, t3.created_at, '')      AS wx_add_time
     , t1.report_link_json
     , t1.cat
     , t1.click_id
     , t1.ad_id
     , t1.sucai_id
FROM place_order t1
         LEFT JOIN (SELECT * FROM dws.dws_ai_sales_dt WHERE dt = '${datebuf}') ai -- 20250311增加AI销售清单
                   ON t1.sales_id = ai.sales_id
                       AND t1.goods_name = ai.goods_name
         LEFT JOIN dim.dim_sales_ai_250321_temp tmp -- 20250320增加陈雪萍AI标签
                   ON t1.goods_name = tmp.goods_name
                       AND t1.sales_real_name = tmp.sales_real_name
    -- 封号销售
         LEFT JOIN change_count t11
                   ON t1.unionid = t11.ex_unionid AND t1.special_id = t11.special_id
    -- 使用客户微信id和销售微信id精准匹配
         LEFT JOIN (SELECT ex_unionid
                         , userid
                         , MIN(created_at) AS created_at -- 取用户微信+销售id的首次加微时间
                    FROM ods.ods_place_contact
                    WHERE ex_unionid IS NOT NULL
                      AND ex_unionid <> ''
                    GROUP BY ex_unionid
                           , userid) t2
                   ON t1.unionid = t2.ex_unionid
                       AND t1.corp_userid = t2.userid
    -- 使用客户微信，模糊匹配最早的加微时间
         LEFT JOIN (SELECT ex_unionid
                         , MAX(wx_relation_status) AS wx_rel_status
                         , MIN(created_at)         AS created_at -- 取用户微信的首次加微时间
                    FROM ods.ods_place_contact
                    WHERE ex_unionid IS NOT NULL
                      AND ex_unionid <> ''
                    GROUP BY ex_unionid) t3
                   ON t1.unionid = t3.ex_unionid;


-- 例子解析：优先加微回传、支付回传、订单回传（其中每项包含回传、回传对应点击）

-- 例子解析1: 订单、表单点击
DROP TABLE IF EXISTS add_order_click;
CREATE TEMPORARY TABLE add_order_click AS
SELECT t1.id
     , t1.member_id
     , t1.user_id
     , t1.platform
     , t1.h5_id
     , t1.p_source
     , t1.openid
     , t1.unionid
     , t1.phone
     , t1.sales_id
     , t1.sales_name
     , t1.department
     , t1.user_group
     , t1.created_at
     , t1.pay_time
     , t1.price
     , t1.transaction_id
     , t1.out_trade_no
     , t1.refund_time
     , t1.refund_price
     , t1.updated_at
     , t1.trade_state
     , t1.refunds
     , t1.member_status
     , t1.ip
     , t1.special_id
     , t1.goods_name
     , t1.mch_id
     , t1.corp_userid
     , t1.wx_rel_status
     , t1.wx_add_time
     , t1.report_link_json
     , t1.cat
     , t1.click_id
     -- 由于后面需要模糊匹配回传，这里使用lag获取重复例子的下一次提交表单时间。
     , LAG(t2.created_at, 1, '9999-01-01 00:00:00')
           OVER (PARTITION BY member_id,cat ORDER BY SUBSTR(goods_name, 2, 6) DESC) AS next_period_created_at
     , NVL(t1.ad_id, t2.ad_id)                                                      AS ad_id
     , NVL(t1.sucai_id, t2.sucai_id)                                                AS sucai_id
FROM base_user_add_wx t1
         LEFT JOIN
     dwd.dwd_marketing_ad_click_dt t2
     ON t1.click_id = t2.click_id;


-- 回传模糊匹配情况:report_log表没有place_order_id，根据用户进入训练营的时间模糊匹配回传。
DROP TABLE first_order_report;
CREATE TEMPORARY TABLE first_order_report AS
SELECT *
FROM (SELECT usr.id
           , usr.member_id
           , usr.special_id
           , usr.created_at
           , usr.next_period_created_at
           , rpt.h5_id
           , rpt.event_type
           , rpt.created_at                                                                                     AS report_at
           , rpt.report_link
           , rpt.ad_Id
           , rpt.sucai_id
           , RANK() OVER (PARTITION BY usr.member_id,usr.special_id,rpt.event_type ORDER BY rpt.created_at ASC) AS rnk
      FROM add_order_click usr
               LEFT JOIN
           -- 没有place_order_id的进行模糊匹配
                   (SELECT * FROM dws.dws_report_log_add_click_dt WHERE place_order_id = 0) rpt
           ON usr.member_id = rpt.member_id
-- 回传时间在order表的created_at和next_period_created_at之间
      WHERE rpt.created_at >= usr.created_at
        AND rpt.created_at <= usr.next_period_created_at) a
WHERE rnk = 1
;



DROP TABLE parse_users;
CREATE TEMPORARY TABLE parse_users AS
SELECT t1.id
     , t1.member_id
     , t1.user_id
     , t1.platform
     , COALESCE(rpt_1.h5_id, rpt_2.h5_id, t1.h5_id, fr1.h5_id, fr2.h5_id)                AS h5_id
     , t1.p_source
     , t1.openid
     , t1.unionid
     , t1.phone
     , t1.sales_id
     , t1.sales_name
     , t1.department
     , t1.user_group
     , t1.created_at
     , t1.pay_time
     , t1.price
     , t1.transaction_id
     , t1.out_trade_no
     , t1.refund_time
     , t1.refund_price
     , t1.updated_at
     , t1.trade_state
     , t1.refunds
     , t1.member_status
     , t1.ip
     , t1.special_id
     , t1.goods_name
     , t1.mch_id
     , t1.corp_userid
     , t1.wx_rel_status
     , t1.wx_add_time
     , t1.report_link_json
     , t1.cat
     , t1.click_id
     , COALESCE(rpt_1.ad_id, rpt_2.ad_id, t1.ad_id, fr1.ad_Id, fr2.ad_Id)                AS ad_id
     , COALESCE(rpt_1.sucai_id, rpt_2.sucai_id, t1.sucai_id, fr1.sucai_id, fr2.sucai_id) AS sucai_id
FROM add_order_click t1
         LEFT JOIN
     (SELECT *
      FROM dws.dws_report_log_add_click_dt
      WHERE place_order_id != 0
        AND event_type = 'EVENT_ADD_WX') rpt_1
     ON t1.id = rpt_1.place_order_id
         LEFT JOIN
     (SELECT *
      FROM dws.dws_report_log_add_click_dt
      WHERE place_order_id != 0
        AND event_type = 'EVENT_PAY') rpt_2
     ON t1.id = rpt_2.place_order_id
         LEFT JOIN
         (SELECT * FROM first_order_report WHERE event_type = 'EVENT_ADD_WX') fr1
         ON t1.id = fr1.id
             AND t1.special_id = fr1.special_id
             AND t1.wx_rel_status != 1
         LEFT JOIN
         (SELECT * FROM first_order_report WHERE event_type = 'EVENT_PAY') fr2
         ON t1.id = fr2.id
             AND t1.special_id = fr2.special_id
             AND (UNIX_TIMESTAMP(fr2.created_at) - UNIX_TIMESTAMP(t1.pay_time)) <= 60;



INSERT OVERWRITE TABLE dws.dws_xt_user_dt PARTITION (dt = '${datebuf}')
SELECT a.id
     , a.member_id
     , a.user_id
     , a.platform
     , a.h5_id
     , a.p_source
     , a.openid
     , a.unionid
     , a.phone
     , a.sales_id
     , a.sales_name
     , a.department
     , a.user_group
     , a.created_at
     , a.pay_time
     , a.price
     , a.transaction_id
     , a.out_trade_no
     , a.refund_time
     , a.refund_price
     , a.updated_at
     , a.trade_state
     , a.refunds
     , a.member_status
     , a.ip
     , a.special_id
     , a.goods_name
     , a.mch_id
     , a.corp_userid
     , a.wx_rel_status
     , a.wx_add_time
     , a.report_link_json
     , a.cat
     , t2.platform_name
     , t2.pos
     , t2.link_type_v2
     , t2.mobile
     , a.click_id
     , b.cost_id
     , a.ad_id
     , a.sucai_id
FROM parse_users a
         LEFT JOIN
     (SELECT advertiser_id AS cost_id
           , promotion_id  AS ad_id
      FROM dw.hd2016_marketing_report --当前通过历史所有的api消耗数据聚合得出, 截止240722，聚合后有约20万条数据, 后续需要优化
      WHERE cdate <= '${datebuf}'
      GROUP BY advertiser_id
             , promotion_id) b
     ON a.ad_id = b.ad_id
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
     ON a.h5_id = t2.h5_id;