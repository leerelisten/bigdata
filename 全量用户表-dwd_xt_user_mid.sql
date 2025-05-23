-- 建表思路：基于place_order，匹配H5例子、账户-计划-素材、问卷

SET mapred.job.name="dwd_xt_user_mid#${datebuf}";
USE dw;
CREATE EXTERNAL TABLE IF NOT EXISTS dw.dwd_xt_user_mid
(
    member_id           int COMMENT '用户ID',
    cat                 string COMMENT '品类',
    ad_department       string COMMENT '投放部门',
    platform            string COMMENT '渠道',
    platform_name       string COMMENT '渠道(中文)',
    h5_id               int COMMENT 'h5id',
    p_source            string COMMENT '渠道编号',
    pos                 string COMMENT '版位',
    price               string COMMENT '价格',
    mobile              string COMMENT '收集手机号',
    link_type_v1        string COMMENT '链路类型-旧' -- x元有/无手机号
    ,
    link_type_v2        string COMMENT '链路类型-新' -- 小程序加微,获客助手,企微活码,新一键授权,无
    ,
    cost_id             string COMMENT '投放账户id',
    ad_id               string COMMENT '投放计划id',
    sucai_id            string COMMENT '投放素材id',
    report_link         string COMMENT '回传链接',
    openid              string COMMENT '支付小程序的openid',
    unionid             string COMMENT '微信unionid',
    phone               string COMMENT '手机号',
    sales_id            int COMMENT '销售id',
    sales_name          string COMMENT '销售姓名',
    sales_sop_type      string COMMENT '销售SOP类型',
    department          string COMMENT '部门',
    user_group          string COMMENT '组',
    created_at          string COMMENT '创建时间',
    pay_time            string COMMENT '支付时间',
    pay_price           string COMMENT '支付金额',
    transaction_id      string COMMENT '交易单号',
    out_trade_no        string COMMENT '内部订单号',
    refund_time         string COMMENT '退款时间',
    refund_price        string COMMENT '退款金额',
    updated_at          string COMMENT '更新时间',
    trade_state         string COMMENT '交易状态'    -- SUCCESS/PREPARE:成功/临时用户池,REFUND:已退款
    ,
    refunds             int COMMENT '退款状态'       -- 0:无退款,1:已退款
    ,
    member_status       int COMMENT '用户状态'       -- 1:有效用户
    ,
    ip                  string COMMENT '下单的ip地址',
    wx_rel_status       string COMMENT '加微状态'    -- 1:未添加,2:已添加,3:单向好友
    ,
    wx_add_time         string COMMENT '添加微信时间',
    special_id          string COMMENT '专栏id',
    goods_name          string COMMENT '专栏名称',
    mch_id              string COMMENT '交易商户号',
    contact_ex_nickname string COMMENT '用户昵称',
    is_get_ticket       string COMMENT '已领券',
    xe_id               string COMMENT '小鹅通ID',
    ifcollect           string COMMENT '是否填写问卷',
    sex                 string COMMENT '性别',
    age                 int COMMENT '年龄',
    age_level           string COMMENT '年龄层',
    address             string COMMENT '地址',
    city_level          string COMMENT '城市等级',
    work                string COMMENT '职业',
    collect_form_name   string COMMENT '问卷标题',
    collect_time        string COMMENT '问卷采集时间',
    extra               string COMMENT '问卷内容json',
    country_code        string COMMENT '国家编码'
)
    COMMENT '培训业务-期次用户明细-中间表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/olap/db/dwd_xt_user_mid/';

SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8196;
SET mapreduce.reduce.memory.mb=8196;
-- set hive.execution.engine = tez;
SET hive.exec.parallel=true;
SET mapred.map.tasks =20;
-- set mapred.reduce.tasks = 10;
SET hive.exec.parallel.thread.number=12;
-- 更新记录
-- 20240808 添加腾讯公众号关注渠道，与肖傲沟通和腾讯区分开来
-- 20240812 当渠道为“腾讯公众号关注时”从report_link中抓取投放计划id
-- 20240821 新增百度解析规则,使用宏参数解析(百度搜索)
-- 20240821 新增百度素材ID,用宏参数解析(百度搜索)
-- 20240822 解析腾讯渠道下投放计划ID情况D
-- 20240902 H5_id全部使用线上维表
-- 20240912 头条直播换直播间或渠道加微导致根据回传链接clickid解析不出计划，修复该问题；增加tencent、fenxiao、xiaotangsiyu的转译；修改加微时间判断逻辑分割线为0805
-- 待调整
-- 腾讯账户计划解析办法可能需要调整qz_gdt|gdt_vid,先和click表中的clickid关联,然后再和click表中report_link中的request_id|impression_id关联


drop TABLE IF EXISTS chage_id;
CREATE TEMPORARY TABLE chage_id AS
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

drop TABLE IF EXISTS base_user_row_data;
CREATE TEMPORARY TABLE base_user_row_data as
   SELECT t1.id
          , t1.member_id
          , t3.cat
          , t1.platform
          , t1.h5_id
          , t1.p_source
          , t1.report_link
          , CASE
                WHEN t1.p_source = '' OR t1.p_source IS NULL
                    THEN NVL(SUBSTR(SPLIT(t1.report_link, 'p_source=')[1], 0, 3), '无')
                ELSE t1.p_source
            END                                                 AS p_source_1 -- 为匹配手工h5维表中的p_source为空的定义为“无”的情况
          , CASE
                WHEN t1.platform = 'douyin' AND t1.report_link RLIKE 'live_' -- 优化判断方式,避免reportlink中无意包含了live字段
                    THEN 'live'
                ELSE '无'
            END                                                 AS live       -- 为匹配手工h5维表中头条渠道下回传的特殊参数作为匹配参数，其他情况定义为“无”
          , CASE
                WHEN t1.platform = 'baidu'
                    THEN CASE
                             WHEN t1.report_link LIKE '%a_id=ss%'
                                 THEN 'a_id=ss'
                             WHEN t1.report_link LIKE '%a_id=xxl%'
                                 THEN 'a_id=xxl'
                             ELSE '无'
                    END
                ELSE '无'
            END                                                 AS a_id       -- 为匹配手工h5维表中百度渠道下回传的特殊参数作为匹配参数，其他情况定义为“无”
          , CASE
                WHEN t1.platform = 'wx' AND t1.report_link LIKE '%form_type=2%'
                    THEN 'form_type=2'
                ELSE '无'
            END                                                 AS form_type  -- 为匹配手工h5维表中腾讯渠道下回传的特殊参数作为匹配参数，其他情况定义为“无”
          , t1.openid
          , t4.unionid                                                        -- 微信从place_member中取
          , t4.mobile                                           AS phone      -- 手机号从place_member中取
          , t1.sales_id
          , t2.name                                             AS sales_name
          , SPLIT(ps.clean_name, '_')[0]                        AS sales_real_name
          -- 单独处理250309 ~ 250311 期次销售对应的部门 取其最新的部门 组。4月25日再次调整，口径为0424、0426期次按照最新架构处理
          , CASE
                WHEN SUBSTR(t3.goods_name, 2, 6) IN ('250309', '250311', '250313', '250424', '250426')
                    THEN NVL(dept2.name, '其他')
                ELSE NVL(dept.name, '其他')
            END                                                 AS department
          , CASE
                WHEN SUBSTR(t3.goods_name, 2, 6) IN ('250309', '250311', '250313', '250424', '250426')
                    THEN NVL(grp2.name, '其他')
                ELSE NVL(grp.name, '其他')
            END                                                 AS user_group
          , t1.created_at
          , t1.pay_time
          , t1.price / 100                                      AS pay_price
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
          , REGEXP_REPLACE(TRIM(t3.goods_name), '【2024', '【24') AS goods_name
          , t1.mch_id
          , t5.xe_id
          , t2.corp_userid
     FROM (SELECT * FROM ods.ods_place_order_dt WHERE dt = '${datebuf}') t1
              LEFT JOIN ods.ods_place_sales t2
                        ON t1.sales_id = t2.id
              INNER JOIN dwd.dwd_xiaoe_special t3
                         ON t1.special_id = t3.special_id
              LEFT JOIN dwd.dwd_place_sales ps
                        ON t2.id = ps.id
              LEFT JOIN ods.ods_place_member t4
                        ON t1.member_id = t4.id
              LEFT JOIN ods.ods_xiaoe_member t5
                        ON t1.member_id = t5.id
              LEFT JOIN (SELECT * FROM ods.ods_crm_department_group WHERE parentid = 0) dept
                        ON t1.department = dept.id
              LEFT JOIN (SELECT * FROM ods.ods_crm_department_group WHERE parentid != 0) grp
                        ON t1.user_group = grp.id
              LEFT JOIN (SELECT * FROM ods.ods_crm_department_group WHERE parentid = 0) dept2 -- 0309 ~0313 销售使用最新的部门
                        ON t2.department = dept2.id
              LEFT JOIN (SELECT * FROM ods.ods_crm_department_group WHERE parentid != 0) grp2
                        ON t2.user_group = grp2.id

;


DROP TABLE IF EXISTS base_user;
CREATE TEMPORARY TABLE base_user as
SELECT t1.id
                        , t1.member_id
                        , t1.platform
                        , t1.h5_id
                        , t1.cat
                        , t1.p_source
                        , t1.report_link
                        , t1.p_source_1
                        , t1.live
                        , t1.a_id
                        , t1.form_type
                        , t1.openid
                        , t1.unionid
                        , t1.phone
                        , t1.sales_id
                        , t1.sales_real_name
                        , COALESCE(tmp.actual_version_unified, ai.ai_type, '无') AS sales_sop_type
                        , CASE
        -- 3月20日更改：更改AI的打标逻辑，0309期-0321期按照陈雪萍提供的维表进行打标。3月21日之后按照系统配置进行打标。
                              WHEN SUBSTR(t1.goods_name, 2, 6) BETWEEN '250301' AND '250321' AND tmp.is_ai = 1
                                  THEN CONCAT('AI-', t1.department)
                              WHEN SUBSTR(t1.goods_name, 2, 6) BETWEEN '250301' AND '250321' AND
                                   (tmp.is_ai = 0 OR tmp.goods_name IS NULL)
                                  THEN CONCAT('人工-', t1.department)
                              WHEN SUBSTR(t1.goods_name, 2, 6) > '250321' AND (ai.goods_name IS NOT NULL) AND
                                   (ai.sales_id IS NOT NULL) THEN CONCAT('AI-', t1.department)

                              WHEN SUBSTR(t1.goods_name, 2, 6) > '250321'
                                  AND ai.sales_id IS NULL
                                  AND t1.sales_id > 0 THEN CONCAT('人工-', t1.department)
                              ELSE t1.department
        END                                                                      AS department
                        , t1.user_group
                        , t1.created_at
                        , t1.pay_time
                        , t1.pay_price
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
                        , t1.goods_name
                        , t1.mch_id
                        , t1.xe_id
                        , t1.corp_userid
                   FROM base_user_row_data t1
                            LEFT JOIN (SELECT * FROM dws.dws_ai_sales_dt WHERE dt = '${datebuf}') ai -- 20250311增加AI销售清单
                                      ON t1.sales_id = ai.sales_id
                                          AND t1.goods_name = ai.goods_name
                            LEFT JOIN dim.dim_sales_ai_250321_temp tmp -- 20250320增加陈雪萍AI标签
                                      ON t1.goods_name = tmp.goods_name
                                          AND t1.sales_real_name = tmp.sales_real_name
;

-- 20240912 汤文奇 将加微的处理逻辑前置，后面头条直播例子解析会用到加微时间
DROP  TABLE IF EXISTS base_user_add_wx;
CREATE TEMPORARY TABLE base_user_add_wx AS
  -- 添加加微时间
        SELECT t1.id
             , t1.member_id
             , t1.platform
             , t1.h5_id
             , t1.cat
             , t1.p_source
             , t1.report_link
             , t1.p_source_1
             , t1.live
             , t1.a_id
             , t1.form_type
             , t1.openid
             , t1.unionid
             , t1.phone
             , t1.sales_id
             , t1.sales_real_name                                              AS sales_name
             , t1.sales_sop_type
             , t1.department
             , t1.user_group
             , t1.created_at
             , t1.pay_time
             , t1.pay_price
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
             , t1.xe_id
             , t1.corp_userid
             , CASE
                   WHEN TO_DATE(t1.created_at) >= '2024-08-05' THEN t1.wx_rel_status -- 20240912 汤文奇 修改两套逻辑分割时间为0805
                   ELSE IF(t3.ex_unionid IS NOT NULL, t3.wx_rel_status, 1) END AS wx_rel_status
             , COALESCE(t11.created_at, t2.created_at, t3.created_at, '')      AS wx_add_time
        FROM base_user t1
                 LEFT JOIN chage_id t11
                           ON t1.unionid = t11.ex_unionid AND t1.special_id = t11.special_id
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
                 LEFT JOIN (SELECT ex_unionid
                                 , MAX(wx_relation_status) AS wx_rel_status
                                 , MIN(created_at)         AS created_at -- 取用户微信的首次加微时间
                            FROM ods.ods_place_contact
                            WHERE ex_unionid IS NOT NULL
                              AND ex_unionid <> ''
                            GROUP BY ex_unionid) t3
                           ON t1.unionid = t3.ex_unionid
;

DROP TABLE IF EXISTS base_user_add_adid;
CREATE TEMPORARY TABLE base_user_add_adid  AS
    -- 补充百度、抖音的计划id，腾讯的计划id和素材id
        SELECT id
             , member_id
             , platform
             , h5_id
             , cat
             , p_source
             , report_link
             , p_source_1
             , live
             , a_id
             , form_type
             , openid
             , unionid
             , phone
             , sales_id
             , sales_name
             , sales_sop_type
             , department
             , user_group
             , created_at
             , pay_time
             , pay_price
             , transaction_id
             , out_trade_no
             , refund_time
             , refund_price
             , updated_at
             , trade_state
             , refunds
             , member_status
             , ip
             --,wx_rel_status
             , special_id
             , goods_name
             , mch_id
             , xe_id
             , corp_userid
             , wx_rel_status
             , wx_add_time
             , '' AS ad_id
             , '' AS tx_sucai_id
             , '' AS baidu_sucai_id
             , '' AS dy_sucai_id
        FROM base_user_add_wx
        WHERE platform NOT IN
              ('baidu', 'wx', 'wxmini', 'wxpcad', 'wxyoulianghui', 'wxyoulianhui',
               'douyin') --2024-08-27 汤文奇加上'wxyoulianghui'
        UNION ALL
        SELECT id
             , member_id
             , platform
             , h5_id
             , cat
             , p_source
             , report_link
             , p_source_1
             , live
             , a_id
             , form_type
             , openid
             , unionid
             , phone
             , sales_id
             , sales_name
             , sales_sop_type
             , department
             , user_group
             , created_at
             , pay_time
             , pay_price
             , transaction_id
             , out_trade_no
             , refund_time
             , refund_price
             , updated_at
             , trade_state
             , refunds
             , member_status
             , ip
             --,wx_rel_status
             , special_id
             , goods_name
             , mch_id
             , xe_id
             , corp_userid
             , wx_rel_status
             , wx_add_time
             , CASE
                   WHEN t1.platform IN ('wx', 'wxmini', 'wxpcad', 'wxyoulianghui', 'wxyoulianhui')
                       THEN COALESCE(t3.tx_adid, t5.tx_adid, t6.tx_adid)
                   WHEN t1.platform = 'douyin'
                       THEN t4.adid
                   WHEN t1.platform = 'baidu'
                       THEN t2.adid
            END                                                         ad_id
             , COALESCE(t3.tx_sucai_id, t5.tx_sucai_id, t6.tx_sucai_id) tx_sucai_id
             , t2.baidu_sucai_id
             , t4.dy_sucai_id
        FROM (SELECT *
                   , SPLIT(SPLIT(report_link, 'bd_vid=')[1], '&')[0]           bd_vid
                   , REGEXP_EXTRACT(report_link, 'qz_gdt=([a-zA-Z0-9]+)', 1)   qz_gdt  -- 20240816需要调整为qz_gdt|gdt_vid,先和click表中的clickid关联,然后再和click表中report_link中的request_id|impression_id关联
                   , REGEXP_EXTRACT(report_link, 'clickid=([a-zA-Z0-9.]+)', 1) clickid
                   , REGEXP_EXTRACT(report_link, 'gdt_vid=([a-zA-Z0-9]+)', 1)  gdt_vid -- 20240929 腾讯渠道需要gdt_vid去和click表的report_link关联
              FROM base_user_add_wx
              WHERE platform IN ('baidu', 'wx', 'wxmini', 'wxpcad', 'wxyoulianghui', 'wxyoulianhui', 'douyin')) t1
                 LEFT JOIN
             (SELECT *
              FROM (SELECT click_id
                         , GET_JSON_OBJECT(report_link_json, '$.pid')                         adid
                         , GET_JSON_OBJECT(report_link_json, '$.aid')                         baidu_sucai_id
                         , ROW_NUMBER() OVER (PARTITION BY click_id ORDER BY created_at DESC) rank
                    FROM ods.ods_marketing_ad_click --一个click_id仅对应一条记录，防止存在脏数据取最新记录
                    WHERE click_id IS NOT NULL
                      AND GET_JSON_OBJECT(report_link_json, '$.pid') IS NOT NULL
                      AND platform = 'baidu') temp
              WHERE rank = 1) t2
             ON t1.bd_vid = t2.click_id
                 LEFT JOIN
             (SELECT *
              FROM (SELECT click_id
                         , REGEXP_REPLACE(GET_JSON_OBJECT(report_link_json, '$.element_info'), '[^0-9,]',
                                          '')                                                 tx_sucai_id
                         , GET_JSON_OBJECT(report_link_json, '$.adgroup_id')                  tx_adid
                         , ROW_NUMBER() OVER (PARTITION BY click_id ORDER BY created_at DESC) rank
                    FROM ods.ods_marketing_ad_click --一个click_id仅对应一条记录，防止存在脏数据取最新记录
                    WHERE click_id IS NOT NULL
                      AND GET_JSON_OBJECT(report_link_json, '$.adgroup_id') IS NOT NULL
                      AND platform = 'tencent') temp
              WHERE rank = 1) t5
             ON t1.qz_gdt = t5.click_id
                 LEFT JOIN
             (SELECT *
              FROM (SELECT REGEXP_REPLACE(GET_JSON_OBJECT(report_link_json, '$.element_info'), '[^0-9,]',
                                          '')                                                 tx_sucai_id
                         , GET_JSON_OBJECT(report_link_json, '$.impression_id')               impression_id
                         , GET_JSON_OBJECT(report_link_json, '$.adgroup_id')                  tx_adid
                         , ROW_NUMBER() OVER (PARTITION BY click_id ORDER BY created_at DESC) rank
                    FROM ods.ods_marketing_ad_click --一个click_id仅对应一条记录，防止存在脏数据取最新记录
                    WHERE click_id IS NOT NULL
                      AND GET_JSON_OBJECT(report_link_json, '$.adgroup_id') IS NOT NULL
                      AND platform = 'tencent') temp
              WHERE rank = 1) t3
             ON t3.impression_id = t1.gdt_vid
                 LEFT JOIN
             (SELECT *
              FROM (SELECT REGEXP_REPLACE(GET_JSON_OBJECT(report_link_json, '$.element_info'), '[^0-9,]',
                                          '')                                                 tx_sucai_id
                         , GET_JSON_OBJECT(report_link_json, '$.request_id')                  request_id
                         , GET_JSON_OBJECT(report_link_json, '$.adgroup_id')                  tx_adid
                         , ROW_NUMBER() OVER (PARTITION BY click_id ORDER BY created_at DESC) rank
                    FROM ods.ods_marketing_ad_click --一个click_id仅对应一条记录，防止存在脏数据取最新记录
                    WHERE click_id IS NOT NULL
                      AND GET_JSON_OBJECT(report_link_json, '$.adgroup_id') IS NOT NULL
                      AND platform = 'tencent') temp
              WHERE rank = 1) t6
             ON t6.request_id = t1.gdt_vid
                 LEFT JOIN
             (SELECT *
              FROM (SELECT click_id
                         , GET_JSON_OBJECT(report_link_json, '$.promotion_id')                adid
                         , GET_JSON_OBJECT(report_link_json, '$.creative_id')                 creative_id
                         , GET_JSON_OBJECT(report_link_json, '$.video_material_id')           dy_sucai_id
                         , ROW_NUMBER() OVER (PARTITION BY click_id ORDER BY created_at DESC) rank
                    FROM ods.ods_marketing_ad_click --一个click_id仅对应一条记录，防止存在脏数据取最新记录
                    WHERE click_id IS NOT NULL
                      AND GET_JSON_OBJECT(report_link_json, '$.promotion_id') IS NOT NULL
                      AND platform = 'douyin') temp
              WHERE rank = 1) t4
             ON t1.clickid = t4.click_id
;
-- 20240902 全部使用线上维表
-- , mid_v1 AS -- 关联h5手工维表
-- (
-- 	SELECT  t1.member_id
-- 	       ,t1.platform
-- 	       ,t1.h5_id
-- 	       ,t1.p_source
-- 	       ,t1.report_link
-- 	       ,t1.openid
-- 	       ,t1.unionid
-- 	       ,t1.phone
-- 	       ,t1.sales_id
-- 	       ,t1.sales_name
-- 	       ,t1.department
-- 	       ,t1.user_group
-- 	       ,t1.created_at
-- 	       ,t1.pay_time
-- 	       ,t1.pay_price
-- 	       ,t1.transaction_id
-- 	       ,t1.out_trade_no
-- 	       ,t1.refund_time
-- 	       ,t1.refund_price
-- 	       ,t1.updated_at
-- 	       ,t1.trade_state
-- 	       ,t1.refunds
-- 	       ,t1.member_status
-- 	       ,t1.ip
-- 	       ,t1.wx_rel_status
-- 	       ,t1.special_id
-- 	       ,t1.goods_name
-- 	       ,t1.mch_id
-- 		   ,t1.xe_id
-- 		   ,t1.corp_userid
-- 	       ,t2.cat
-- 	       ,t2.d_pos      AS pos
-- 	       ,t2.link_type  AS link_type_v1
-- 	       ,t2.d_linktype AS link_type_v2
--            ,t2.platform_type as platform_name
-- 		   ,t2.price
-- 		   ,t2.mobile
-- 		   ,t1.ad_id
-- 		   ,t1.tx_sucai_id
-- 	FROM
-- 	(
-- 		SELECT  *
-- 		       ,concat(live,'-',a_id,'-',form_type) AS dbt_live_aid_formtype
-- 		FROM base_user_add_adid
-- 	) t1
-- 	LEFT JOIN
-- 	(
-- 		select *
-- 		from (
-- 				SELECT  platform -- 渠道
-- 				       ,h5_id
-- 				       ,dbt_live_aid_formtype -- 组合参数
-- 				       ,platform_type -- 渠道中文
-- 				       ,d_pos -- 版位信息
-- 				       ,d_cat as cat -- 品类信息
-- 				       ,price -- 价格
-- 					   ,case when collect_phone_type in ('收集手机号','授权手机号') then '有手机号'
-- 					   		 when collect_phone_type in ('不收集') then '无手机号' else '无手机号' end as mobile -- 有无手机号
-- 					   ,link_type -- 链路类型-旧
-- 				       ,d_linktype -- 链路类型-新
-- 				       ,p_source as p_source_1
-- 					   ,ROW_NUMBER() over(partition by platform,h5_id,p_source,dbt_live_aid_formtype order by updated_at desc ) as rnum
-- 				FROM dim.tdlive_place_h5_history
-- 				WHERE updated_at = '2024-05-15'
-- 				) t0
-- 			where rnum = 1
-- 	) t2
-- 	ON t1.platform = t2.platform
-- 	AND t1.h5_id = t2.h5_id
-- 	AND t1.p_source_1 = t2.p_source_1
-- 	AND t1.dbt_live_aid_formtype = t2.dbt_live_aid_formtype
-- 	WHERE t1.h5_id <= 295
-- )
-- 关联h5线上维表
DROP TABLE IF EXISTS  mid_v2;
CREATE TEMPORARY TABLE mid_v2 AS
    SELECT t1.id
          , t1.member_id
          , t1.platform
          , t1.h5_id
          , t1.p_source
          , t1.report_link
          , t1.openid
          , t1.unionid
          , t1.phone
          , t1.sales_id
          , t1.sales_name
          , t1.sales_sop_type
          , t1.department
          , t1.user_group
          , t1.created_at
          , t1.pay_time
          , t1.pay_price
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
          , t1.xe_id
          , t1.corp_userid
          , t1.wx_rel_status
          , t1.wx_add_time
          , NVL(t1.cat, t2.cat) as cat
          , t2.platform_section AS pos
          , t2.link_type        AS link_type_v1
          , t2.d_link_type      AS link_type_v2
          , t2.platform_name
          , t2.price
          , t2.mobile
          , t1.ad_id
          , t1.tx_sucai_id
          , t1.baidu_sucai_id
          , t1.dy_sucai_id
          , t2.ad_department
     FROM base_user_add_adid t1
              LEFT JOIN
          (SELECT h5_id
                , category AS cat  --品类信息
                , ad_department
                , platform         --渠道信息
                , platform_section --版位信息
                , link_type        --链路类型-旧
                , d_link_type      --链路类型-新
                , platform_name
                , mobile
                , price
           FROM (SELECT h5_id
                      , category
                      , ad_department
                      , platform
                      , NVL(IF(pltf.long_name = '腾讯pcad', '腾讯', pltf.long_name), platform)           platform_name    -- 20240808 添加腾讯公众号关注渠道，与肖傲沟通和腾讯区分开来
                      , CASE
                            WHEN platform_section = '腾讯视频号付费流' THEN '腾讯视频号直播付费流'
                            WHEN platform_section = '腾讯视频号免费流' THEN '腾讯视频号直播免费流'
                            WHEN platform_section = '腾讯视频号直播' THEN '腾讯视频号直播免费流'
                            WHEN platform_section = '腾讯视频号' AND
                                 platform IN ('wx', 'wxpcad', 'wxyoulianghui', 'wxyoulianhui')
                                THEN '腾讯视频号信息流'
                   -- 24.12.30 数据标准化暂时添加到此
                            ELSE IF(platform_section IS NULL OR platform_section IN ('其它', ''), '其他',
                                    platform_section) END                                                platform_section -- 20240808 腾讯公众号关注渠道的腾讯视频号不做映射处理，与肖傲确认，和腾讯渠道下面的腾讯视频号是两码事
                      , CONCAT(CAST(CASE
                                        WHEN PMOD(price, 100) = 0 THEN CAST(ROUND(price / 100, 0) AS int)
                                        ELSE CAST(ROUND(price / 100, 2) AS string) END AS string), '元',
                               IF(mobile_show IN (1, 2), '有手机号', '无手机号'))                        link_type
                      , NVL(lt.long_name, '无')                                                          d_link_type
                      , IF(mobile_show IN (1, 2), '有手机号', '无手机号')                                mobile
                      , CONCAT(CAST(CASE
                                        WHEN PMOD(price, 100) = 0 THEN CAST(ROUND(price / 100, 0) AS int)
                                        ELSE CAST(ROUND(price / 100, 2) AS string) END AS string), '元') price
                      , ROW_NUMBER() OVER (PARTITION BY h5_id ORDER BY updated_at DESC)                  row_1            --取更新后的最新一条
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
           WHERE row_1 = 1) t2
          ON t1.h5_id = t2.h5_id
;
   -- 24.10.16新增几个不解析的账号
   -- 合并并补充账户id
DROP TABLE IF EXISTS mid_v3;
CREATE TEMPORARY TABLE  mid_v3 AS
    SELECT t0.*
          , t6.country_code
     FROM (SELECT t1.id
                , t1.member_id
                , t1.platform
                , t1.h5_id
                , t1.p_source
                , t1.report_link
                , t1.openid
                , t1.unionid
                , t1.phone
                , t1.sales_id
                , t1.sales_name
                , t1.sales_sop_type
                , t1.department
                , t1.user_group
                , t1.created_at
                , t1.pay_time
                , t1.pay_price
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
                , t1.xe_id
                , t1.corp_userid
                , t1.wx_rel_status
                , t1.wx_add_time
                , t1.cat
                , t1.pos
                , t1.link_type_v1
                , t1.link_type_v2
                , t1.platform_name
                , t1.price
                , t1.mobile
                , t1.ad_department
                , CASE
                      WHEN t1.platform = 'douyin' AND t1.report_link LIKE '%promotionid%'
                          THEN
                          CASE
                              WHEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'promotionid') + 12, 19) RLIKE '^\\d+$'
                                  AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'promotionid') + 12, 19)) =
                                      19 --19位数字
                                  THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'promotionid') + 12, 19)
                              ELSE
                                  CASE
                                      WHEN t1.report_link LIKE '%adid%'
                                          AND
                                           SUBSTR(t1.report_link, INSTR(t1.report_link, 'adid') + 5, 16) RLIKE '^\\d+$'
                                          AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'adid') + 5, 16)) =
                                              16 --16位数字
                                          THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'adid') + 5, 16)
                                      END
                              END
             --解析抖音非直播渠道下的投放计划ID情况A

                      WHEN t1.platform = 'douyin'
                          AND t1.report_link LIKE '%adid%'
                          AND SUBSTR(t1.report_link, INSTR(t1.report_link, 'adid') + 5, 16) RLIKE '^\\d+$'--数字
                          AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'adid') + 5, 16)) = 16
                          THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'adid') + 5, 16)
             --解析抖音非直播渠道下的投放计划ID情况B

                      WHEN t1.platform = 'douyin' --20240702新增抖音直播
                          AND t1.ad_id RLIKE '^\\d+$'--数字
                          AND LENGTH(t1.ad_id) = 19
                          THEN t1.ad_id
             --抖音直播渠道下从“click”中关联得到的投放计划id
                      WHEN t1.platform = 'douyin' AND t1.ad_id IS NULL AND t3.report_link LIKE '%promotionid%'
                          THEN
                          CASE
                              WHEN SUBSTR(t3.report_link, INSTR(t3.report_link, 'promotionid') + 12, 19) RLIKE '^\\d+$'
                                  AND LENGTH(SUBSTR(t3.report_link, INSTR(t3.report_link, 'promotionid') + 12, 19)) =
                                      19 --19位数字
                                  THEN SUBSTR(t3.report_link, INSTR(t3.report_link, 'promotionid') + 12, 19)
                              END
             --20240912 抖音直播支付,换直播间加微，place_order回传链接是首次的，但头条直播加微才回传，取第二次加微的回传链接解析
                      WHEN t1.platform = 'douyin' AND t1.ad_id IS NULL AND t3.report_link NOT LIKE '%promotionid%'
                          THEN t4.adid
             --20240912 抖音直播支付,信息流加微，取信息流加微的回传链接解析
                      WHEN t1.platform = 'wx'
                          AND SUBSTR(t1.report_link, INSTR(t1.report_link, 'wx_aid') + 7, 11) RLIKE '^\\d+$'--数字
                          AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'wx_aid') + 7, 11)) = 11
                          THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'wx_aid') + 7, 11)
             --解析腾讯渠道下投放计划ID情况A

                      WHEN t1.platform = 'wx' --20230908兼容10位
                          AND SUBSTR(t1.report_link, INSTR(t1.report_link, 'wx_aid') + 7, 10) RLIKE '^\\d+$'--数字
                          AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'wx_aid') + 7, 10)) = 10
                          THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'wx_aid') + 7, 10)
             --解析腾讯渠道下投放计划ID情况B

                      WHEN t1.platform IN ('wx', 'wxmini', 'wxpcad')
                          AND SUBSTR(t1.report_link, INSTR(t1.report_link, 'weixinadinfo=') + 13, 11) RLIKE '^\\d+$'--数字
                          AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'weixinadinfo=') + 13, 11)) = 11
                          THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'weixinadinfo=') + 13, 11)
             --20240822解析腾讯渠道下投放计划ID情况D
             --解析腾讯渠道下投放计划ID情况E

                      WHEN t1.platform = 'wxmini' --20230908兼容10位
                          AND SUBSTR(t1.report_link, INSTR(t1.report_link, 'weixinadinfo') + 13, 10) RLIKE '^\\d+$'--数字
                          AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'weixinadinfo') + 13, 10)) = 10
                          THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'weixinadinfo') + 13, 10)
             --解析腾讯渠道下投放计划ID情况F

                      WHEN t1.platform = 'kuaishou' --20231221新增快手
                          AND SUBSTR(t1.report_link, INSTR(t1.report_link, 'aid') + 4, 10) RLIKE '^\\d+$'--数字
                          AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'aid') + 4, 10)) = 10
                          THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'aid') + 4, 10)
             --解析快手渠道下投放计划ID情况

                      WHEN t1.platform = 'oppo' --20240315新增oppo
                          AND SUBSTR(t1.report_link, INSTR(t1.report_link, 'mastplanid') + 11, 9) RLIKE '^\\d+$'--数字
                          AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'mastplanid') + 11, 9)) = 9
                          THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'mastplanid') + 11, 9)
             --解析oppo渠道下投放计划ID情况
--  25.03.23 腾讯渠道解析不出来的，统一从提交表单的点击取数
                      WHEN t1.platform IN ('wxpcad', 'wxyoulianghui', 'wx', 'wxyoulianhui') THEN t1.ad_id
             --当渠道为“腾讯pcad”时直接返回已经从“click”中关联得到的投放计划id

                      WHEN t1.platform = 'baidu' --20240108新增百度
                          THEN CASE
                                   WHEN t1.ad_id IS NULL
                                       AND
                                        LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'baidu_adid=') + 11, 9)) = 9
                                       AND SUBSTR(t1.report_link, INSTR(t1.report_link, 'baidu_adid=') + 11, 9) RLIKE
                                           '^\\d+$'--数字
                                       THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'baidu_adid=') + 11,
                                                   9) --20240821新增百度规则:使用宏参数解析
                                   WHEN t1.ad_id RLIKE '^\\d+$'
                                       AND LENGTH(t1.ad_id) = 9 --数字
                                       THEN t1.ad_id END

             --当渠道为“百度”时直接返回已经从“click”中关联得到的投放计划id,未解析clickid的尝试

                      WHEN t1.platform = 'wxmp'
                          AND SUBSTR(t1.report_link, INSTR(t1.report_link, 'adgroup_id') + 11, 11) RLIKE '^\\d+$'--数字
                          AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'adgroup_id') + 11, 11)) = 11
                          THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'adgroup_id') + 11, 11)
             --20240812 当渠道为“腾讯公众号关注时”从report_link中抓取投放计划id
                      WHEN t1.platform = 'facebook'
                          AND SUBSTR(t1.report_link, INSTR(t1.report_link, 'campaign_id=') + 12, 18) RLIKE '^\\d+$'--数字
                          AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'campaign_id=') + 12, 18)) = 18
                          THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'campaign_id=') + 12, 18)
             --20250312 当渠道为脸书从report_link中抓取投放计划id
                      WHEN t1.platform = 'google'
                          AND SUBSTR(t1.report_link, INSTR(t1.report_link, 'campaignid=') + 11, 11) RLIKE '^\\d+$'--数字
                          AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'campaignid=') + 11, 11)) = 11
                          THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'campaignid=') + 11, 11)
             --20250328 当渠道为谷歌从report_link中抓取投放计划id
                      ELSE 'other'
             END                                                                                   ad_id


                ---------------------解析素材id-----------------------------------
                --解析拿到投放计划ID
                , CASE
                      WHEN t1.platform = 'douyin'
                          AND t1.report_link LIKE '%promotionid%'--20231225增加头条2.0素材id
                          THEN
                          CASE
                              WHEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'material_id') + 12, 19) RLIKE '^\\d+$'--数字
                                  AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'material_id') + 12, 19)) =
                                      19 --19位
                                  THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'material_id') + 12, 19)
                              END
                      WHEN t1.platform = 'douyin' --20240702新增抖音直播
                          AND t1.dy_sucai_id RLIKE '^\\d+$'--数字
                          AND LENGTH(t1.dy_sucai_id) = 19
                          THEN t1.dy_sucai_id
             --抖音直播渠道下从“click”中关联得到的投放素材id
                      WHEN t1.platform = 'douyin' AND t1.dy_sucai_id IS NULL AND t3.report_link LIKE '%material_id%'
                          THEN
                          CASE
                              WHEN SUBSTR(t3.report_link, INSTR(t3.report_link, 'material_id') + 12, 19) RLIKE '^\\d+$'
                                  AND LENGTH(SUBSTR(t3.report_link, INSTR(t3.report_link, 'material_id') + 12, 19)) =
                                      19 --19位数字
                                  THEN SUBSTR(t3.report_link, INSTR(t3.report_link, 'material_id') + 12, 19)
                              END
             --抖音直播支付,信息流加微，取信息流加微的回传链接解析
                      WHEN t1.platform = 'douyin' AND t1.dy_sucai_id IS NULL AND t3.report_link NOT LIKE '%material_id%'
                          THEN t4.dy_sucai_id
             --抖音直播支付,换直播间加微，place_order回传链接是首次的，但头条直播加微才回传，取第二次加微的回传链接解析
                      WHEN t1.platform IN ('wx', 'wxmini', 'wxpcad', 'manual', 'wxyoulianghui', 'wxyoulianhui')
                          AND (t1.report_link LIKE '%gdt_vid%' OR t1.report_link LIKE '%qz_gdt%')
                          AND t1.tx_sucai_id IS NOT NULL
                          THEN t1.tx_sucai_id
             -- 20240821新增百度素材ID,用宏参数解析(百度搜索)
                      WHEN t1.platform = 'baidu' AND t1.pos = '百度搜索'
                          AND SUBSTR(t1.report_link, INSTR(t1.report_link, 'baidu_unitid') + 13, 11) RLIKE '^\\d+$'--数字
                          AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'baidu_unitid') + 13, 11)) = 11
                          THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'baidu_unitid') + 13, 11)
             -- 20240926 汤文奇 百度搜索素材id采用单元id，单元id有10位也有11位的
                      WHEN t1.platform = 'baidu' AND t1.pos = '百度搜索'
                          AND SUBSTR(t1.report_link, INSTR(t1.report_link, 'baidu_unitid') + 13, 10) RLIKE '^\\d+$'--数字
                          AND LENGTH(SUBSTR(t1.report_link, INSTR(t1.report_link, 'baidu_unitid') + 13, 10)) = 10
                          THEN SUBSTR(t1.report_link, INSTR(t1.report_link, 'baidu_unitid') + 13, 10)
                      WHEN t1.platform = 'baidu' AND t1.pos IN ('百度信息流', '百度信息流北京')
                          THEN t1.baidu_sucai_id -- 20240926 汤文奇 百度信息流用click表里的aid(创意id)
             END                                                                                   sucai_id
                , CASE WHEN t1.platform = 'facebook'
                       THEN REGEXP_EXTRACT(t1.report_link, '&zh_id=(\\d+)', 1)
             END                                                                                   cost_id_fb
                , REGEXP_EXTRACT(t1.report_link, '&loc_physical_ms=(\\d+)', 1)                     loc_physical_ms
                -- 20250312 汤文奇 解析facebook账户 如果计划解析不出来，直接从取zh_id
                , ROW_NUMBER() OVER (PARTITION BY t1.id,t1.wx_add_time ORDER BY t3.created_at ASC) rn -- 20240912 汤文奇 防止report表有重复数据去重
           -- from (
           -- 	SELECT  *
           -- 	FROM mid_v1
           -- 	UNION ALL
           -- 	SELECT  *
           -- 	FROM mid_v2
           -- 	) t1
           -- 20240902 全部使用线上维表
           FROM mid_v2 t1
/*                    -- 20250218 新增从report_log表中优先获取h5_id
                    LEFT JOIN (SELECT *
                               FROM ods.ods_place_report_log
                               WHERE place_order_id != 0
                                 AND event_type = 'EVENT_ADD_WX') t33
                              ON t1.id = t33.place_order_id*/
                    LEFT JOIN ods.ods_place_report_log t3 -- 20240912 头条直播换直播间或者换渠道加微，需要从report表取加微回传链接，因为头条直播转化目标为加微
                              ON t1.member_id = t3.member_id
                                  AND t3.created_at >= t1.wx_add_time
                                  AND t3.created_at >= t1.created_at
                                  AND (UNIX_TIMESTAMP(t3.created_at) - UNIX_TIMESTAMP(t1.wx_add_time)) <= 60
                                  AND t3.event_type = 'EVENT_ADD_WX'
                    LEFT JOIN
                (SELECT *
                 FROM (SELECT click_id
                            , GET_JSON_OBJECT(report_link_json, '$.promotion_id')                adid
                            , GET_JSON_OBJECT(report_link_json, '$.creative_id')                 creative_id
                            , GET_JSON_OBJECT(report_link_json, '$.video_material_id')           dy_sucai_id
                            , ROW_NUMBER() OVER (PARTITION BY click_id ORDER BY created_at DESC) rank
                       FROM ods.ods_marketing_ad_click --一个click_id仅对应一条记录，防止存在脏数据取最新记录
                       WHERE click_id IS NOT NULL
                         AND GET_JSON_OBJECT(report_link_json, '$.promotion_id') IS NOT NULL
                         AND platform = 'douyin') temp
                 WHERE rank = 1) t4
                ON t4.click_id = REGEXP_EXTRACT(t3.report_link, 'clickid=([a-zA-Z0-9.]+)', 1)) t0
--               LEFT JOIN
--           (SELECT advertiser_id AS cost_id
--                 , promotion_id  AS ad_id
--            FROM ods.ods_marketing_report --当前通过历史所有的api消耗数据聚合得出, 截止240722，聚合后有约20万条数据, 后续需要优化
--            WHERE cdate <= '${datebuf}'
--            GROUP BY advertiser_id
--                   , promotion_id) t2
--           ON t0.ad_id = t2.ad_id
              LEFT JOIN
          (SELECT criteria_id
                , country_code
           FROM ods.ods_geotargets
          ) t6
          ON t0.loc_physical_ms = t6.criteria_id
     WHERE t0.rn = 1
;

-- 关联xiaoe_member和问卷表获取问卷信息
DROP TABLE IF EXISTS mid_v4;
CREATE TEMPORARY TABLE  mid_v4 AS

    SELECT t1.*
          , REGEXP_REPLACE(t2.contact_ex_nickname, '\n|\t|\r', '')           contact_ex_nickname
          , CASE
                WHEN t2.is_get_ticket = 2 AND
                     DATEDIFF('${datebuf}',
                              FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTR(t1.goods_name, 2, 6), 'yyMMdd'), 'yyyy-MM-dd')) >= 3
                    THEN '2是'
                ELSE '1否' END                                               is_get_ticket
          , IF(t3.xe_id IS NOT NULL, 1, 0)                                   ifcollect
          , NVL(IF(TRIM(t3.sex) = '', NULL, t3.sex), '未填写')               sex
          , NVL(t3.age, 0)                                                   age
          , NVL(IF(TRIM(t3.age_level) = '', NULL, t3.age_level), '未填写')   age_level
          , NVL(IF(TRIM(t3.address) = '', NULL, t3.address), '未填写')       address
          , NVL(IF(TRIM(t3.city_level) = '', NULL, t3.city_level), '未填写') city_level
          , NVL(IF(TRIM(t3.work) = '', NULL, t3.work), '未填写')             work
          , NVL(t3.form_name, '未填写')                                      form_name
          , NVL(t3.collect_time, '未填写')                                   collect_time
          , NVL(t3.extra_original, '未填写')                                 extra
     FROM mid_v3 t1
              LEFT JOIN ods.ods_xiaoe_member t2
                        ON t1.member_id = t2.id
              LEFT JOIN (SELECT * FROM dw.dws_sale_questionnaire_day WHERE dt = '${datebuf}') t3
                        ON t2.xe_id = t3.xe_id
                            AND t1.cat = t3.form_cat
;

/* ,mid_v5 as
( -- 添加加微时间
	select   t1.member_id
       		,t1.cat
       		,t1.platform
       		,t1.platform_name
       		,t1.h5_id
       		,t1.p_source
       		,t1.pos
	   		,t1.price
	   		,t1.mobile
       		,t1.link_type_v1
       		,t1.link_type_v2
       		,t1.cost_id
       		,t1.ad_id
       		,t1.sucai_id
       		,t1.report_link
       		,t1.openid
       		,t1.unionid
       		,t1.phone
       		,t1.sales_id
       		,t1.sales_name
       		,t1.department
       		,t1.user_group
       		,t1.created_at
       		,t1.pay_time
       		,t1.pay_price
       		,t1.transaction_id
       		,t1.out_trade_no
       		,t1.refund_time
       		,t1.refund_price
       		,t1.updated_at
       		,t1.trade_state
       		,t1.refunds
       		,t1.member_status
       		,t1.ip
       		,case when to_date(t1.created_at) >= '2024-07-13' then t1.wx_rel_status
				else if(t3.ex_unionid is not null,t3.wx_rel_status,1) end as wx_rel_status
       		,t1.special_id
       		,t1.goods_name
       		,t1.mch_id
       		,t1.contact_ex_nickname
       		,t1.is_get_ticket
       		,t1.xe_id
       		,t1.ifcollect
       		,t1.sex
       		,t1.age
       		,t1.age_level
       		,t1.address
       		,t1.city_level
       		,t1.work
       		,t1.form_name
       		,t1.collect_time
       		,t1.extra
			,coalesce(t2.created_at,t3.created_at,'') as wx_add_time
	from mid_v4 t1
	left join (
        select  ex_unionid
                ,userid
                ,min(created_at) as created_at -- 取用户微信+销售id的首次加微时间
        from ods.ods_place_contact
        where ex_unionid is not null
        and ex_unionid <> ''
        group by ex_unionid
                ,userid
        ) t2
	on t1.unionid = t2.ex_unionid
	and t1.corp_userid = t2.userid
	left join (
        select  ex_unionid
				,max(wx_relation_status) as wx_rel_status
                ,min(created_at) as created_at -- 取用户微信+销售id的首次加微时间
        from ods.ods_place_contact
        where ex_unionid is not null
        and ex_unionid <> ''
        group by ex_unionid
        ) t3
	on t1.unionid = t3.ex_unionid
) */

INSERT
OVERWRITE
TABLE
dw.dwd_xt_user_mid
SELECT member_id
     , cat
     , ad_department
     , platform
     , IF(platform_name = '' OR platform_name IS NULL, '其他', platform_name) AS platform_name
     , h5_id
     , p_source
     , IF(pos = '' OR pos IS NULL, '其他', pos)                               AS pos
     , price
     , mobile
     , link_type_v1
     , link_type_v2
     , cost_id_fb
     , ad_id
     , sucai_id
     , report_link
     , openid
     , unionid
     , phone
     , sales_id
     , sales_name
     , sales_sop_type
     , department
     , user_group
     , created_at
     , pay_time
     , pay_price
     , transaction_id
     , out_trade_no
     , refund_time
     , refund_price
     , updated_at
     , trade_state
     , refunds
     , member_status
     , ip
     , wx_rel_status
     , wx_add_time
     , special_id
     , goods_name
     , mch_id
     , contact_ex_nickname
     , is_get_ticket
     , xe_id
     , ifcollect
     , sex
     , age
     , age_level
     , address
     , city_level
     , work
     , form_name
     , collect_time
     , extra
     , country_code
FROM mid_v4
--where member_status = 1
--and trade_state in ('SUCCESS','PREPARE')
--and sales_id > 0
-- and to_date(created_at) <= '2024-07-31'
--and to_date(created_at) >= '2024-08-20'
;
