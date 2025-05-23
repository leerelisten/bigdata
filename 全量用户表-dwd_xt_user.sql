-- 建表思路：基于place_order，匹配H5例子、账户-计划-素材、问卷

SET mapred.job.name="dwd_xt_user#${datebuf}";
USE dw;
CREATE EXTERNAL TABLE IF NOT EXISTS dw.dwd_xt_user
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
    COMMENT '培训业务-期次用户明细'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/olap/db/dwd_xt_user/';

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


CREATE TEMPORARY TABLE add_costid AS
SELECT  a.*
      , CASE
            WHEN b.cost_id IN (
                --24.12.03 添加 51842463 51842455  51829940 51829945 51829951 51829957 51829964
                -- 24.11.23添加46500701
                -- 25.3.18 添加57563973 57563977 57563982 57563985 57563991
                                '51240623',
                                '51240618',
                                '49187018',
                                '49187023',
                                '49187033',
                                '49187044',
                                '49187048',
                                '49187055',
                                '49187063',
                                '58028438',
                                '58028429',
                                '58028422',
                                '56517598',
                                '56517590',
                                '58028413',
                                '58028405',
                                '57563973',
                                '57563977',
                                '57563982',
                                '57563985',
                                '57563991',
                                '46500712',
                                '46500718',
                                '46500701',
                                '51842463',
                                '51842455',
                                '49203184',
                                '50841483',
                                '50841487',
                                '51829940',
                                '51829945',
                                '51829951',
                                '51829957',
                                '51829964',
                                '49187070',
                                '49187077',
                                '49187087'
                ) THEN 'ADQ账户不解析'
            WHEN b.cost_id IS NULL THEN a.cost_id --20250312 汤文奇 如果计划id无法解析直接取reportlink里的zh_id
            ELSE b.cost_id END  cost_id2
      , CASE
            WHEN b.cost_id IN (
                --24.12.03 添加 51842463 51842455  51829940 51829945 51829951 51829957 51829964
                -- 24.11.23添加46500701
                -- 24.12.18 张惠晶：一颗豆子主体不能用了，更换成乐岁精彩
                -- 25.3.18 添加57563973 57563977 57563982 57563985 57563991
                                '51240623',
                                '51240618',
                                '49187018',
                                '49187023',
                                '49187033',
                                '49187044',
                                '49187048',
                                '49187055',
                                '49187063',
                                '58028438',
                                '58028429',
                                '58028422',
                                '56517598',
                                '56517590',
                                '58028413',
                                '58028405',
                                '57563973',
                                '57563977',
                                '57563982',
                                '57563985',
                                '57563991',
                                '46500712',
                                '46500718',
                                '46500701',
                                '51842463',
                                '51842455',
                                '49203184',
                                '50841483',
                                '50841487',
                                '51829940',
                                '51829945',
                                '51829951',
                                '51829957',
                                '51829964',
                                '49187070',
                                '49187077',
                                '49187087'
                ) THEN 'ADQ账户不解析'
            ELSE a.ad_id END    ad_id2
      , CASE
            WHEN b.cost_id IN (
                --24.12.03 添加 51842463 51842455  51829940 51829945 51829951 51829957 51829964
                -- 24.11.23添加46500701
                -- 24.12.18 张惠晶：一颗豆子主体不能用了，更换成乐岁精彩
                -- 25.3.18 添加57563973 57563977 57563982 57563985 57563991
                                '51240623',
                                '51240618',
                                '49187018',
                                '49187023',
                                '49187033',
                                '49187044',
                                '49187048',
                                '49187055',
                                '49187063',
                                '58028438',
                                '58028429',
                                '58028422',
                                '56517598',
                                '56517590',
                                '58028413',
                                '58028405',
                                '57563973',
                                '57563977',
                                '57563982',
                                '57563985',
                                '57563991',
                                '46500712',
                                '46500718',
                                '46500701',
                                '51842463',
                                '51842455',
                                '49203184',
                                '50841483',
                                '50841487',
                                '51829940',
                                '51829945',
                                '51829951',
                                '51829957',
                                '51829964',
                                '49187070',
                                '49187077',
                                '49187087'
                ) THEN 'ADQ账户不解析'
            ELSE a.sucai_id END sucai_id2

FROM dw.dwd_xt_user_mid a
LEFT JOIN (SELECT advertiser_id AS cost_id
                , promotion_id  AS ad_id
           FROM ods.ods_marketing_report --当前通过历史所有的api消耗数据聚合得出, 截止240722，聚合后有约20万条数据, 后续需要优化
           WHERE cdate <= '${datebuf}'
           GROUP BY advertiser_id
                  , promotion_id) b
ON a.ad_id = b.ad_id;

INSERT
OVERWRITE
TABLE
dw.dwd_xt_user
PARTITION
(
dt = '${datebuf}'
)
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
     , cost_id2
     , ad_id2
     , sucai_id2
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
     , collect_form_name
     , collect_time
     , extra
     , country_code
FROM add_costid
--where member_status = 1
--and trade_state in ('SUCCESS','PREPARE')
--and sales_id > 0
-- and to_date(created_at) <= '2024-07-31'
--and to_date(created_at) >= '2024-08-20'
;
DFS -touchz /olap/db/dwd_xt_user/dt=${datebuf}/_SUCCESS;