

CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_lexue_log_parsed_detail
(
    ac           STRING COMMENT 'ac 参数',
    mod          STRING COMMENT 'mod 参数',
    random_num   STRING COMMENT '_random_num 参数',
    cli_version  STRING COMMENT 'cli_version 参数',
    creativetype STRING COMMENT 'creativetype 参数',
    wxurllink    STRING COMMENT 'wxurllink 参数',
    wxmpname     STRING COMMENT '_wxmpname 参数',
    link_type    STRING COMMENT 'link_type 参数',
    h5_id        STRING COMMENT 'h5_id 参数',
    platform_id  STRING COMMENT 'platform_id 参数',
    platform     STRING COMMENT 'platform 参数',
    event_id     STRING COMMENT 'event_id 参数',
    promotionid  STRING COMMENT 'promotionid 参数',
    p_price      STRING COMMENT 'p_price 参数',
    buckets      STRING COMMENT 'buckets 参数',
    bucketlist   STRING COMMENT 'bucketlist 参数',
    p_time       STRING COMMENT 'p_time 参数',
    p_status     STRING COMMENT 'p_status 参数',
    p_button     STRING COMMENT 'p_button 参数',
    order_id     STRING COMMENT 'order_id 参数',
    member_id    STRING COMMENT 'member_id 参数',
    mobile_show  STRING COMMENT 'mobile_show 参数',
    clickid      STRING COMMENT 'clickid 参数',
    link         STRING COMMENT 'link 参数',
    env          STRING COMMENT 'env 参数',
    unionid      STRING COMMENT 'unionid 参数',
    p_unionid    STRING COMMENT 'p_unionid 参数',
    p_openid_wx  STRING COMMENT 'p_openid_wx 参数',
    p_link       STRING COMMENT 'p_link 参数',
    special_id   STRING COMMENT 'special_id 参数',
    p_name       STRING COMMENT 'p_name 参数',
    mobile       STRING COMMENT 'p_mobile 参数',
    age          STRING COMMENT 'age 参数',
    gender       STRING COMMENT 'gender 参数',
    sale_id      STRING COMMENT 'sale_id 参数',
    openid       STRING COMMENT 'openid 参数',
    category     STRING COMMENT '_category 参数',
    from_page    STRING COMMENT 'from_page 参数',
    code         STRING COMMENT 'code 参数',
    state        STRING COMMENT 'state 参数',
    projectid    STRING COMMENT 'projectid 参数',
    p_checked    STRING COMMENT 'p_checked 参数',
    request      STRING COMMENT '原始请求字段',
    json_str     STRING COMMENT 'JSON 字符串字段',
    is_ori       STRING COMMENT '1表示原价购买  0表示折扣价购买',
    ori_price    STRING COMMENT '原价字段'
)
    COMMENT '解析后的日志表，存储从 request 字段中提取的参数'
    PARTITIONED BY (dt STRING COMMENT '分区字段，按天分区')
    STORED AS TEXTFILE
    LOCATION '/wuhan_hive/warhouse/dwd.db/dwd_lexue_log_parsed_detail'; -- 外部表存储路径，需根据实际路径修改



DROP TEMPORARY FUNCTION IF EXISTS parse_url2;
CREATE TEMPORARY FUNCTION parse_url2 AS 'com.td.bigdata.udf.UrlToJsonUDF' USING JAR 'hdfs:///user/admin/parse_url-3.0-SNAPSHOT.jar';

CREATE  TEMPORARY TABLE  base_Data as
SELECT
    parse_url2(request) as request
     , json_str
FROM dw.dwd_lexue_log_text
WHERE dt = '${datebuf}'
;

INSERT OVERWRITE TABLE dwd.dwd_lexue_log_parsed_detail PARTITION (dt = '${datebuf}')
SELECT GET_JSON_OBJECT(request, '$.params.ac')            AS ac
     , GET_JSON_OBJECT(request, '$.params.mod')           AS mod
     , GET_JSON_OBJECT(request, '$.params._random_num')   AS random_num
     , GET_JSON_OBJECT(request, '$.params.cli_version')   AS cli_version
     , GET_JSON_OBJECT(request, '$.params.creativetype')  AS creativetype
     , GET_JSON_OBJECT(request, '$.params.wxurllink')     AS wxurllink
     , GET_JSON_OBJECT(request, '$.params._wxmpname')     AS wxmpname
     , GET_JSON_OBJECT(request, '$.params.link_type')     AS link_type
     , GET_JSON_OBJECT(request, '$.params.h5_id')         AS h5_id
     , GET_JSON_OBJECT(request, '$.params.platform_id')   AS platform_id
     , GET_JSON_OBJECT(request, '$.params.platform')      AS platform
     , GET_JSON_OBJECT(request, '$.params.event_id')      AS event_id
     , GET_JSON_OBJECT(request, '$.params.promotionid')   AS promotionid
     , GET_JSON_OBJECT(request, '$.params.p_price')       AS p_price
     , GET_JSON_OBJECT(request, '$.params.buckets')       AS buckets
     , GET_JSON_OBJECT(request, '$.params.bucketlist')    AS bucketlist
     , GET_JSON_OBJECT(request, '$.params.p_time')        AS p_time
     , GET_JSON_OBJECT(request, '$.params.p_status')      AS p_status
     , GET_JSON_OBJECT(request, '$.params.p_button')      AS p_button
     , GET_JSON_OBJECT(request, '$.params.order_id')      AS order_id
     , GET_JSON_OBJECT(request, '$.params.member_id')     AS member_id
     , GET_JSON_OBJECT(request, '$.params.mobile_show')   AS mobile_show
     , GET_JSON_OBJECT(request, '$.params.clickid')       AS clickid
     , GET_JSON_OBJECT(request, '$.params.link')          AS link
     , GET_JSON_OBJECT(request, '$.params.env')           AS env
     , GET_JSON_OBJECT(request, '$.params.unionid')       AS unionid
     , GET_JSON_OBJECT(request, '$.params.p_unionid')     AS p_unionid
     , GET_JSON_OBJECT(request, '$.params.p_openid_wx')   AS p_openid_wx
     , GET_JSON_OBJECT(request, '$.params.p_link')        AS p_link
     , GET_JSON_OBJECT(request, '$.params.special_id')    AS special_id
     , GET_JSON_OBJECT(request, '$.params.p_name')        AS p_name
     , GET_JSON_OBJECT(request, '$.params.p_mobile')      AS mobile
     , GET_JSON_OBJECT(request, '$.params.age')           AS age
     , GET_JSON_OBJECT(request, '$.params.gender')        AS gender
     , GET_JSON_OBJECT(request, '$.params.sale_id')       AS sale_id
     , GET_JSON_OBJECT(request, '$.params.openid')        AS openid
     , GET_JSON_OBJECT(request, '$.params._category')     AS category
     , GET_JSON_OBJECT(request, '$.params.from_page')     AS from_page
     , GET_JSON_OBJECT(request, '$.params.code')          AS code
     , GET_JSON_OBJECT(request, '$.params.state')         AS state
     , GET_JSON_OBJECT(request, '$.params.projectid')     AS projectid
     , GET_JSON_OBJECT(request, '$.params.p_checked')     AS p_checked
     , request
     , json_str
     --20250417新增 is_ori ori_price字段
     , GET_JSON_OBJECT(request, '$.params.is_ori')     AS is_ori
     , GET_JSON_OBJECT(request, '$.params.ori_price')     AS ori_price
FROM base_Data
;


