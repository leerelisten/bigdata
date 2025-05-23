
CREATE TABLE IF NOT EXISTS dwd.dwd_lexue_abtest
(
    buckets     STRING COMMENT '实验分桶组',
    test_type   STRING COMMENT '实验类型',
    platform    STRING COMMENT '渠道',
    platform_id STRING COMMENT '渠道用户唯一标识',
    p_unionid   STRING COMMENT 'unionid 小程序内用户唯一标识',
    p_openid_wx STRING COMMENT '用户openid 小程序内获取',
    member_id   STRING COMMENT '用户自增ID',
    link_type   STRING COMMENT '链路类型',
    h5_id       STRING COMMENT 'h5_id',
    special_id  STRING COMMENT '期次ID',
    p_name      STRING COMMENT 'p_name',
    event_id    STRING COMMENT '曝光/点击/页面停留  e_dadan_course_sw  e_dadan_course_ck',
    p_button    STRING COMMENT '点击按钮',
    p_time      STRING COMMENT '事件触发时间',
    mobile      STRING COMMENT '手机号',
    age         STRING COMMENT '年龄',
    gender      STRING COMMENT '性别',
    sale_id     STRING COMMENT '销售ID',
    p_price     STRING COMMENT '落地页价格',
    p_checked   STRING COMMENT '是否勾选协议',
    p_link      STRING COMMENT '表单提交以后的跳转链接 如小程序链接或获客助手链接',
    link        STRING COMMENT '落地页链接',
    p_status    STRING COMMENT '支付状态',
    order_id    STRING COMMENT '订单ID',
    mobile_show STRING COMMENT '区分是否收集手机号',
    is_ori       STRING COMMENT '1表示原价购买  0表示折扣价购买',
    ori_price    STRING COMMENT '原价字段'
)
    COMMENT 'dwd层解析后的AB日志明细表'
    PARTITIONED BY (dt STRING COMMENT '分区字段，日期')
    STORED AS TEXTFILE
;


INSERT OVERWRITE TABLE dwd.dwd_lexue_abtest PARTITION (dt = '${datebuf}')

SELECT
      DISTINCT
      split(IF(TRIM(bucklist) = '', NULL, bucklist),'-')[0] as bucket
     ,split(IF(TRIM(bucklist) = '', NULL, bucklist),'-')[1] as test_type
     , IF(TRIM(platform) = '', NULL, platform)                                                          AS platform
     , IF(TRIM(platform_id) = '', NULL, platform_id)                                                    AS platform_id
     , IF(TRIM(p_unionid) = '', NULL, p_unionid)                                                        AS p_unionid
     , IF(TRIM(p_openid_wx) = '', NULL, p_openid_wx)                                                    AS p_openid_wx
     , IF(TRIM(member_id) = '', NULL, member_id)                                                        AS member_id
     , IF(TRIM(link_type) = '', NULL, link_type)                                                        AS link_type
     , IF(TRIM(h5_id) = '', NULL, h5_id)                                                                AS h5_id
     , IF(TRIM(special_id) = '', NULL, special_id)                                                      AS special_id
     , IF(TRIM(p_name) = '', NULL, p_name)                                                              AS p_name
     , IF(TRIM(event_id) = '', NULL, event_id)                                                          AS event_id
     , IF(TRIM(p_button) = '', NULL, p_button)                                                          AS p_button
     , IF(TRIM(p_time) = '', NULL, p_time) AS p_time
     , IF(TRIM(mobile) = '', NULL, mobile)                                                              AS mobile
     , IF(TRIM(age) = '', NULL, age)                                                                    AS age
     , IF(TRIM(gender) = '', NULL, gender)                                                              AS gender
     , IF(TRIM(sale_id) = '', NULL, sale_id)                                                            AS sale_id
     , IF(TRIM(p_price) = '', NULL, p_price)                                                            AS p_price
     , IF(TRIM(p_checked) = '', NULL, p_checked)                                                        AS p_checked
     , IF(TRIM(p_link) = '', NULL, p_link)                                                              AS p_link
     , IF(TRIM(link) = '', NULL, link)                                                                  AS link
     , IF(TRIM(p_status) = '', NULL, p_status)                                                          AS p_status
     , IF(TRIM(order_id) = '', NULL, order_id)                                                          AS order_id
     , IF(TRIM(mobile_show) = '', NULL, mobile_show)                                                    AS mobile_show
     ,IF(TRIM(is_ori) = '',NULL,is_ori)                                                                 AS is_ori
     ,IF(TRIM(ori_price) = '',NULL,ori_price)                                                           AS ori_price
FROM (

         SELECT
                --这里做处理是因为有的buckets 中有列表
             regexp_replace(regexp_replace(buckets, '\\[|\\]', ''), '"', '') as buckets
              , regexp_replace(regexp_replace(bucketlist, '\\[|\\]', ''), '"', '') as bucketlist
              ,platform
              ,platform_id
              ,p_unionid
              ,p_openid_wx
              ,member_id
              ,link_type
              ,h5_id
              ,special_id
              ,p_name
              ,event_id
              ,p_button
              ,FROM_UNIXTIME(CAST(p_time / 1000 AS BIGINT), 'yyyy-MM-dd HH:mm:ss') as p_time
              ,mobile
              ,age
              ,gender
              ,sale_id
              ,p_price
              ,p_checked
              ,p_link
              ,link
              ,p_status
              ,order_id
              ,mobile_show
              ,is_ori
              ,ori_price
         from  dwd.dwd_lexue_log_parsed_detail
         WHERE dt = '${datebuf}'
           --筛选AB的数据
           AND env IN (
                       'production', 'release'
             )
           AND event_id IN (
                            'e_dadan_course_sw', 'e_dadan_course_ck'
             )
     )a
         LATERAL VIEW EXPLODE(SPLIT(IF( buckets IS NOT NULL AND buckets <> '', buckets, bucketlist),
                                    ',')) exploded_buckeist AS bucklist
where  bucklist RLIKE 'link_survey_250219|link_code_250219|link_agreement_250219|link_refuse_250219|link_pop_250219|link_payment_250219|link_ori_price_250219'

;