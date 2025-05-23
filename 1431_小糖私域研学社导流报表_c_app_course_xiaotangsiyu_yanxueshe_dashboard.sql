SET mapred.job.name="c_app_course_xiaotangsiyu_yanxueshe_dashboard#${datebuf}";
USE app;
CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_course_xiaotangsiyu_yanxueshe_dashboard
(
    d_date          string COMMENT '分区日期',
    week            string COMMENT '时间区间',
    h5_id           string COMMENT 'h5_id',
    title           string COMMENT '渠道来源',
    pos             string COMMENT '版位',
    course          string COMMENT '课程',
    wx_num          int COMMENT '加微人数(个)',
    wxdelete_num    int COMMENT '单删人数(个)',
    user_num        int COMMENT '有效例子数(个)',
    pay_num         float COMMENT '订单数(单)',
    pay_sum         float COMMENT '营收(元)',
    conv_rate       float COMMENT '转化率=订单数/有效例子数(%)',
    user_efficiency float COMMENT '人效=营收/有效例子数(元/人)'
)
    COMMENT '培训主题数仓-小糖私域研学社导流报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_xiaotangsiyu_yanxueshe_dashboard';

SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;
SET hive.execution.engine = tez;

WITH user_info AS
    ( -- 所有例子的投放属性&加微、问卷状态&购买正价课状态
        SELECT a.member_id
             , a.cat
             , a.platform
             , a.platform_name
             , a.h5_id
             , c.title
             , a.p_source
             , a.pos
             , a.price
             , a.mobile
             , a.link_type_v1
             , CASE WHEN a.link_type_v2 = '新一键' THEN '新一键授权' ELSE a.link_type_v2 END AS link_type_v2
             , a.cost_id
             , a.ad_id
             , a.sucai_id
             , a.created_at
             , a.wx_rel_status
             , a.goods_name
             , a.xe_id
             , a.ifcollect
             , a.trade_state
             , a.member_status
             , a.sales_id
             , IF(b.user_id IS NOT NULL, 1, 0)                                                  ifbuy
             , NVL(b.pay_num, 0)                                                                pay_num
             , NVL(b.pay_sum, 0)                                                                pay_sum
             , IF(wx_rel_status IN (2, 3) AND UNIX_TIMESTAMP(a.wx_add_time) - UNIX_TIMESTAMP(a.created_at) <= 300, 1,
                  0)                                                                            wx_active
             , IF(ifcollect = 1 AND UNIX_TIMESTAMP(a.collect_time) - UNIX_TIMESTAMP(a.wx_add_time) <= 3600, 1,
                  0)                                                                            collect_active
        FROM dw.dwd_xt_user a
                 LEFT JOIN (SELECT * FROM dws.dws_sale_buy_course_day WHERE dt = '${datebuf}') b
                           ON a.xe_id = b.user_id
                               AND a.special_id = b.owner_class
                 LEFT JOIN (SELECT h5_id
                                 , title
                            FROM (SELECT h5_id
                                       , title
                                       , ROW_NUMBER() OVER (PARTITION BY h5_id ORDER BY updated_at DESC) row_1 --取更新后的最新一条
                                  FROM dim.dim_place_h5
                                  WHERE TO_DATE(updated_at) <= '${datebuf}') tt
                            WHERE row_1 = 1) c
                           ON a.h5_id = c.h5_id
        WHERE a.dt = '${datebuf}'
          AND TO_DATE(a.created_at) <= '${datebuf}'
          AND a.platform_name = '小糖私域'
          AND a.h5_id not in (2316)
          AND a.goods_name RLIKE '艾灸|手诊')

   , mid AS
    (SELECT TO_DATE(created_at)                                                                 AS d_date
          , pos
          , h5_id
          , title
          , goods_name
          , CASE
                WHEN goods_name LIKE '%太极%' THEN '太极体验课'
                WHEN goods_name RLIKE '艾灸' THEN '艾灸体验课'
                WHEN goods_name LIKE '%手诊%' THEN '手诊体验课' END                             AS course
          , COUNT(IF(member_status IN (1, 15), member_id, NULL))                                AS user_num     -- 有效例子数                                                  AS user_num           -- 例子数
          , COUNT(IF(member_status IN (1, 15) AND wx_rel_status IN (2, 3, 4), member_id, NULL)) AS wx_num       -- 加微人数
          , COUNT(IF(member_status IN (1, 15) AND wx_rel_status IN (3), member_id, NULL))       AS wxdelete_num -- 单删人数
          , SUM(IF(member_status IN (1, 15), pay_num,
                   0))                                                                          AS pay_num      -- 正价课订单数
          , SUM(IF(member_status IN (1, 15), pay_sum,
                   0))                                                                          AS pay_sum
     FROM user_info
     GROUP BY TO_DATE(created_at)
            , pos
            , h5_id
            , title
            , goods_name
            , CASE
                  WHEN goods_name LIKE '%太极%' THEN '太极体验课'
                  WHEN goods_name LIKE '%艾灸%' THEN '艾灸体验课'
                  WHEN goods_name LIKE '%手诊%' THEN '手诊体验课' END)

INSERT
OVERWRITE
TABLE
app.c_app_course_xiaotangsiyu_yanxueshe_dashboard
PARTITION
(
dt = '${datebuf}'
)
SELECT '${datebuf}'                                                                                        AS d_date1
     , CONCAT(DATE_ADD(d_date, 1 - CASE WHEN DAYOFWEEK(d_date) = 1 THEN 7 ELSE DAYOFWEEK(d_date) - 1 END), '至',
              DATE_ADD(d_date, 7 - CASE WHEN DAYOFWEEK(d_date) = 1 THEN 7 ELSE DAYOFWEEK(d_date) - 1 END)) AS week
     , h5_id
     , title
     , pos
     , course
     , SUM(wx_num)                                                                                         AS wx_num          --加微人数
     , SUM(wxdelete_num)                                                                                   AS wxdelete_num    --单删人数
     , SUM(user_num)                                                                                       AS user_num        --有效例子数
     , CASE WHEN course = '艾灸体验课' THEN 0 ELSE SUM(pay_num) END                                        AS pay_num         --订单数
     , CASE WHEN course = '艾灸体验课' THEN 0 ELSE SUM(pay_sum) END                                        AS pay_sum         --营收
     , CASE
           WHEN course = '艾灸体验课' THEN 0
           ELSE NVL(SUM(pay_num) / SUM(user_num) * 100, 0) END                                             AS conv_rate       --转化率
     , CASE
           WHEN course = '艾灸体验课' THEN 0
           ELSE NVL(SUM(pay_sum) / SUM(user_num), 0) END                                                   AS user_efficiency --人效
FROM mid
GROUP BY '${datebuf}'
       , CONCAT(DATE_ADD(d_date, 1 - CASE WHEN DAYOFWEEK(d_date) = 1 THEN 7 ELSE DAYOFWEEK(d_date) - 1 END), '至',
                DATE_ADD(d_date, 7 - CASE WHEN DAYOFWEEK(d_date) = 1 THEN 7 ELSE DAYOFWEEK(d_date) - 1 END))
       , h5_id
       , title
       , pos
       , course


