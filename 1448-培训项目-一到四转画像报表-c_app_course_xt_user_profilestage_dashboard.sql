SET mapred.job.name="c_app_course_xt_user_profilestage_dashboard#${datebuf}";
USE app;
CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_course_xt_user_profilestage_dashboard
(
    d_date        string COMMENT '时间',
    grouptype     string COMMENT '分组类型',
    conversion_stage string COMMENT '转化阶段',
    grouptype2    string COMMENT '画像类型',
    is_abroad     string COMMENT '海外/国内',
    cat           string COMMENT '品类',
    goods_name    string COMMENT '期次',
    ad_department string COMMENT '投放部门',
    platform_name string COMMENT '渠道',
    pos           string COMMENT '版位',
    portrait      string COMMENT '属性',
    user_num      int COMMENT '例子数(个)',
    pay_num       float COMMENT '订单数(单)',
    pay_sum       float COMMENT 'GMV(元)',
    conv_rate     float COMMENT '转化率(%)',
    pay_num_vip   float COMMENT '会员订单数(单)',
    pay_sum_vip   float COMMENT '会员GMV(元)',
    conv_rate_vip float COMMENT '会员转化率(%)',
    permission    string COMMENT '权限用户'
)
    COMMENT '培训项目-一到四转画像报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_xt_user_profilestage_dashboard';


SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=2048;
SET mapreduce.reduce.memory.mb=2048;
-- set hive.execution.engine = tez;   -- 潘孟姣240910：表里用了tez引擎和unionall操作，导致数据写入dt下的不同分区了，impala加载不出来

DROP TABLE user_1z;
CREATE TEMPORARY TABLE user_1z AS
SELECT is_abroad,
       goods_name,
       sex,
       address,
       city_level,
       age,
       age_level,
       work,
       taiji_exp,
       taiji_basic,
       taiji_hope,
       taiji_cause,
       taiji_interest,
       taiji_influence,
       pay_num,
       pay_sum,
       xe_id
from app.c_app_course_xt_user_profile
where dt='${datebuf}'
  and goods_name rlike '武当秘传太极养生功';

-- 订单表补充属性
DROP TABLE order_0;
CREATE TEMPORARY TABLE order_0 AS
SELECT a.owner_class_name
     ,a.goods_name
     ,a.order_coefficient
     ,a.order_amount
     ,a.user_id
     ,if(a.owner_period_type rlike '海内外','海外','国内') as is_abroad
     ,a.conversion_stage
     ,a.goods_period_type
from dwd.dwd_xiaoe_order_dt a
where a.dt='${datebuf}';


DROP TABLE order_1;
CREATE TEMPORARY TABLE order_1 AS
SELECT is_abroad
     , goods_name
     , NVL(b.sex, '未填写')             sex
     , NVL(b.address, '未填写')         address
     , NVL(b.city_level, '未填写')      city_level
     , NVL(b.age, '未填写')             age
     , NVL(b.age_level, '未填写')       age_level
     , NVL(b.work, '未填写')            work
     , NVL(b.taiji_exp, '未填写')       taiji_exp
     , NVL(b.taiji_basic, '未填写')     taiji_basic
     , NVL(b.taiji_hope, '未填写')      taiji_hope
     , NVL(b.taiji_cause, '未填写')     taiji_cause
     , NVL(b.taiji_interest, '未填写')  taiji_interest
     , NVL(b.taiji_influence, '未填写') taiji_influence
     , order_coefficient as pay_num
     , order_amount      as pay_sum
     , conversion_stage
     , a.user_id
     , a.owner_class_name
     , a.goods_period_type
from order_0 a
         left join (SELECT * FROM dw.dws_sale_questionnaire_day
                    WHERE dt = '${datebuf}'
                      AND form_cat='太极') b
                   ON a.user_id = b.xe_id
;
DROP TABLE user_2z;
CREATE TEMPORARY TABLE user_2z AS
SELECT a.*
     ,b.pay_sum as pay_sum_1,b.pay_num as pay_num_1
FROM (SELECT *
      FROM order_1
      WHERE conversion_stage='一转') a
         LEFT JOIN (SELECT *
                    FROM order_1
                    WHERE conversion_stage='二转') b
                   on a.user_id=b.user_id and a.goods_name=b.owner_class_name;

DROP TABLE user_3z;
CREATE TEMPORARY TABLE user_3z AS
SELECT a.*,
       nvl(b.pay_sum,0) as pay_sum_1,
       nvl(b.pay_num,0) as pay_num_1,
       if(c.user_id is not null,1980,0) as pay_sum_vip,
       if(c.user_id is not null,1,0) as pay_num_vip
FROM (SELECT * FROM order_1 WHERE conversion_stage='二转') a
         LEFT JOIN (SELECT *
                    FROM order_1
                    WHERE conversion_stage='三转'
                      AND goods_period_type in ('炼神营线上班','炼神营全栈班','炼神营续费班')) b
                   on a.user_id=b.user_id and a.goods_name=b.owner_class_name
         LEFT JOIN (SELECT *
                    FROM order_1
                    WHERE conversion_stage='三转'
                      AND goods_period_type in ('会员班')) c
                   on a.user_id=c.user_id and a.goods_name=c.owner_class_name;

DROP TABLE user_4z;
CREATE TEMPORARY TABLE user_4z AS
SELECT a.*,
       nvl(b.pay_sum,0) as pay_sum_1,
       nvl(b.pay_num,0) as pay_num_1,
       if(c.user_id is not null,1980,0) as pay_sum_vip,
       if(c.user_id is not null,1,0) as pay_num_vip
FROM (SELECT * FROM order_1 WHERE conversion_stage='三转'
                              AND goods_period_type in ('炼神营线上班','炼神营全栈班')) a
         LEFT JOIN (SELECT *
                    FROM order_1
                    WHERE conversion_stage='四转'
                      AND goods_period_type in ('炼神营续费班')) b
                   on a.user_id=b.user_id and a.goods_name=b.owner_class_name
         LEFT JOIN (SELECT *
                    FROM order_1
                    WHERE conversion_stage='四转'
                      AND goods_period_type in ('会员班')) c
                   on a.user_id=c.user_id and a.goods_name=c.owner_class_name;

DROP TABLE user_all;
CREATE TEMPORARY TABLE user_all AS
select xe_id,
       is_abroad,
       goods_name,
       sex,
       address,
       city_level,
       age,
       age_level,
       work,
       taiji_exp,
       taiji_basic,
       taiji_hope,
       taiji_cause,
       taiji_interest,
       taiji_influence,
       pay_num,
       pay_sum,
       0 AS pay_num_vip,
       0 AS pay_sum_vip,
       '一转' AS stage
from user_1z
UNION ALL
select user_id,
       is_abroad,
       owner_class_name,
       sex,
       address,
       city_level,
       age,
       age_level,
       work,
       taiji_exp,
       taiji_basic,
       taiji_hope,
       taiji_cause,
       taiji_interest,
       taiji_influence,
       pay_num_1,
       pay_sum_1,
       0 AS pay_num_vip,
       0 AS pay_sum_vip,
       '二转' AS stage
from user_2z
UNION ALL
SELECT user_id
     ,is_abroad
     , goods_name
     , sex
     , address
     , city_level
     , age
     , age_level
     , work
     , taiji_exp
     , taiji_basic
     , taiji_hope
     , taiji_cause
     , taiji_interest
     , taiji_influence
     , pay_num_1
     , pay_sum_1
     , pay_num_vip
     , pay_sum_vip
     , '三转'
from  user_3z
UNION ALL
SELECT user_id
     ,is_abroad
     , goods_name
     , sex
     , address
     , city_level
     , age
     , age_level
     , work
     , taiji_exp
     , taiji_basic
     , taiji_hope
     , taiji_cause
     , taiji_interest
     , taiji_influence
     , pay_num_1
     , pay_sum_1
     , pay_num_vip
     , pay_sum_vip
     , '四转'
from user_4z;

DROP TABLE c_app_course_xt_user_profilestage_dashboard_tmp;
CREATE TEMPORARY TABLE c_app_course_xt_user_profilestage_dashboard_tmp AS
--1.性别
SELECT '${datebuf}'                          AS d_date
     , '性别'                                AS grouptype2
     , stage
     , is_abroad
     , goods_name
     , sex                                   AS portrait
     , COUNT(1)                          AS user_num
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(1) * 100 AS conv_rate
     , SUM(pay_num_vip)                      AS pay_num_vip
     , SUM(pay_sum_vip)                      AS pay_sum_vip
     , SUM(pay_num_vip) / COUNT(1) * 100 AS conv_rate_vip
FROM user_all
GROUP BY stage
       , is_abroad
       , goods_name
       , sex
UNION ALL
--2.年龄层
SELECT '${datebuf}'                          AS d_date
     , '年龄层'                              AS grouptype2
     , stage
     , is_abroad
     , goods_name
     , age_level                             AS portrait
     , COUNT(1)                          AS user_num
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(1) * 100     AS conv_rate
     , SUM(pay_num_vip)                      AS pay_num_vip
     , SUM(pay_sum_vip)                      AS pay_sum_vip
     , SUM(pay_num_vip) / COUNT(1) * 100 AS conv_rate_vip
FROM user_all
GROUP BY stage
       , is_abroad
       , goods_name
       , age_level
UNION ALL
--3.城市等级
SELECT '${datebuf}'                          AS d_date
     , '城市等级'                            AS grouptype2
     , stage
     , is_abroad
     , goods_name
     , city_level                            AS portrait
     , COUNT(1)                          AS user_num
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(1) * 100 AS conv_rate
     , SUM(pay_num_vip)                      AS pay_num_vip
     , SUM(pay_sum_vip)                      AS pay_sum_vip
     , SUM(pay_num_vip) / COUNT(1) * 100 AS conv_rate_vip
FROM user_all
GROUP BY stage
       , is_abroad
       , goods_name
       , city_level
UNION ALL
-- 4.职业
SELECT '${datebuf}'                          AS d_date
     , '职业'                                AS grouptype2
     , stage
     , is_abroad
     , goods_name
     , work
     , COUNT(1)                          AS user_num
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(1) * 100 AS conv_rate
     , SUM(pay_num_vip)                      AS pay_num_vip
     , SUM(pay_sum_vip)                      AS pay_sum_vip
     , SUM(pay_num_vip) / COUNT(1) * 100 AS conv_rate_vip
FROM user_all
GROUP BY stage
       , is_abroad
       , goods_name
       , work
UNION ALL
-- 5.太极报名历史
SELECT '${datebuf}'                          AS d_date
     , '报名历史'                            AS grouptype2
     , stage
     , is_abroad
     , goods_name
     , taiji_exp                             AS portrait
     , COUNT(1)                          AS user_num
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(1) * 100 AS conv_rate
     , SUM(pay_num_vip)                      AS pay_num_vip
     , SUM(pay_sum_vip)                      AS pay_sum_vip
     , SUM(pay_num_vip) / COUNT(1) * 100 AS conv_rate_vip
FROM user_all
GROUP BY stage,is_abroad
       , goods_name
       , taiji_exp
UNION ALL
-- 6.太极基础
SELECT '${datebuf}'                          AS d_date
     , '学习基础'                            AS grouptype2 --20241125 tangwenqi 修复瑜伽画像数据，由太极基础改为学习基础
     , stage
     , is_abroad
     , goods_name
     , taiji_basic                           AS portrait
     , COUNT(1)                          AS user_num
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(1) * 100 AS conv_rate
     , SUM(pay_num_vip)                      AS pay_num_vip
     , SUM(pay_sum_vip)                      AS pay_sum_vip
     , SUM(pay_num_vip) / COUNT(1) * 100 AS conv_rate_vip
FROM user_all
GROUP BY stage
       , is_abroad
       , goods_name
       , taiji_basic
UNION ALL
-- 7.太极核心问题
SELECT '${datebuf}'                          AS d_date
     , '核心问题'                            AS grouptype2
     , stage
     , is_abroad
     , goods_name
     , taiji_hope                            AS portrait
     , COUNT(1)                          AS user_num
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(1) * 100 AS conv_rate
     , SUM(pay_num_vip)                      AS pay_num_vip
     , SUM(pay_sum_vip)                      AS pay_sum_vip
     , SUM(pay_num_vip) / COUNT(1) * 100 AS conv_rate_vip
FROM user_all
GROUP BY stage
       , is_abroad
       , goods_name
       , taiji_hope
UNION ALL
-- 8.太极学习原因
SELECT '${datebuf}'                          AS d_date
     , '学习原因'                            AS grouptype2
     , stage
     , is_abroad
     , goods_name
     , taiji_cause                           AS portrait
     , COUNT(1)                          AS user_num
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(1) * 100 AS conv_rate
     , SUM(pay_num_vip)                      AS pay_num_vip
     , SUM(pay_sum_vip)                      AS pay_sum_vip
     , SUM(pay_num_vip) / COUNT(1) * 100 AS conv_rate_vip
FROM user_all
GROUP BY stage
       , is_abroad
       , goods_name
       , taiji_cause
UNION ALL
-- 9.太极的兴趣度
SELECT '${datebuf}'                          AS d_date
     , '兴趣度'                              AS grouptype2
     , stage
     , is_abroad
     , goods_name
     , taiji_interest                        AS portrait
     , COUNT(1)                          AS user_num
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(1) * 100 AS conv_rate
     , SUM(pay_num_vip)                      AS pay_num_vip
     , SUM(pay_sum_vip)                      AS pay_sum_vip
     , SUM(pay_num_vip) / COUNT(1) * 100 AS conv_rate_vip
FROM user_all
GROUP BY stage,is_abroad
       , goods_name
       , taiji_interest
UNION ALL
-- 10.太极健康问题的紧迫度
SELECT '${datebuf}'                          AS d_date
     , '紧迫度'                              AS grouptype2
     , stage
     , is_abroad
     , goods_name
     , taiji_influence                       AS portrait
     , COUNT(1)                          AS user_num
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(1) * 100 AS conv_rate
     , SUM(pay_num_vip)                      AS pay_num_vip
     , SUM(pay_sum_vip)                      AS pay_sum_vip
     , SUM(pay_num_vip) / COUNT(1) * 100 AS conv_rate_vip
FROM user_all
GROUP BY stage,is_abroad
       , goods_name
       , taiji_influence
;

INSERT OVERWRITE TABLE app.c_app_course_xt_user_profilestage_dashboard PARTITION (dt = '${datebuf}')
SELECT a.d_date
     , a.stage
     , a.grouptype2
     , a.is_abroad
     , a.goods_name
     , a.portrait
     , a.user_num
     , a.pay_num
     , a.pay_sum
     , a.conv_rate
     , a.pay_num_vip
     , a.pay_sum_vip
     , a.conv_rate_vip
     , CONCAT_WS(',', COLLECT_SET(pm.emails)) AS permission
FROM c_app_course_xt_user_profilestage_dashboard_tmp a
         LEFT JOIN
     (SELECT is_abroad
           , cat
           , platform_name
           , pos
           , ad_department
           , sale_department
           , sop_type
           , emails
      FROM dws.dws_report_permission_day
      WHERE dt = '${datebuf}'
        AND report_id = '1448') pm
     ON (pm.is_abroad = '全部' OR a.is_abroad = pm.is_abroad)
GROUP BY a.d_date
       , a.stage
       , a.grouptype2
       , a.is_abroad
       , a.goods_name
       , a.portrait
       , a.user_num
       , a.pay_num
       , a.pay_sum
       , a.conv_rate
       , a.pay_num_vip
       , a.pay_sum_vip
       , a.conv_rate_vip