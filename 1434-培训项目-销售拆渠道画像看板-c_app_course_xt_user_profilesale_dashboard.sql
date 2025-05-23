SET mapred.job.name="c_app_course_xt_user_profilesale_dashboard#${datebuf}";
USE app;
CREATE TABLE IF NOT EXISTS app.c_app_course_xt_user_profilesale_dashboard
(
    d_date        string COMMENT '时间',
    grouptype     string COMMENT '分组类型',
    grouptype2    string COMMENT '画像类型',
    is_abroad     string COMMENT '海外/国内',
    cat           string COMMENT '品类',
    goods_name    string COMMENT '期次',
    ad_department string COMMENT '投放部门',
    platform_name string COMMENT '渠道',
    pos           string COMMENT '版位',
    department    string COMMENT '部门',
    user_group    string COMMENT '组',
    sales_name    string COMMENT '销售姓名',
    --price         string COMMENT '价格',
    --mobile        string COMMENT '手机号',
    --link_type_v2  string COMMENT '链路',
    --cost_id       string COMMENT '广告账户',
    portrait      string COMMENT '属性',
    user_nem      int COMMENT '例子数(个)',
    pay_num       float COMMENT '订单数(单)',
    pay_sum       float COMMENT 'GMV(元)',
    conv_rate     float COMMENT '转化率(%)',
    permission    string COMMENT '权限用户'
)
    COMMENT '培训项目-画像报表_新(期次分区)'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_xt_user_profilesale_dashboard';


SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=2048;
SET mapreduce.reduce.memory.mb=2048;
-- set hive.execution.engine = tez;   -- 潘孟姣240910：表里用了tez引擎和unionall操作，导致数据写入dt下的不同分区了，impala加载不出来

CREATE TEMPORARY TABLE c_app_course_xt_user_profile_ten_goods AS
SELECT
    d_date
     ,cat
     ,member_id
     ,contact_ex_nickname
     ,phone
     ,goods_name
     ,department
     ,user_group
     ,sales_name
     ,cost_id
     ,wx_rel_status
     ,is_get_ticket
     ,xe_id
     ,h5_id
     ,ad_department
     ,platform_name
     ,pos
     ,price
     ,mobile
     ,link_type_v2
     ,wx_add_time
     ,pay_num
     ,pay_sum
     ,collect_time
     ,form_name
     ,form_cat
     ,extra
     ,sex
     ,address
     ,city_level
     ,age
     , case when goods_name like '%道门八段锦%' then age else age_level end as age_level
     ,work
     ,taiji_exp
     ,taiji_basic
     ,taiji_hope
     ,taiji_cause
     ,taiji_interest
     ,taiji_influence
     ,is_abroad
FROM app.c_app_course_xt_user_profile
WHERE dt = '${datebuf}'
  AND goods_name <> ''
  --这里先注释掉 背景：20250403 发现业务方有 期次包含“测试”二字 但是数据是需要的
--   AND goods_name NOT LIKE '%测试%'
  AND goods_name IS NOT NULL
  AND TO_DATE(CONCAT('20', SUBSTR(goods_name, 2, 2), '-', SUBSTR(goods_name, 4, 2), '-',
                     SUBSTR(goods_name, 6, 2))) BETWEEN DATE_SUB(CURRENT_DATE, 12) AND DATE_SUB(CURRENT_DATE, 1)
;


CREATE TEMPORARY TABLE c_app_course_xt_user_profilesale_dashboard_tmp AS
--1.性别
SELECT '${datebuf}'                          AS d_date
     , CASE GROUPING__ID
           WHEN 254 THEN '1_海内外x品类'
           WHEN 126 THEN '2_海内外x品类_期次'
           WHEN 30 THEN '3_海内外x品类_期次x投放部门x渠道'
           WHEN 14 THEN '4_海内外x品类_期次x投放部门x渠道x版位'
           WHEN 6 THEN '5_海内外x品类_期次x投放部门x渠道x版位x部门'
           WHEN 2 THEN '6_海内外x品类_期次x投放部门x渠道x版位x部门x组'
           WHEN 0 THEN '7_海内外x品类_期次x投放部门x渠道x版位x部门x组x销售'
    END                                      AS grouptype
     , '性别'                                AS grouptype2
     , is_abroad
     , cat
     , goods_name
     , ad_department
     , platform_name
     , pos
     , department
     , user_group
     , sales_name
     --, price
     --, mobile
     --, link_type_v2
     --, cost_id
     , sex                                   AS portrait
     , COUNT(member_id)                      AS user_nem
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(member_id) * 100 AS conv_rate
FROM c_app_course_xt_user_profile_ten_goods
GROUP BY is_abroad
       , cat
       , goods_name
       , ad_department
       , platform_name
       , pos
       , department
       , user_group
       , sales_name
--, price
--, mobile
--, link_type_v2
--, cost_id
       , sex
    GROUPING SETS (
       (is_abroad, cat, sex)
       , (is_abroad, cat, goods_name, sex)
       , (is_abroad, cat, goods_name, ad_department, platform_name, sex)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, sex)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, sex)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, sex)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, sales_name, sex)
    )
UNION ALL
--2.年龄层
SELECT '${datebuf}'                          AS d_date
     , CASE GROUPING__ID
           WHEN 254 THEN '1_海内外x品类'
           WHEN 126 THEN '2_海内外x品类_期次'
           WHEN 30 THEN '3_海内外x品类_期次x投放部门x渠道'
           WHEN 14 THEN '4_海内外x品类_期次x投放部门x渠道x版位'
           WHEN 6 THEN '5_海内外x品类_期次x投放部门x渠道x版位x部门'
           WHEN 2 THEN '6_海内外x品类_期次x投放部门x渠道x版位x部门x组'
           WHEN 0 THEN '7_海内外x品类_期次x投放部门x渠道x版位x部门x组x销售'
    END                                      AS grouptype
     , '年龄层'                              AS grouptype2
     , is_abroad
     , cat
     , goods_name
     , ad_department
     , platform_name
     , pos
     , department
     , user_group
     , sales_name
     --, price
     --, mobile
     --, link_type_v2
     --, cost_id
     , age_level                             AS portrait
     , COUNT(member_id)                      AS user_nem
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(member_id) * 100 AS conv_rate
FROM c_app_course_xt_user_profile_ten_goods
GROUP BY is_abroad
       , cat
       , goods_name
       , ad_department
       , platform_name
       , pos
       , department
       , user_group
       , sales_name
--, price
--, mobile
--, link_type_v2
--, cost_id
       , age_level
    GROUPING SETS (
       (is_abroad, cat, age_level)
       , (is_abroad, cat, goods_name, age_level)
       , (is_abroad, cat, goods_name, ad_department, platform_name, age_level)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, age_level)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, age_level)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, age_level)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, sales_name, age_level)
    )
UNION ALL
--3.城市等级
SELECT '${datebuf}'                          AS d_date
     , CASE GROUPING__ID
           WHEN 254 THEN '1_海内外x品类'
           WHEN 126 THEN '2_海内外x品类_期次'
           WHEN 30 THEN '3_海内外x品类_期次x投放部门x渠道'
           WHEN 14 THEN '4_海内外x品类_期次x投放部门x渠道x版位'
           WHEN 6 THEN '5_海内外x品类_期次x投放部门x渠道x版位x部门'
           WHEN 2 THEN '6_海内外x品类_期次x投放部门x渠道x版位x部门x组'
           WHEN 0 THEN '7_海内外x品类_期次x投放部门x渠道x版位x部门x组x销售'
    END                                      AS grouptype
     , '城市等级'                            AS grouptype2
     , is_abroad
     , cat
     , goods_name
     , ad_department
     , platform_name
     , pos
     , department
     , user_group
     , sales_name
     --, price
     --, mobile
     --, link_type_v2
     --, cost_id
     , city_level                            AS portrait
     , COUNT(member_id)                      AS user_nem
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(member_id) * 100 AS conv_rate
FROM c_app_course_xt_user_profile_ten_goods
GROUP BY is_abroad
       , cat
       , goods_name
       , ad_department
       , platform_name
       , pos
       , department
       , user_group
       , sales_name
--, price
--, mobile
--, link_type_v2
--, cost_id
       , city_level
    GROUPING SETS (
       (is_abroad, cat, city_level)
       , (is_abroad, cat, goods_name, city_level)
       , (is_abroad, cat, goods_name, ad_department, platform_name, city_level)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, city_level)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, city_level)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, city_level)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, sales_name, city_level)
    )
UNION ALL
-- 4.职业
SELECT '${datebuf}'                          AS d_date
     , CASE GROUPING__ID
           WHEN 254 THEN '1_海内外x品类'
           WHEN 126 THEN '2_海内外x品类_期次'
           WHEN 30 THEN '3_海内外x品类_期次x投放部门x渠道'
           WHEN 14 THEN '4_海内外x品类_期次x投放部门x渠道x版位'
           WHEN 6 THEN '5_海内外x品类_期次x投放部门x渠道x版位x部门'
           WHEN 2 THEN '6_海内外x品类_期次x投放部门x渠道x版位x部门x组'
           WHEN 0 THEN '7_海内外x品类_期次x投放部门x渠道x版位x部门x组x销售'
    END                                      AS grouptype
     , '职业'                                AS grouptype2
     , is_abroad
     , cat
     , goods_name
     , ad_department
     , platform_name
     , pos
     , department
     , user_group
     , sales_name
     --, price
     --, mobile
     --, link_type_v2
     --, cost_id
     , work
     , COUNT(member_id)                      AS user_num
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(member_id) * 100 AS conv_rate
FROM c_app_course_xt_user_profile_ten_goods
GROUP BY is_abroad
       , cat
       , goods_name
       , ad_department
       , platform_name
       , pos
       , department
       , user_group
       , sales_name
--, price
--, mobile
--, link_type_v2
--, cost_id
       , work
    GROUPING SETS (
       (is_abroad, cat, work)
       , (is_abroad, cat, goods_name, work)
       , (is_abroad, cat, goods_name, ad_department, platform_name, work)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, work)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, work)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, work)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, sales_name, work)
    )
UNION ALL
-- 5.太极报名历史
SELECT '${datebuf}'                          AS d_date
     , CASE GROUPING__ID
           WHEN 254 THEN '1_海内外x品类'
           WHEN 126 THEN '2_海内外x品类_期次'
           WHEN 30 THEN '3_海内外x品类_期次x投放部门x渠道'
           WHEN 14 THEN '4_海内外x品类_期次x投放部门x渠道x版位'
           WHEN 6 THEN '5_海内外x品类_期次x投放部门x渠道x版位x部门'
           WHEN 2 THEN '6_海内外x品类_期次x投放部门x渠道x版位x部门x组'
           WHEN 0 THEN '7_海内外x品类_期次x投放部门x渠道x版位x部门x组x销售'
    END                                      AS grouptype
     , '报名历史'                            AS grouptype2
     , is_abroad
     , cat
     , goods_name
     , ad_department
     , platform_name
     , pos
     , department
     , user_group
     , sales_name
     --, price
     --, mobile
     --, link_type_v2
     --, cost_id
     , taiji_exp                             AS portrait
     , COUNT(member_id)                      AS user_nem
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(member_id) * 100 AS conv_rate
FROM c_app_course_xt_user_profile_ten_goods
GROUP BY is_abroad
       , cat
       , goods_name
       , ad_department
       , platform_name
       , pos
       , department
       , user_group
       , sales_name
--, price
--, mobile
--, link_type_v2
--, cost_id
       , taiji_exp
    GROUPING SETS (
       (is_abroad, cat, taiji_exp)
       , (is_abroad, cat, goods_name, taiji_exp)
       , (is_abroad, cat, goods_name, ad_department, platform_name, taiji_exp)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, taiji_exp)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, taiji_exp)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, taiji_exp)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, sales_name, taiji_exp)
    )
UNION ALL
-- 6.太极基础
SELECT '${datebuf}'                          AS d_date
     , CASE GROUPING__ID
           WHEN 254 THEN '1_海内外x品类'
           WHEN 126 THEN '2_海内外x品类_期次'
           WHEN 30 THEN '3_海内外x品类_期次x投放部门x渠道'
           WHEN 14 THEN '4_海内外x品类_期次x投放部门x渠道x版位'
           WHEN 6 THEN '5_海内外x品类_期次x投放部门x渠道x版位x部门'
           WHEN 2 THEN '6_海内外x品类_期次x投放部门x渠道x版位x部门x组'
           WHEN 0 THEN '7_海内外x品类_期次x投放部门x渠道x版位x部门x组x销售'
    END                                      AS grouptype
     , '太极基础'                            AS grouptype2
     , is_abroad
     , cat
     , goods_name
     , ad_department
     , platform_name
     , pos
     , department
     , user_group
     , sales_name
     --, price
     --, mobile
     --, link_type_v2
     --, cost_id
     , taiji_basic                           AS portrait
     , COUNT(member_id)                      AS user_nem
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(member_id) * 100 AS conv_rate
FROM c_app_course_xt_user_profile_ten_goods
GROUP BY is_abroad
       , cat
       , goods_name
       , ad_department
       , platform_name
       , pos
       , department
       , user_group
       , sales_name
--, price
--, mobile
--, link_type_v2
--, cost_id
       , taiji_basic
    GROUPING SETS (
       (is_abroad, cat, taiji_basic)
       , (is_abroad, cat, goods_name, taiji_basic)
       , (is_abroad, cat, goods_name, ad_department, platform_name, taiji_basic)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, taiji_basic)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, taiji_basic)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, taiji_basic)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, sales_name, taiji_basic)
    )
UNION ALL
-- 7.太极核心问题
SELECT '${datebuf}'                          AS d_date
     , CASE GROUPING__ID
           WHEN 254 THEN '1_海内外x品类'
           WHEN 126 THEN '2_海内外x品类_期次'
           WHEN 30 THEN '3_海内外x品类_期次x投放部门x渠道'
           WHEN 14 THEN '4_海内外x品类_期次x投放部门x渠道x版位'
           WHEN 6 THEN '5_海内外x品类_期次x投放部门x渠道x版位x部门'
           WHEN 2 THEN '6_海内外x品类_期次x投放部门x渠道x版位x部门x组'
           WHEN 0 THEN '7_海内外x品类_期次x投放部门x渠道x版位x部门x组x销售'
    END                                      AS grouptype
     , '核心问题'                            AS grouptype2
     , is_abroad
     , cat
     , goods_name
     , ad_department
     , platform_name
     , pos
     , department
     , user_group
     , sales_name
     --, price
     --, mobile
     --, link_type_v2
     --, cost_id
     , taiji_hope                            AS portrait
     , COUNT(member_id)                      AS user_nem
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(member_id) * 100 AS conv_rate
FROM c_app_course_xt_user_profile_ten_goods
GROUP BY is_abroad
       , cat
       , goods_name
       , ad_department
       , platform_name
       , pos
       , department
       , user_group
       , sales_name
--, price
--, mobile
--, link_type_v2
--, cost_id
       , taiji_hope
    GROUPING SETS (
       (is_abroad, cat, taiji_hope)
       , (is_abroad, cat, goods_name, taiji_hope)
       , (is_abroad, cat, goods_name, ad_department, platform_name, taiji_hope)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, taiji_hope)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, taiji_hope)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, taiji_hope)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, sales_name, taiji_hope)
    )
UNION ALL
-- 8.太极学习原因
SELECT '${datebuf}'                          AS d_date
     , CASE GROUPING__ID
           WHEN 254 THEN '1_海内外x品类'
           WHEN 126 THEN '2_海内外x品类_期次'
           WHEN 30 THEN '3_海内外x品类_期次x投放部门x渠道'
           WHEN 14 THEN '4_海内外x品类_期次x投放部门x渠道x版位'
           WHEN 6 THEN '5_海内外x品类_期次x投放部门x渠道x版位x部门'
           WHEN 2 THEN '6_海内外x品类_期次x投放部门x渠道x版位x部门x组'
           WHEN 0 THEN '7_海内外x品类_期次x投放部门x渠道x版位x部门x组x销售'
    END                                      AS grouptype
     , '学习原因'                            AS grouptype2
     , is_abroad
     , cat
     , goods_name
     , ad_department
     , platform_name
     , pos
     , department
     , user_group
     , sales_name
     --, price
     --, mobile
     --, link_type_v2
     --, cost_id
     , taiji_cause                           AS portrait
     , COUNT(member_id)                      AS user_nem
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(member_id) * 100 AS conv_rate
FROM c_app_course_xt_user_profile_ten_goods
GROUP BY is_abroad
       , cat
       , goods_name
       , ad_department
       , platform_name
       , pos
       , department
       , user_group
       , sales_name
--, price
--, mobile
--, link_type_v2
--, cost_id
       , taiji_cause
    GROUPING SETS (
       (is_abroad, cat, taiji_cause)
       , (is_abroad, cat, goods_name, taiji_cause)
       , (is_abroad, cat, goods_name, ad_department, platform_name, taiji_cause)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, taiji_cause)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, taiji_cause)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, taiji_cause)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, sales_name, taiji_cause)
    )
UNION ALL
-- 9.太极的兴趣度
SELECT '${datebuf}'                          AS d_date
     , CASE GROUPING__ID
           WHEN 254 THEN '1_海内外x品类'
           WHEN 126 THEN '2_海内外x品类_期次'
           WHEN 30 THEN '3_海内外x品类_期次x投放部门x渠道'
           WHEN 14 THEN '4_海内外x品类_期次x投放部门x渠道x版位'
           WHEN 6 THEN '5_海内外x品类_期次x投放部门x渠道x版位x部门'
           WHEN 2 THEN '6_海内外x品类_期次x投放部门x渠道x版位x部门x组'
           WHEN 0 THEN '7_海内外x品类_期次x投放部门x渠道x版位x部门x组x销售'
    END                                      AS grouptype
     , '兴趣度'                              AS grouptype2
     , is_abroad
     , cat
     , goods_name
     , ad_department
     , platform_name
     , pos
     , department
     , user_group
     , sales_name
     --, price
     --, mobile
     --, link_type_v2
     --, cost_id
     , taiji_interest                        AS portrait
     , COUNT(member_id)                      AS user_nem
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(member_id) * 100 AS conv_rate
FROM c_app_course_xt_user_profile_ten_goods
GROUP BY is_abroad
       , cat
       , goods_name
       , ad_department
       , platform_name
       , pos
       , department
       , user_group
       , sales_name
--, price
--, mobile
--, link_type_v2
--, cost_id
       , taiji_interest
    GROUPING SETS (
       (is_abroad, cat, taiji_interest)
       , (is_abroad, cat, goods_name, taiji_interest)
       , (is_abroad, cat, goods_name, ad_department, platform_name, taiji_interest)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, taiji_interest)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, taiji_interest)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, taiji_interest)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, sales_name
       , taiji_interest)
    )
UNION ALL
-- 10.太极健康问题的紧迫度
SELECT '${datebuf}'                          AS d_date
     , CASE GROUPING__ID
           WHEN 254 THEN '1_海内外x品类'
           WHEN 126 THEN '2_海内外x品类_期次'
           WHEN 30 THEN '3_海内外x品类_期次x投放部门x渠道'
           WHEN 14 THEN '4_海内外x品类_期次x投放部门x渠道x版位'
           WHEN 6 THEN '5_海内外x品类_期次x投放部门x渠道x版位x部门'
           WHEN 2 THEN '6_海内外x品类_期次x投放部门x渠道x版位x部门x组'
           WHEN 0 THEN '7_海内外x品类_期次x投放部门x渠道x版位x部门x组x销售'
    END                                      AS grouptype
     , '紧迫度'                              AS grouptype2
     , is_abroad
     , cat
     , goods_name
     , ad_department
     , platform_name
     , pos
     , department
     , user_group
     , sales_name
     --, price
     --, mobile
     --, link_type_v2
     --, cost_id
     , taiji_influence                       AS portrait
     , COUNT(member_id)                      AS user_nem
     , SUM(pay_num)                          AS pay_num
     , SUM(pay_sum)                          AS pay_sum
     , SUM(pay_num) / COUNT(member_id) * 100 AS conv_rate
FROM c_app_course_xt_user_profile_ten_goods
GROUP BY is_abroad
       , cat
       , goods_name
       , ad_department
       , platform_name
       , pos
       , department
       , user_group
       , sales_name
--, price
--, mobile
--, link_type_v2
--, cost_id
       , taiji_influence
    GROUPING SETS (
       (is_abroad, cat, taiji_influence)
       , (is_abroad, cat, goods_name, taiji_influence)
       , (is_abroad, cat, goods_name, ad_department, platform_name, taiji_influence)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, taiji_influence)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, taiji_influence)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, taiji_influence)
       , (is_abroad, cat, goods_name, ad_department, platform_name, pos, department, user_group, sales_name
       , taiji_influence)
    )
;
INSERT OVERWRITE TABLE app.c_app_course_xt_user_profilesale_dashboard PARTITION (dt = '${datebuf}')
SELECT a.d_date
     , a.grouptype
     , a.grouptype2
     , a.is_abroad
     , a.cat
     , a.goods_name
     , a.ad_department
     , a.platform_name
     , a.pos
     , a.department
     , a.user_group
     , a.sales_name
     , a.portrait
     , a.user_nem
     , a.pay_num
     , a.pay_sum
     , conv_rate
     , CONCAT_WS(',', COLLECT_SET(pm.emails)) AS permission
FROM c_app_course_xt_user_profilesale_dashboard_tmp a
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
        AND report_id = '1434') pm
     ON (pm.is_abroad = '全部' OR a.is_abroad = pm.is_abroad)
         AND (pm.cat = '全部' OR a.cat = pm.cat)
         AND (pm.sale_department = '全部' OR a.department = pm.sale_department)
         AND (pm.ad_department = '全部' OR a.ad_department = pm.ad_department)
GROUP BY a.d_date
       , a.grouptype
       , a.grouptype2
       , a.is_abroad
       , a.cat
       , a.goods_name
       , a.ad_department
       , a.platform_name
       , a.pos
       , a.department
       , a.user_group
       , a.sales_name
       , a.portrait
       , a.user_nem
       , a.pay_num
       , a.pay_sum
       , conv_rate