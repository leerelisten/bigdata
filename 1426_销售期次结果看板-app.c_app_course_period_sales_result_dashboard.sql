-- 1426:销售期次结果看板
-- 汪娇需求：销售期次结果看板开发
-- 25.3.19日销售分层上线，根据 过去两期次目标达成率均值 s:>=120% A:>=90% B:>=55% C:<55%
-- 25.4.26日销售架构变动，0424、0426两期小组销售架构按照组员匹配出小组结果


SET mapred.job.name="c_app_course_period_sales_result_dashboard#${datebuf}";
USE app;


SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;


CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_course_period_sales_result_dashboard
(
    d_date                    string COMMENT '日期',
    grouptype                 string COMMENT '分组类型',
    goods_name                string COMMENT '期次',
    department                string COMMENT '部门',
    user_group                string COMMENT '小组',
    sales_name                string COMMENT '销售',
    sales_level               string COMMENT '销售层级',
    R_value_reach_rate        float COMMENT '两期次目标达成率均值',
    avg_period_r_squared      float COMMENT '两期平均达成R值',
    avg_period_goal_r_squared float COMMENT '两期平均目标R值',
    user_num                  int COMMENT '本期次例子数（个）',
    pay_user_num_d4           int COMMENT '本期次D4购买例子数（个）',
    pay_num_d4                float COMMENT '本期次D4正价课订单数（单）',
    pay_sum_d4                float COMMENT '本期次D4正价课GMV（元）',
    pay_user_num_d8           int COMMENT '本期次D8购买例子数（个）',
    pay_num_d8                float COMMENT '本期次D8正价课订单数（单）',
    pay_sum_d8                float COMMENT '本期次D8正价课GMV（元）',
    pay_user_num              int COMMENT '本期次购买例子数（个）',
    pay_num                   float COMMENT '本期次正价课订单数（单）',
    pay_sum                   float COMMENT '本期次正价课GMV（元）',


    L1_goods_name             string COMMENT 'last期次',
    L1_user_num               int COMMENT 'last期次例子数（个）',
    L1_pay_user_num_d4        int COMMENT 'last期次D4购买例子数（个）',
    L1_pay_num_d4             float COMMENT 'last期次D4正价课订单数（单）',
    L1_pay_sum_d4             float COMMENT 'last期次D4正价课GMV（元）',
    L1_pay_user_num_d8        int COMMENT 'last期次D8购买例子数（个）',
    L1_pay_num_d8             float COMMENT 'last期次D8正价课订单数（单）',
    L1_pay_sum_d8             float COMMENT 'last期次D8正价课GMV（元）',
    L1_pay_user_num           int COMMENT 'last期次购买例子数（个）',
    L1_pay_sum                float COMMENT 'last期次正价课订单数（单）',
    L1_pay_num                float COMMENT 'last期次正价课GMV（元）',


    L2_goods_name             string COMMENT 'last2期次',
    L2_user_num               int COMMENT 'last2期次例子数（个）',
    L2_pay_user_num_d4        int COMMENT 'last2期次D4购买例子数（个）',
    L2_pay_num_d4             float COMMENT 'last2期次D4正价课订单数（单）',
    L2_pay_sum_d4             float COMMENT 'last2期次D4正价课GMV（元）',
    L2_pay_user_num_d8        int COMMENT 'last2期次D8购买例子数（个）',
    L2_pay_num_d8             float COMMENT 'last2期次D8正价课订单数（单）',
    L2_pay_sum_d8             float COMMENT 'last2期次D8正价课GMV（元）',
    L2_pay_user_num           int COMMENT 'last2期次购买例子数（个）',
    L2_pay_sum                float COMMENT 'last2期次正价课订单数（单）',
    L2_pay_num                float COMMENT 'last2期次正价课GMV（元）'
)
    COMMENT '培训主题数仓-销售期次结果报表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_period_sales_result_dashboard';


-- 取最新周期完结的期次
DROP TABLE IF EXISTS recent_period;
CREATE TEMPORARY TABLE recent_period AS
SELECT goods_name, rn
FROM (SELECT *, RANK() OVER (ORDER BY SUBSTR(goods_name, 2, 6) DESC,user_num DESC) AS rn
      FROM app.c_app_course_period_sales_dashboard
      WHERE dt = '${datebuf}'
--         AND goods_name LIKE '%太极%'
        AND cat = '太极'
        AND is_abroad = '国内'
        AND grouptype = '1_海内外x品类x期次'
--         AND goods_name RLIKE '•5天训练营$'
        AND SUBSTR(goods_name, 2, 6) <= DATE_FORMAT(DATE_ADD('${datebuf}', -7), 'YYMMdd')
        AND user_num >= 30) a
WHERE rn = 1
;


-- 销售本周期结果
DROP TABLE IF EXISTS this_period_result;
CREATE TEMPORARY TABLE this_period_result AS
SELECT b.goods_name
     , c.`ai_r-squared` AS goal_R_Squared
     , a.department
     , a.user_group
     , a.sales_name
     , a.user_num

     , a.pay_user_num
     , a.pay_num
     , a.pay_sum

     , a.pay_user_num_d4
     , a.pay_num_D4
     , a.pay_sum_D4

     , a.pay_user_num_d8
     , a.pay_num_d8
     , a.pay_sum_d8

FROM (SELECT *
      -- 为避免接量期换组，这里一个销售只取一个组,25.4.2日更改口径：如果销售接量期修改架构，导致一个期次内两个架构下同时接量，则取接量多的架构取值
      FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY goods_name,sales_name ORDER BY user_num DESC ) AS rn
            FROM app.c_app_course_period_sales_dashboard
            WHERE dt = '${datebuf}'
              AND grouptype = '4_海内外x品类x期次x部门x组x销售xSOP'
              AND is_abroad = '国内'
              AND cat = '太极'
              AND department IN ('AI-销售一部',
                                 'AI-销售二部')
              AND user_num >= 30) aa
      WHERE rn = 1) a
         INNER JOIN
         (SELECT * FROM recent_period WHERE rn = 1) b
         ON a.goods_name = b.goods_name
         LEFT JOIN ods.ods_xiaoe_sale_goal_dashboard c
                   ON SUBSTR(a.goods_name, 2, 6) = c.goods_name;


DROP TABLE IF EXISTS sale_result;
CREATE TEMPORARY TABLE sale_result AS
SELECT b.*
     , b.conv_rate * 19.80                                                      AS R_Squared
     , ROW_NUMBER() OVER (PARTITION BY b.sales_name ORDER BY b.goods_name DESC) AS rank
FROM this_period_result a
         LEFT JOIN
     (SELECT aa.*, c.`ai_r-squared` AS goal_R_Squared
      FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY goods_name,sales_name ORDER BY user_num DESC ) AS rn
            FROM app.c_app_course_period_sales_dashboard
            WHERE dt = '${datebuf}'
              AND cat = '太极'
              AND is_abroad = '国内'
              AND grouptype = '4_海内外x品类x期次x部门x组x销售xSOP'
              AND SUBSTR(goods_name, 2, 6) <= DATE_FORMAT(DATE_ADD('${datebuf}', -6), 'YYMMdd')
              AND department IN ('AI-销售一部',
                                 'AI-销售二部',
                                 '销售X部')
              AND user_num >= 30) aa
               LEFT JOIN
           ods.ods_xiaoe_sale_goal_dashboard c
           ON SUBSTR(aa.goods_name, 2, 6) = c.goods_name
      WHERE rn = 1) b
     ON a.sales_name = b.sales_name
         AND SUBSTR(a.goods_name, 2, 6) >= SUBSTR(b.goods_name, 2, 6);


-- 小组结果
DROP TABLE IF EXISTS group_result;
CREATE TEMPORARY TABLE group_result AS
SELECT b.*
FROM recent_period a
         LEFT JOIN
     (SELECT aa.*, c.`ai_r-squared` AS goal_R_Squared
      FROM (SELECT *
                 , ROW_NUMBER() OVER (PARTITION BY department,user_group ORDER BY SUBSTR(goods_name, 2, 6) DESC ) AS rn
            FROM app.c_app_course_period_sales_dashboard
            WHERE dt = '${datebuf}'
              AND cat = '太极'
              AND is_abroad = '国内'
              AND grouptype = '3_海内外x品类x期次x部门x组'
              AND SUBSTR(goods_name, 2, 6) <= DATE_FORMAT(DATE_ADD('${datebuf}', -6), 'YYMMdd')
              AND department IN ('AI-销售一部',
                                 'AI-销售二部',
                                 '销售X部')
              AND user_num >= 30) aa
               LEFT JOIN
           ods.ods_xiaoe_sale_goal_dashboard c
           ON SUBSTR(aa.goods_name, 2, 6) = c.goods_name) b
     ON SUBSTR(a.goods_name, 2, 6) >= SUBSTR(b.goods_name, 2, 6);


-- 销售上周期结果
DROP TABLE L1_result;
CREATE TEMPORARY TABLE L1_result AS
SELECT goods_name      AS L1_goods_name
     , goal_R_Squared  AS L1_goal_R_Squared
     , department
     , user_group
     , sales_name

     , user_num        AS L1_user_num

     , pay_user_num_d4 AS L1_pay_user_num_d4
     , pay_sum_d4      AS L1_pay_sum_d4
     , pay_num_d4      AS L1_pay_num_d4

     , pay_user_num_d8 AS L1_pay_user_num_d8
     , pay_num_d8      AS L1_pay_num_d8
     , pay_sum_d8      AS L1_pay_sum_d8

     , pay_user_num    AS L1_pay_user_num
     , pay_num         AS L1_pay_num
     , pay_sum         AS L1_pay_sum
FROM sale_result
WHERE rank = 2;

-- 销售上2周期结果
DROP TABLE L2_result;
CREATE TEMPORARY TABLE L2_result AS
SELECT goods_name      AS L2_goods_name
     , goal_R_Squared  AS L2_goal_R_Squared
     , department
     , user_group
     , sales_name

     , user_num        AS L2_user_num

     , pay_user_num_d4 AS L2_pay_user_num_d4
     , pay_sum_d4      AS L2_pay_sum_d4
     , pay_num_d4      AS L2_pay_num_d4

     , pay_user_num_d8 AS L2_pay_user_num_d8
     , pay_num_d8      AS L2_pay_num_d8
     , pay_sum_d8      AS L2_pay_sum_d8

     , pay_user_num    AS L2_pay_user_num
     , pay_num         AS L2_pay_num
     , pay_sum         AS L2_pay_sum
FROM sale_result
WHERE rank = 3;


-- 计算小组本期次数据
DROP TABLE this_period_group_result;
CREATE TEMPORARY TABLE this_period_group_result AS
SELECT a.goods_name
     , a.department
     , a.user_group
     , a.goal_R_Squared
     , SUM(a.user_num)        AS user_num
     , SUM(a.pay_user_num_d4) AS pay_user_num_d4
     , SUM(a.pay_num_D4)      AS pay_num_D4
     , SUM(a.pay_sum_d4)      AS pay_sum_d4
     , SUM(a.pay_user_num_d8) AS pay_user_num_d8
     , SUM(a.pay_num_d8)      AS pay_num_d8
     , SUM(a.pay_sum_d8)      AS pay_sum_d8
     , SUM(a.pay_user_num)    AS pay_user_num
     , SUM(a.pay_num)         AS pay_num
     , SUM(a.pay_sum)         AS pay_sum
FROM group_result a
         INNER JOIN
     recent_period rp
     ON a.goods_name = rp.goods_name
WHERE a.rn = 1
GROUP BY a.goods_name
       , a.department
       , a.user_group
       , a.goal_R_Squared;


-- 计算小组上期次数据
DROP TABLE last_period_group_result;
CREATE TEMPORARY TABLE last_period_group_result AS
SELECT a.goods_name
     , a.department
     , a.user_group
     , a.goal_R_Squared
     , SUM(a.user_num)        AS user_num
     , SUM(a.pay_user_num_d4) AS pay_user_num_d4
     , SUM(a.pay_num_D4)      AS pay_num_D4
     , SUM(a.pay_sum_d4)      AS pay_sum_d4
     , SUM(a.pay_user_num_d8) AS pay_user_num_d8
     , SUM(a.pay_num_d8)      AS pay_num_d8
     , SUM(a.pay_sum_d8)      AS pay_sum_d8
     , SUM(a.pay_user_num)    AS pay_user_num
     , SUM(a.pay_num)         AS pay_num
     , SUM(a.pay_sum)         AS pay_sum
FROM group_result a
         INNER JOIN
     this_period_group_result b
     ON a.user_group = b.user_group
WHERE a.rn = 2
GROUP BY a.goods_name
       , a.department
       , a.user_group
       , a.goal_R_Squared;


DROP TABLE IF EXISTS result;
CREATE TEMPORARY TABLE result AS
SELECT this.goods_name
     , this.department
     , this.user_group
     , this.sales_name


     , ((this.pay_sum_d8 - this.pay_sum_D4) +
        NVL(last.L1_pay_sum_d8 - last.L1_pay_sum_d4, 0)) /
       ((this.user_num - this.pay_user_num_d4) +
        NVL(last.L1_user_num - last.L1_pay_user_num_d4, 0)) AS 2period_R_Squared


     , (this.goal_R_Squared * (this.user_num - this.pay_user_num_d4) +
        NVL(last.L1_goal_R_Squared * (last.L1_user_num - last.L1_pay_user_num_d4), 0)) /
       ((this.user_num - this.pay_user_num_d4) +
        NVL(last.L1_user_num - last.L1_pay_user_num_d4, 0)) AS 2period_goal_R_Squared


     , this.user_num
     , this.pay_user_num_d4
     , this.pay_num_D4
     , this.pay_sum_D4
     , this.pay_user_num_d8
     , this.pay_num_d8
     , this.pay_sum_d8
     , this.pay_user_num
     , this.pay_num
     , this.pay_sum
     , this.goal_R_Squared


     , last.L1_goods_name
     , last.L1_user_num
     , last.L1_pay_user_num_d4
     , last.L1_pay_num_d4
     , last.L1_pay_sum_d4
     , last.L1_pay_user_num_d8
     , last.L1_pay_num_d8
     , last.L1_pay_sum_d8
     , last.L1_pay_user_num
     , last.L1_pay_sum
     , last.L1_pay_num
     , last.L1_goal_R_Squared


     , last2.L2_goods_name
     , last2.L2_user_num
     , last2.L2_pay_user_num_d4
     , last2.L2_pay_num_d4
     , last2.L2_pay_sum_d4
     , last2.L2_pay_user_num_d8
     , last2.L2_pay_num_d8
     , last2.L2_pay_sum_d8
     , last2.L2_pay_user_num
     , last2.L2_pay_sum
     , last2.L2_pay_num
FROM this_period_result this
         LEFT JOIN L1_result last
                   ON this.sales_name = last.sales_name
         LEFT JOIN L2_result last2
                   ON last.sales_name = last2.sales_name;


-- 计算小组结果
DROP TABLE result_group;
CREATE TEMPORARY TABLE result_group AS
SELECT a.goods_name
     , a.department
     , a.user_group
     , 'ALL-汇总'                               AS sales_name
     , ((a.pay_sum_d8 - a.pay_sum_D4) +
        NVL(b.pay_sum_d8 - b.pay_sum_D4, 0)) /
       ((a.user_num - a.pay_user_num_d4) +
        NVL(b.user_num - b.pay_user_num_d4, 0)) AS 2period_R_Squared


     , (a.goal_R_Squared * (a.user_num - a.pay_user_num_d4) +
        NVL(b.goal_R_Squared * (b.user_num - b.pay_user_num_d4), 0)) /
       ((a.user_num - a.pay_user_num_d4) +
        NVL(b.user_num - b.pay_user_num_d4, 0)) AS 2period_goal_R_Squared

     , a.user_num
     , a.pay_user_num_d4
     , a.pay_num_D4
     , a.pay_sum_D4
     , a.pay_user_num_d8
     , a.pay_num_d8
     , a.pay_sum_d8
     , a.pay_user_num
     , a.pay_num
     , a.pay_sum

     , b.goods_name                             AS L1_goods_name
     , b.user_num                               AS L1_user_num
     , b.pay_user_num_d4                        AS L1_pay_user_num_d4
     , b.pay_num_D4                             AS L1_pay_num_D4
     , b.pay_sum_D4                             AS L1_pay_sum_D4
     , b.pay_user_num_d8                        AS L1_pay_user_num_d8
     , b.pay_num_d8                             AS L1_pay_num_d8
     , b.pay_sum_d8                             AS L1_pay_sum_d8
     , b.pay_user_num                           AS L1_pay_user_num
     , b.pay_num                                AS L1_pay_num
     , b.pay_sum                                AS L1_pay_sum

     , CAST(NULL AS string)                     AS L2_goods_name
     , CAST(NULL AS int)                        AS L2_user_num
     , CAST(NULL AS int)                        AS L2_pay_user_num_d4
     , CAST(NULL AS float)                      AS L2_pay_num_d4
     , CAST(NULL AS float)                      AS L2_pay_sum_d4
     , CAST(NULL AS int)                        AS L2_pay_user_num_d8
     , CAST(NULL AS float)                      AS L2_pay_num_d8
     , CAST(NULL AS float)                      AS L2_pay_sum_d8
     , CAST(NULL AS int)                        AS L2_pay_user_num
     , CAST(NULL AS float)                      AS L2_pay_sum
     , CAST(NULL AS float)                      AS L2_pay_num
FROM this_period_group_result a
         LEFT JOIN
     last_period_group_result b
     ON a.department = b.department
         AND a.user_group = b.user_group
WHERE SUBSTR(a.goods_name, 2, 6) NOT IN ('250424', '250426');

-- 计算小组结果（临时，0424、0426期）
DROP TABLE IF EXISTS temp_group_result;
CREATE TEMPORARY TABLE temp_group_result AS
SELECT goods_name
     , department
     , user_group
     , 'ALL-汇总'                                      AS sales_name
     , (SUM(pay_sum_d8 - pay_sum_D4) +
        SUM(NVL(L1_pay_sum_d8 - L1_pay_sum_D4, 0))) /
       (SUM(user_num - pay_user_num_d4) +
        SUM(NVL(L1_user_num - L1_pay_user_num_d4, 0))) AS 2period_R_Squared


     , (SUM(goal_R_Squared * (user_num - pay_user_num_d4)) +
        SUM(NVL(l1_goal_R_Squared * (l1_user_num - l1_pay_user_num_d4), 0))) /
       (SUM(user_num - pay_user_num_d4) +
        SUM(NVL(l1_user_num - l1_pay_user_num_d4, 0))) AS 2period_goal_R_Squared

     , SUM(user_num)                                   AS user_num
     , SUM(pay_user_num_d4)                            AS pay_user_num_d4
     , SUM(pay_num_D4)                                 AS pay_num_D4
     , SUM(pay_sum_D4)                                 AS pay_sum_D4
     , SUM(pay_user_num_d8)                            AS pay_user_num_d8
     , SUM(pay_num_d8)                                 AS pay_num_d8
     , SUM(pay_sum_d8)                                 AS pay_sum_d8
     , SUM(pay_user_num)                               AS pay_user_num
     , SUM(pay_num)                                    AS pay_num
     , SUM(pay_sum)                                    AS pay_sum

     , CAST(NULL AS string)                            AS L1_goods_name
     , SUM(L1_user_num)                                AS L1_user_num
     , SUM(L1_pay_user_num_d4)                         AS L1_pay_user_num_d4
     , SUM(L1_pay_num_D4)                              AS L1_pay_num_D4
     , SUM(L1_pay_sum_D4)                              AS L1_pay_sum_D4
     , SUM(L1_pay_user_num_d8)                         AS L1_pay_user_num_d8
     , SUM(L1_pay_num_d8)                              AS L1_pay_num_d8
     , SUM(L1_pay_sum_d8)                              AS L1_pay_sum_d8
     , SUM(L1_pay_user_num)                            AS L1_pay_user_num
     , SUM(L1_pay_num)                                 AS L1_pay_num
     , SUM(L1_pay_sum)                                 AS L1_pay_sum

     , CAST(NULL AS string)                            AS L2_goods_name
     , CAST(NULL AS int)                               AS L2_user_num
     , CAST(NULL AS int)                               AS L2_pay_user_num_d4
     , CAST(NULL AS float)                             AS L2_pay_num_d4
     , CAST(NULL AS float)                             AS L2_pay_sum_d4
     , CAST(NULL AS int)                               AS L2_pay_user_num_d8
     , CAST(NULL AS float)                             AS L2_pay_num_d8
     , CAST(NULL AS float)                             AS L2_pay_sum_d8
     , CAST(NULL AS int)                               AS L2_pay_user_num
     , CAST(NULL AS float)                             AS L2_pay_sum
     , CAST(NULL AS float)                             AS L2_pay_num
FROM result
WHERE SUBSTR(goods_name, 2, 6) IN ('250424', '250426')
GROUP BY goods_name, department, user_group;

INSERT OVERWRITE TABLE app.c_app_course_period_sales_result_dashboard PARTITION (dt = '${datebuf}')
SELECT '${datebuf}'                                 AS d_date
     , '1_期次x销售'                                AS grouptype
     , goods_name
     , department
     , user_group
     , sales_name
     , CASE
           WHEN (2period_R_Squared / 2period_goal_R_Squared) >= 1.2 THEN 'S'
           WHEN (2period_R_Squared / 2period_goal_R_Squared) >= 0.9 THEN 'A'
           WHEN (2period_R_Squared / 2period_goal_R_Squared) >= 0.55 THEN 'B'
           ELSE 'C' END                             AS sale_level
     , (2period_R_Squared / 2period_goal_R_Squared) AS R_value_reach_rate
     , 2period_R_Squared
     , 2period_goal_R_Squared
     , user_num
     , pay_user_num_d4
     , pay_num_D4
     , pay_sum_D4
     , pay_user_num_d8
     , pay_num_d8
     , pay_sum_d8
     , pay_user_num
     , pay_num
     , pay_sum
     , L1_goods_name
     , L1_user_num
     , L1_pay_user_num_d4
     , L1_pay_num_d4
     , L1_pay_sum_d4
     , L1_pay_user_num_d8
     , L1_pay_num_d8
     , L1_pay_sum_d8
     , L1_pay_user_num
     , L1_pay_sum
     , L1_pay_num
     , L2_goods_name
     , L2_user_num
     , L2_pay_user_num_d4
     , L2_pay_num_d4
     , L2_pay_sum_d4
     , L2_pay_user_num_d8
     , L2_pay_num_d8
     , L2_pay_sum_d8
     , L2_pay_user_num
     , L2_pay_sum
     , L2_pay_num
FROM result
-- ORDER BY R_value_reach_rate DESC
UNION ALL
SELECT '${datebuf}'                                 AS d_date
     , '2_期次x小组'                                AS grouptype
     , goods_name
     , department
     , user_group
     , sales_name
     , CAST(NULL AS string)                         AS sales_level
     , (2period_R_Squared / 2period_goal_R_Squared) AS R_value_reach_rate
     , 2period_r_squared                            AS avg_period_r_squared
     , 2period_goal_r_squared                       AS avg_period_goal_r_squared
     , user_num
     , pay_user_num_d4
     , pay_num_d4
     , pay_sum_d4
     , pay_user_num_d8
     , pay_num_d8
     , pay_sum_d8
     , pay_user_num
     , pay_num
     , pay_sum
     , l1_goods_name
     , l1_user_num
     , l1_pay_user_num_d4
     , l1_pay_num_d4
     , l1_pay_sum_d4
     , l1_pay_user_num_d8
     , l1_pay_num_d8
     , l1_pay_sum_d8
     , l1_pay_user_num
     , l1_pay_sum
     , l1_pay_num
     , l2_goods_name
     , l2_user_num
     , l2_pay_user_num_d4
     , l2_pay_num_d4
     , l2_pay_sum_d4
     , l2_pay_user_num_d8
     , l2_pay_num_d8
     , l2_pay_sum_d8
     , l2_pay_user_num
     , l2_pay_sum
     , l2_pay_num
FROM result_group
-- 单独处理0424、0426两个期次
UNION ALL
SELECT '${datebuf}'                                 AS d_date
     , '2_期次x小组（0424、0426单独处理）'             AS grouptype
     , goods_name
     , department
     , user_group
     , NULL                                         AS sales_name
     , CAST(NULL AS string)                         AS sales_level
     , (2period_R_Squared / 2period_goal_R_Squared) AS R_value_reach_rate
     , 2period_r_squared                            AS avg_period_r_squared
     , 2period_goal_r_squared                       AS avg_period_goal_r_squared
     , user_num
     , pay_user_num_d4
     , pay_num_d4
     , pay_sum_d4
     , pay_user_num_d8
     , pay_num_d8
     , pay_sum_d8
     , pay_user_num
     , pay_num
     , pay_sum
     , l1_goods_name
     , l1_user_num
     , l1_pay_user_num_d4
     , l1_pay_num_d4
     , l1_pay_sum_d4
     , l1_pay_user_num_d8
     , l1_pay_num_d8
     , l1_pay_sum_d8
     , l1_pay_user_num
     , l1_pay_sum
     , l1_pay_num
     , CAST(NULL AS string)                         AS l2_goods_name
     , CAST(NULL AS int)                            AS l2_user_num
     , CAST(NULL AS int)                            AS l2_pay_user_num_d4
     , CAST(NULL AS float)                          AS l2_pay_num_d4
     , CAST(NULL AS float)                          AS l2_pay_sum_d4
     , CAST(NULL AS int)                            AS l2_pay_user_num_d8
     , CAST(NULL AS float)                          AS l2_pay_num_d8
     , CAST(NULL AS float)                          AS l2_pay_sum_d8
     , CAST(NULL AS int)                            AS l2_pay_user_num
     , CAST(NULL AS float)                          AS l2_pay_sum
     , CAST(NULL AS float)                          AS l2_pay_num
FROM temp_group_result