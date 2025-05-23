DROP TEMPORARY FUNCTION IF EXISTS unicode2string;
CREATE TEMPORARY FUNCTION unicode2string AS 'com.td.bigdata.udf.unicode.Unicode2String' USING JAR 'hdfs:///user/admin/udf-2.0-SNAPSHOT-jar-with-dependencies.jar';

CREATE EXTERNAL TABLE IF NOT EXISTS dw.dws_sale_questionnaire_day
(
    `xe_id`           string COMMENT '小鹅ID',
    `form_name`       STRING COMMENT '问卷名',
    `form_cat`        STRING COMMENT '问卷品类',
    `collect_time`    STRING COMMENT '问卷时间',
    `sex`             STRING COMMENT '性别',
    `age`             STRING COMMENT '年龄',
    `age_level`       STRING COMMENT '年龄段',
    `address`         STRING COMMENT '住址',
    `city_level`      STRING COMMENT '城市等级',
    `work`            STRING COMMENT '工作',
    `extra_original`  STRING COMMENT '原始问卷',
    `taiji_exp`       STRING COMMENT '是否学过太极',
    `taiji_basic`     STRING COMMENT '太极基础',
    `taiji_hope`      STRING COMMENT '希望解决的问题',
    `taiji_cause`     STRING COMMENT '学习目的',
    `taiji_interest`  STRING COMMENT '对课程了解程度和兴趣度',
    `taiji_influence` STRING COMMENT '现在健康情况是否影响生活'
)
    COMMENT '问卷表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/olap/db/dws_sale_questionnaire_day/';


INSERT OVERWRITE TABLE dw.dws_sale_questionnaire_day PARTITION (dt = '${datebuf}')
SELECT t2.user_id   AS                            xe_id
     , t2.form_name
     , t2.form_cat
     , t2.collect_time
     , t2.sex
     , t2.age
     , CASE
           WHEN t2.age <= 40 THEN '40岁及以下'
           WHEN t2.age <= 45 THEN '41-45岁'
           WHEN t2.age <= 50 THEN '46-50岁'
           WHEN t2.age <= 55 THEN '51-55岁'
           WHEN t2.age <= 60 THEN '56-60岁'
           WHEN t2.age <= 65 THEN '61-65岁'
           WHEN t2.age <= 70 THEN '66-70岁'
           WHEN t2.age > 70 THEN '71岁及以上' END age_level
     , t2.address
     , t3.city_live AS                            city_level
     , t2.work
     , t2.extra_original
     , GET_JSON_OBJECT(extra, '$.exp')            taiji_exp
     , GET_JSON_OBJECT(extra, '$.basic')          taiji_basic
     , GET_JSON_OBJECT(extra, '$.hope')           taiji_hope
     , GET_JSON_OBJECT(extra, '$.cause')          taiji_cause
     , GET_JSON_OBJECT(extra, '$.interest')       taiji_interest
     , GET_JSON_OBJECT(extra, '$.influence')      taiji_influence
FROM (SELECT user_id
           , form_name
           , form_cat
           , collect_time
           , extra_original
           , GET_JSON_OBJECT(t1.extra, '$.age')                  age
           , GET_JSON_OBJECT(t1.extra, '$.sex')                  sex
           , GET_JSON_OBJECT(t1.extra, '$.work')                 work


           , REGEXP_REPLACE(
            REGEXP_REPLACE(
                    REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                            REGEXP_REPLACE(extra, '您之前有学过太极课程吗？|您之前有学过太极或八段锦课程吗？|3、您之前有学过瑜伽课程吗？【单选题】', 'exp')
                                        , '您之前的太极、八段锦基础怎么样呢？|您之前的太极基础怎么样呢？|2、您之前的瑜伽基础怎么样呢\\?【单选题】', 'basic')
                                , '您最希望解决的问题是【单选】|4、您最想解决的身体问题是？【单选题】|您最希望解决的问题是', 'hope')
                        ,
                            '您来小糖课堂学习太极、八段锦的核心原因是什么呢？|您来小糖课堂学习太极的核心原因是什么呢？|您来小糖课堂学习太极八段锦的核心原因是什么呢？|1、您来小糖课堂学习瑜伽的初衷是什么呢\\?【单选题】',
                            'cause')
                , '您目前对太极养生的了解和兴趣程度如何|您目前对道门八段锦的了解程度如何', 'interest')
        , '现有的健康问题对您生活的影响如何', 'influence')       extra --20241125 汤文奇 增加瑜伽问卷解析 --20250324道门八段锦问卷解析更新


           , CASE
                 WHEN GET_JSON_OBJECT(t1.extra, '$.address') IS NULL THEN '未填写'
                 WHEN form_id = 'in_pKWDfqVi97kzsKRG'
                     THEN CASE
                              WHEN GET_JSON_OBJECT(t1.extra, '$.address') LIKE '%北京%'
                                  THEN CONCAT('北京市', GET_JSON_OBJECT(t1.extra, '$.address'))
                              WHEN GET_JSON_OBJECT(t1.extra, '$.address') LIKE '%上海%'
                                  THEN CONCAT('上海市', GET_JSON_OBJECT(t1.extra, '$.address'))
                              WHEN GET_JSON_OBJECT(t1.extra, '$.address') LIKE '%天津%'
                                  THEN CONCAT('天津市', GET_JSON_OBJECT(t1.extra, '$.address'))
                              WHEN GET_JSON_OBJECT(t1.extra, '$.address') LIKE '%重庆%'
                                  THEN CONCAT('重庆市', GET_JSON_OBJECT(t1.extra, '$.address'))
                              ELSE GET_JSON_OBJECT(t1.extra, '$.address') END
                 ELSE GET_JSON_OBJECT(t1.extra, '$.address') END address


      FROM (SELECT user_id
                 , form_name
                 , collect_time
                 , unicode2string(extra)  AS                       extra_original
                 , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE((REPLACE(unicode2string(extra), '地址', 'address')), '年龄',
                                                           'age'), '性别', 'sex'), '您的职业是', 'work'), '您的职业',
                                   'work') , '职业', 'work')                                extra -- 20241125 增加瑜伽职业解析
                 , form_id
                 , CASE
                       WHEN form_name RLIKE '古典舞' THEN '古典舞'
                       WHEN form_name RLIKE '居家养生|中医' THEN '养生'
                       WHEN form_name RLIKE '太极' THEN '太极'
                       when form_name RLIKE '道门八段锦' THEN '道门八段锦'
                       WHEN form_name RLIKE '瑜伽' THEN '瑜伽' END form_cat
                 , ROW_NUMBER() OVER (PARTITION BY user_id,CASE
                                                               WHEN form_name RLIKE '古典舞' THEN '古典舞'
                                                               WHEN form_name RLIKE '居家养生|中医' THEN '养生'
                                                               WHEN form_name RLIKE '太极' THEN '太极'
                                                               when form_name RLIKE '道门八段锦' THEN '道门八段锦'
                                                               WHEN form_name RLIKE '瑜伽' THEN '瑜伽' END
              ORDER BY collect_time DESC) AS                       rnum
            FROM ods.ods_xiaoe_info_collect
            WHERE form_id IN (
                              'in_aitPUzd9GWs37HZ5' -- 《古典舞训练营》入学档案
                , 'in_jsSBKhDAmYgsERFQ' -- 武当秘传太极养生入学登记
                , 'in_Yj5KZFMUVn66uoqw' -- 武当秘传太极养生课入学登记
                , 'in_V2MV5uHAnVyDbWPv' -- 【武当秘传】养生太极入学登记
                , 'in_pKWDfqVi97kzsKRG' -- 古典舞训练营入学档案
                , 'in_ugmjfklXOMiE6WQv' -- 《气血瑜伽养生训练营》入学登记
                , 'in_chJFUHyf7Ni0YUig' -- 《居家养生益寿营》入学问卷
                , 'in_66151f08a42a9' -- 古典舞训练营入学档案
                , 'in_662b8f0e7afd8' -- 古典舞训练营入学档案
                , 'in_AnYvAgJYkG1AwzwO' -- 武当太极八段锦养生营入学问卷【新版】
                , 'in_C5s2oywysOIz2Hhu' -- 武当太极八段锦养生营入学问卷
                , 'in_5Ti818ZkeidbIi0l' -- 居家养生训练营--入学问卷
                , 'in_9cQJVHg8UuFqrXlN' -- 28天舒气润腑・养生太极筑基营入学档案
                , 'in_QLqbx1e4U2vQZBnp' -- 【武当秘传】太极养生课入学登记
                , 'in_ZzrMO2bd8ROyOLOl' -- 中医脏腑清毒营-入学问卷
                , 'in_q3YSCatVOufe2WxO' -- 《气血瑜伽养生训练营》入学登记
                , 'in_tZIEHP4FnLgfcOuh' -- 武当太极养生营入学问卷【新版】 20240827 汤文奇添加
                , 'in_kn6mrjALSihv2Phc' --【武当秘传】太极养生入学登记 20240827 郑牧添加
                , 'in_1Qdxal5JRpMcUGOa' -- 武当秘传太极养生功•5天精修训练营入学问卷【海外】 20250213 康斌添加
                , 'in_uJWL1W5JZ8ly2Fpx' --《经络瑜伽养生训练营》入学登记-安妮老师20250310 郑牧添加
                , 'in_app_form'         --太极APP问卷 20250312康斌添加
                , 'in_jwam4xxOP56UykL1' --道门八段锦问卷 20250312康斌添加
                )) t1
      WHERE t1.rnum = 1) t2
         LEFT JOIN dw.tdlive_dim_place_city t3
                   ON t2.address = t3.address;