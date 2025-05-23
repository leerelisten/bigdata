

-- 建表思路  口径：
-- 渠道：抖音、腾讯、百度
-- 数据来源：取自 投放素材报表、百度素材报表、腾讯素材报表(只取的视频)
-- 平均点击单价(元/次)	账面消耗/点击数
-- 点击率(%)	点击/曝光
-- 转化率(%)	转化数/点击
-- 转化成本(元/次)	账面消耗/转化数
-- 付费成本(元/次)	账面消耗/付费次数
-- 有效获客成本(加微成本)(元/次)	账面消耗/有效获客
-- 有效播放成本(元/次)	账面消耗/有效播放数
-- 有效播放率(%)	有效播放数/点击次数
-- 3秒播放率(%)	3s播放完成次数/（抖音分母：总播放数、腾讯、百度分母：曝光）
-- 完播率(%)	100%进度播放次数/（抖音分母：总播放数、腾讯、百度分母：曝光）

CREATE TABLE IF NOT EXISTS app.c_app_lexue_sucai_month_report
(
    date_month               STRING COMMENT '月份',
    ad_department            STRING COMMENT '投放部门',
    cat                      string COMMENT '品类',
    platform_name            STRING COMMENT '平台名称',
    sucai_id                 STRING COMMENT '素材ID',
    sucai_name               STRING COMMENT '素材名称',
    director                 STRING COMMENT '编导',
    post_production          STRING COMMENT '后期',
    items                    STRING COMMENT '项目',
    content_type             STRING COMMENT '内容类型',
    cost                     DECIMAL(19,4) COMMENT '账面消耗(元)',
    cost_real                DECIMAL(19,4) COMMENT '实际消耗(元)',
    roi_D4                   DECIMAL(19,4) COMMENT 'D4ROI',
    roi_D7                   DECIMAL(19,4) COMMENT 'D7ROI',
    roi_D8                   DECIMAL(19,4) COMMENT 'D8ROI',
    roi_D10                  DECIMAL(19,4) COMMENT 'D10ROI',
    roi                      DECIMAL(19,4) COMMENT 'ROI',
    show_cnt                 BIGINT COMMENT '曝光量(次)',
    click_cnt                BIGINT COMMENT '点击量(次)',
    avg_click_price          DECIMAL(19,4) COMMENT '平均点击单价(元/次)',
    click_rate               DECIMAL(19,4) COMMENT '点击率(%) ',
    convert_cnt              BIGINT COMMENT '转化数(次)',
    cov_rate                 DECIMAL(19,4) COMMENT '转化率(%) ',
    cov_price                DECIMAL(19,4) COMMENT '转化成本(元/次)',
    game_pay_count           BIGINT COMMENT '付费次数(次)',
    pay_price                DECIMAL(19,4) COMMENT '付费成本(元/次)',
    customer_effective_price DECIMAL(19,4) COMMENT '有效获客成本(元/次) ',
    valid_play_price         DECIMAL(19,4) COMMENT '有效播放成本(元/次) ',
    valid_rate               DECIMAL(19,4) COMMENT '有效播放率(%)',
    play_3s_rate             DECIMAL(19,4) COMMENT '3秒播放率(%)',
    play_rate                DECIMAL(19,4) COMMENT '完播率(%)'
)COMMENT '素材看板-月维度'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_lexue_sucai_month_report';

DROP TABLE platform_merge_data;
CREATE TEMPORARY TABLE platform_merge_data AS
--抖音
SELECT d_date
     , ad_department
     , cat
     , platform_name
     , sucai_id
     , sucai_name
     , cost
     , cost_real
     , pay_sum_D4
     , pay_sum_D7
     , pay_sum_D8
     , pay_sum_D10
     , pay_sum
     , click_cnt           --点击量
     , show_cnt            --曝光量
     , convert_cnt         --转化数
     , game_pay_count      --付费次数
     , customer_effective  --有效获客
     , total_play          --播放数
     , valid_play          --有效播放数
     , play_duration_3s    --3s播放数
     , play_100_feed_break --100%进度播放数
FROM app.c_app_course_daily_sucai_dashboard
WHERE dt = '${datebuf}'
UNION ALL
--百度
SELECT d_date
     , ad_department
     , cat
     , platform_name
     , sucai_id
     , sucai_name
     , cost
     , cost_real
     , pay_sum_D4
     , pay_sum_D7
     , pay_sum_D8
     , pay_sum_D10
     , pay_sum
     , click              AS click_cnt           --点击量
     , impression         AS show_cnt            --曝光量
     , ocpcTargetTrans    AS convert_cnt         --转化数
     , NULL               AS game_pay_count      --付费次数
     , NULL               AS customer_effective  --有效获客
     , NULL               AS total_play          --播放数
     , effectivePlayCount AS valid_play          --有效播放数
     , NULL               AS play_duration_3s    --3s播放数
     , playCount4         AS play_100_feed_break --100%进度播放数
FROM app.c_app_course_daily_baidusucai_dashboard
WHERE dt = '${datebuf}'
UNION ALL
--腾讯
SELECT a.*
FROM (
         SELECT d_date
              , ad_department
              , cat
              , platform_name
              , sucai_id
              , sucai_name
              , cost
              , cost_real
              , pay_sum_D4
              , pay_sum_D7
              , pay_sum_D8
              , pay_sum_D10
              , pay_sum
              , valid_click_count         AS click_cnt           --点击量
              , view_count                AS show_cnt            --曝光量
              , conversions_count         AS convert_cnt         --转化数
              , NULL                      AS game_pay_count      --付费次数
              , NULL                      AS customer_effective  --有效获客
              , NULL                      AS total_play          --播放数
              , video_outer_play_count    AS valid_play          --有效播放数
              , video_outer_play3s_count  AS play_duration_3s    --3s播放数
              , video_outer_play100_count AS play_100_feed_break --100%进度播放数
         FROM app.c_app_course_daily_tencentsucai_dashboard a
         WHERE dt = '${datebuf}'
     ) a
         JOIN
     (
         SELECT d_date
              , CONCAT('id_', sucai_id) AS sucai_id
         FROM da.da_course_daily_cost_by_sucaiid
         WHERE dt = '${datebuf}'
           AND platform = '腾讯'
           AND pos IN ('腾讯公众号', '腾讯朋友圈', '腾讯视频号信息流','腾讯视频号信息流(北京)','腾讯朋友圈(北京)')
           AND sucai_type = 'video'
         GROUP BY d_date, CONCAT('id_', sucai_id)
     ) b
     ON a.d_date = b.d_date
         AND a.sucai_id = b.sucai_id
;


DROP TABLE process_type_data;
CREATE TEMPORARY TABLE process_type_data AS
SELECT d_date
     , ad_department
     , cat
     , platform_name
     , sucai_id
     , sucai_name
     , CASE
           WHEN sucai_name RLIKE '邓' THEN '邓壮'
           WHEN sucai_name RLIKE '琦' THEN '陈琪琦'
           WHEN sucai_name RLIKE '婉' THEN '张婉茹'
           WHEN sucai_name RLIKE '洁' THEN '周嘉洁'
           WHEN sucai_name RLIKE 'D' THEN '邓壮'
           WHEN sucai_name RLIKE 'C' THEN '陈琪琦'
           WHEN sucai_name RLIKE 'Z' THEN '张婉茹'
           WHEN sucai_name RLIKE 'J' THEN '周嘉洁'
           WHEN sucai_name RLIKE 'PH' THEN '周嘉洁'
           else '无'
    END AS bian_dao
     , CASE
           WHEN sucai_name RLIKE '雪|rxx' THEN '阮小雪'
           WHEN sucai_name RLIKE '坚|QJ' THEN '瞿坚'
           WHEN sucai_name RLIKE '慧|ch' THEN '陈慧'
           WHEN sucai_name RLIKE '可|hyk' THEN '黄钰可'
           WHEN sucai_name RLIKE '欣|hrx' THEN '侯冉欣'
           WHEN sucai_name RLIKE '祥' THEN '张昆祥'
           WHEN sucai_name RLIKE '铭' THEN '孙思铭'
           else '无'
    END AS houqi
     , CASE
           WHEN sucai_name RLIKE 'ZB1|1组' THEN '直播1组'
           WHEN sucai_name RLIKE 'ZB3|3组' THEN '直播3组'
           WHEN sucai_name RLIKE 'ZB4|4组' THEN '直播4组'
           WHEN sucai_name RLIKE 'ZB6|6组' THEN '直播6组'
           WHEN sucai_name RLIKE 'ZB7|7组' THEN '直播7组'
           WHEN sucai_name RLIKE 'ZB8|8组' THEN '直播8组'
           else '无'
    END AS items
     , CASE
           WHEN sucai_name RLIKE '老师|师父' THEN 'IP老师实拍'
           WHEN sucai_name RLIKE '演员' THEN '演员实拍'
           WHEN sucai_name RLIKE '原创混剪' THEN '原创混剪'
           WHEN sucai_name RLIKE '后期混剪' THEN '后期混剪'
           WHEN sucai_name RLIKE '数字人' THEN '数字人AI混剪'
           ELSE '无'
    END AS content_type
     , cost
     , cost_real
     , pay_sum_D4
     , pay_sum_D7
     , pay_sum_D8
     , pay_sum_D10
     , pay_sum
     , click_cnt           --点击量
     , show_cnt            --曝光量
     , convert_cnt         --转化数
     , game_pay_count      --付费次数
     , customer_effective  --有效获客
     , total_play          --播放数
     , valid_play          --有效播放数
     , play_duration_3s    --3s播放数
     , play_100_feed_break --100%进度播放数
FROM platform_merge_data
;


INSERT OVERWRITE TABLE app.c_app_lexue_sucai_month_report PARTITION (dt = '${datebuf}')
SELECT SUBSTR(d_date, 1, 7)                                          AS date_month
     , CASE  when ad_department = '其他' then '无' else ad_department end as ad_department
     , cat
     , platform_name
     , sucai_id
     , sucai_name
     , bian_dao
     , houqi
     , items
     , content_type
     , SUM(cost)                                                     AS cost
     , SUM(cost_real)                                                AS cost_real
     , NVL(SUM(pay_sum_D4) / SUM(cost_real), 0)                      AS roi_D4
     , NVL(SUM(pay_sum_D7) / SUM(cost_real), 0)                      AS roi_D7
     , NVL(SUM(pay_sum_D8) / SUM(cost_real), 0)                      AS roi_D8
     , NVL(SUM(pay_sum_D10) / SUM(cost_real), 0)                     AS roi_D10
     , NVL(SUM(pay_sum) / SUM(cost_real), 0)                         AS roi
     , SUM(show_cnt)                                                 AS show_cnt                 --曝光
     , SUM(click_cnt)                                                AS click_cnt                --点击
     , NVL(SUM(cost) / SUM(click_cnt), 0)                            AS avg_click_price          --平均点击单价
     , NVL(SUM(click_cnt) / SUM(show_cnt), 0)                        AS click_rate               --点击率
     , SUM(convert_cnt)                                              AS convert_cnt              --转化数
     , NVL(SUM(convert_cnt) / SUM(click_cnt), 0)                     AS cov_rate                 --转化率

     , NVL(SUM(cost) / SUM(convert_cnt), 0)                          AS cov_price                --转化成本
     , NVL(SUM(game_pay_count), 0)                                   AS game_pay_count           --付费次数
     , NVL(SUM(cost) / SUM(game_pay_count), 0)                       AS pay_price                --付费成本
     , NVL(SUM(cost) / SUM(customer_effective), 0)                   AS customer_effective_price --有效获客成本
     , NVL(SUM(cost) / SUM(valid_play), 0)                           AS valid_play_price         --有效播放成本
     , CASE
           WHEN platform_name = '抖音' THEN NVL(SUM(valid_play) / SUM(total_play), 0)
           ELSE NVL(SUM(valid_play) / SUM(show_cnt), 0) END          AS valid_rate               --有效播放率


     , CASE
           WHEN platform_name = '抖音' THEN NVL(SUM(play_duration_3s) / SUM(total_play), 0)
           ELSE NVL(SUM(play_duration_3s) / SUM(show_cnt), 0) END    AS play_3s_rate             --3s播放率

     , CASE
           WHEN platform_name = '抖音' THEN NVL(SUM(play_100_feed_break) / SUM(total_play), 0)
           ELSE NVL(SUM(play_100_feed_break) / SUM(show_cnt), 0) END AS play_rate                --完播率

FROM process_type_data
where  SUBSTR(d_date, 1, 7) >= '2025-01'
GROUP BY SUBSTR(d_date, 1, 7)
       ,  CASE  when ad_department = '其他' then '无' else ad_department end
       , cat
       , platform_name
       , sucai_id
       , sucai_name
       , bian_dao
       , houqi
       , items
       , content_type
;

