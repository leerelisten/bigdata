CREATE TABLE IF NOT EXISTS dws.dws_report_log_add_click_dt
(
    `id`              bigint COMMENT '回传表点击ID',
    `member_id`       string,
    `platform`        string,
    `event_type`      string COMMENT '事件类型（回传类型）',
    `report_link`     string COMMENT '回传链接',
    `param_json`      string COMMENT '参数JSON',
    `raw_click_id`    string COMMENT '原始click_id',
    `created_at`      string COMMENT '回传时间',
    `report_link_md5` string COMMENT '回传链接MD5加密',
    `place_order_id`  string COMMENT 'place_order表ID字段',
    `h5_id`           string,
    `click_id`        string COMMENT '点击ID',
    `ad_id`           string COMMENT '计划ID',
    `sucai_id`        string COMMENT '素材ID'
) COMMENT '回传表关联点击信息' STORED AS ORC;


CREATE TEMPORARY TABLE report_log AS
SELECT a.id
     , a.member_id
     , a.platform
     , a.event_type
     , a.report_link
     , a.param_json
     , a.raw_click_id
     , a.created_at
     , a.report_link_md5
     , a.place_order_id
     , a.h5_id
     , a.click_id
     , NVL(a.ad_id, b.ad_id)       AS ad_id
     , NVL(a.sucai_id, b.sucai_id) AS sucai_id
FROM dwd.dwd_place_report_log_dt a
         LEFT JOIN
     (SELECT *
      FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY click_id ORDER BY id DESC) AS rnk
            FROM dwd.dwd_marketing_ad_click_dt) a
      WHERE rnk = 1) b
     ON a.click_id = b.click_id;


INSERT OVERWRITE TABLE dws.dws_report_log_add_click_dt
SELECT id
     , member_id
     , platform
     , event_type
     , report_link
     , param_json
     , raw_click_id
     , created_at
     , report_link_md5
     , place_order_id
     , h5_id
     , click_id
     , ad_id
     , sucai_id
FROM (SELECT id
           , member_id
           , platform
           , event_type
           , report_link
           , param_json
           , raw_click_id
           , created_at
           , report_link_md5
           , place_order_id
           , h5_id
           , click_id
           , ad_id
           , sucai_id
           -- 添加剔除逻辑
           , ROW_NUMBER() OVER (PARTITION BY place_order_id,event_type ORDER BY created_at DESC) AS rn
      FROM report_log
      WHERE place_order_id != 0) a
WHERE rn = 1
UNION ALL
SELECT id
     , member_id
     , platform
     , event_type
     , report_link
     , param_json
     , raw_click_id
     , created_at
     , report_link_md5
     , place_order_id
     , h5_id
     , click_id
     , ad_id
     , sucai_id
FROM report_log
WHERE place_order_id = 0