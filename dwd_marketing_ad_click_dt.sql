CREATE TABLE IF NOT EXISTS dwd.dwd_marketing_ad_click_dt
(
    `id`               bigint COMMENT 'ID自增',
    `platform`         string COMMENT '平台',
    `raw_click_id`     string COMMENT '原始点击ID',
    `report_link`      string COMMENT '上报链接',
    `report_link_json` string COMMENT '参数提取成json',
    `created_at`       string COMMENT '上报时间',

    `click_id`         STRING COMMENT '点击ID',
    `ad_id`            STRING COMMENT '计划ID',
    `sucai_id`         STRING COMMENT '素材id'
)
    COMMENT '广告点击表'
    PARTITIONED BY (dt date COMMENT '分区日期')
    STORED AS ORCFILE;




CREATE TEMPORARY TABLE parse_json AS
SELECT id
     , platform
     , click_id
     , report_link
     , report_link_json
     , created_at
     , bd_vid
     , qz_gdt
     , gdt_vid
     , IF(LENGTH(douyin_promotion_id) = 19, douyin_promotion_id, NULL) AS douyin_promotion_id
     , douyin_creative_id
     , IF(LENGTH(douyin_adid) = 16, douyin_adid, NULL)                 AS douyin_adid
     , IF(LENGTH(douyin_material_id) = 19, douyin_material_id, NULL)   AS douyin_material_id
     , IF(LENGTH(kuaishou_aid) = 10, kuaishou_aid, NULL)               AS kuaishou_aid
     , IF(LENGTH(wx_aid) = 11, wx_aid, NULL)                           AS wx_aid
     , IF(LENGTH(wx_weixinadinfo) IN (10, 11), wx_weixinadinfo, NULL)  AS wx_weixinadinfo
     , IF(LENGTH(wxmp_adgroup_id) = 11, wxmp_adgroup_id, NULL)         AS wxmp_adgroup_id
     , wx_impression_id
     , wx_element_info
     , IF(LENGTH(baidu_adid) = 11, baidu_adid, NULL)                   AS baidu_adid
     , IF(LENGTH(baidu_unitid) IN (10, 11), baidu_unitid, NULL)        AS baidu_unitid
     , baidu_pid
     , baidu_aid
     , baidu_sucai_id
     , IF(LENGTH(oppo_mastplanid) = 9, oppo_mastplanid, NULL)          AS oppo_mastplanid
FROM (SELECT id
           , platform
           , click_id
           , report_link
           , report_link_json
           , created_at
           , GET_JSON_OBJECT(report_link_json, '$.bd_vid')                                        AS bd_vid
           , GET_JSON_OBJECT(report_link_json, '$.qz_gdt')                                        AS qz_gdt
           , GET_JSON_OBJECT(report_link_json, '$.gdt_vid')                                       AS gdt_vid

           , GET_JSON_OBJECT(report_link_json, '$.promotion_id')                                  AS douyin_promotion_id
           , IF(platform = 'douyin', GET_JSON_OBJECT(report_link_json, '$.creative_id'), NULL)    AS douyin_creative_id
           , GET_JSON_OBJECT(report_link_json, '$.adid')                                          AS douyin_adid
           , GET_JSON_OBJECT(report_link_json, '$.material_id')                                   AS douyin_material_id

           , IF(platform = 'kuaishou', GET_JSON_OBJECT(report_link_json, '$.aid'), NULL)          AS kuaishou_aid

           , GET_JSON_OBJECT(report_link_json, '$.wx_aid')                                        AS wx_aid
           , SPLIT(GET_JSON_OBJECT(report_link_json, '$.weixinadinfo'), '\\.')[0]                 AS wx_weixinadinfo

           , GET_JSON_OBJECT(report_link_json, '$.adgroup_id')                                    AS wxmp_adgroup_id
           , IF(platform = 'tencent', GET_JSON_OBJECT(report_link_json, '$.impression_id'), NULL) AS wx_impression_id
           , IF(platform = 'tencent', REGEXP_REPLACE(GET_JSON_OBJECT(report_link_json, '$.element_info'), '[^0-9,]',
                                                     ''), NULL)                                   AS wx_element_info

           , GET_JSON_OBJECT(report_link_json, '$.baidu_adid')                                    AS baidu_adid
           , GET_JSON_OBJECT(report_link_json, '$.baidu_unitid')                                  AS baidu_unitid
           , IF(platform = 'baidu', GET_JSON_OBJECT(report_link_json, '$.pid'), NULL)             AS baidu_pid
           , IF(platform = 'baidu', GET_JSON_OBJECT(report_link_json, '$.aid'), NULL)             AS baidu_aid
           , IF(platform = 'baidu', GET_JSON_OBJECT(report_link_json, '$.aid'), NULL)             AS baidu_sucai_id

           , GET_JSON_OBJECT(report_link_json, '$.mastplanid')                                    AS oppo_mastplanid
      FROM (SELECT id
                 , platform
                 , click_id
                 , report_link
                 -- 避免大小写问题，eg：避免promotionId有大写字母解析不出来的情况
                 , report_link_json AS report_link_json
                 , created_at
            FROM ods.ods_marketing_ad_click
            WHERE TO_DATE(created_at) = '${datebuf}') a) aa;


INSERT OVERWRITE TABLE dwd.dwd_marketing_ad_click_dt PARTITION (dt = '${datebuf}')
SELECT id
     , platform
     , raw_click_id
     , report_link
     , report_link_json
     , created_at
     , click_id
     , ad_id
     , sucai_id
FROM (SELECT *
           -- 为避免重复，开窗取第最后一次
           , ROW_NUMBER() OVER (PARTITION BY click_id ORDER BY created_at DESC ) AS rn
      FROM (SELECT id
                 , platform
                 , click_id AS raw_click_id
                 , report_link
                 , report_link_json
                 , created_at

                 , CASE platform
                       WHEN 'tencent' THEN wx_impression_id
                       ELSE click_id
              END           AS click_id

                 , CASE platform
                       WHEN 'tencent' THEN wxmp_adgroup_id
                       WHEN 'douyin' THEN douyin_promotion_id
                       WHEN 'baidu' THEN baidu_pid
              END           AS ad_id

                 , CASE platform
                       WHEN 'baidu' THEN baidu_aid
              -- 抖音优先订单表的report_link,其次report_link带的点击，其次回传，其次回传的点击
                       WHEN 'tencent' THEN wx_element_info
              END           AS sucai_id

            FROM parse_json) a) aa
WHERE rn = 1;


