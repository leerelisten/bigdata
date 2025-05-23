-- 25.1.22日新增is_abroad字段，区分海内外期次，所有权限按照海内外进行分组。

CREATE TABLE IF NOT EXISTS dws.dws_report_permission_day
(
    `report_id`       bigint COMMENT '报表id',
    `is_abroad`       varchar(20) COMMENT '海内外',
    `cat`             string COMMENT '权限-品类',
    `platform_name`   string COMMENT '权限-渠道',
    `pos`             string COMMENT '权限-版位',
    `ad_department`   string COMMENT '权限-投放部门',
    `sale_department` string COMMENT '权限-销售部门',
    `sop_type`        string COMMENT '权限-SOP类型',
    `emails`          string COMMENT 'emails'
)
    COMMENT 'DWS层BI权限表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE;


-- 分渠道版位权限part1：列转行
DROP TABLE IF EXISTS explode_detial;
CREATE TEMPORARY TABLE explode_detial AS
SELECT report_id
     , email
     , all_permission
     , is_abroad
     -- 炸开后前面有个空格，需去掉
     , TRIM(category)  AS category
     , TRIM(plat_pos)  AS plat_pos
     , TRIM(ad_dept)   AS ad_dept
     , TRIM(sale_dept) AS sale_dept
     , TRIM(sop_type)  AS sop_type
FROM (SELECT *
      FROM dwd.dwd_report_permission_day
      WHERE dt = '${datebuf}') a
         LATERAL VIEW EXPLODE(SPLIT(NVL(cat, '全部'), ',')) exploded_table AS category
         LATERAL VIEW EXPLODE(SPLIT(NVL(platform_pos, '全部'), ',')) exploded_table AS plat_pos
         LATERAL VIEW EXPLODE(SPLIT(NVL(ad_department, '全部'), ',')) exploded_table AS ad_dept
         LATERAL VIEW EXPLODE(SPLIT(NVL(department, '全部'), ',')) exploded_table AS sale_dept
         LATERAL VIEW EXPLODE(SPLIT(NVL(ai_sop, '全部'), ',')) exploded_table AS sop_type
;


-- 行转列，进行汇总。
DROP TABLE permission_detail;
CREATE TEMPORARY TABLE permission_detail AS
SELECT report_id
     , all_permission
     , is_abroad
     , category
     , plat_pos
     , ad_dept
     , sale_dept
     , sop_type
     , CONCAT_WS(',', COLLECT_SET(email)) AS emails
FROM explode_detial
GROUP BY report_id, all_permission, is_abroad, category, plat_pos, ad_dept, sale_dept, sop_type;

-- 关联维表，生成最终表。
INSERT OVERWRITE TABLE dws.dws_report_permission_day PARTITION (dt = '${datebuf}')
SELECT report_id
     , IF(is_abroad IS NULL OR all_permission = 1, '全部', is_abroad) AS is_abroad
     , IF(category IS NULL OR all_permission = 1, '全部', category) AS cat
     , IF(SPLIT(NVL(pp.platform_pos, pd.plat_pos), '-')[0] IS NULL OR all_permission = 1, '全部', SPLIT(NVL(pp.platform_pos, pd.plat_pos), '-')[0]) AS platform_name
     , IF(SPLIT(NVL(pp.platform_pos, pd.plat_pos), '-')[1] IS NULL OR all_permission = 1, '全部', SPLIT(NVL(pp.platform_pos, pd.plat_pos), '-')[1]) AS pos
     , IF(nvl(dept.department, pd.ad_dept) IS NULL OR all_permission = 1, '全部', nvl(dept.department, pd.ad_dept)) AS ad_department
     , IF(nvl(dept2.department, pd.sale_dept) IS NULL OR all_permission = 1, '全部', nvl(dept2.department, pd.sale_dept)) AS sale_department
     , IF(sop_type IS NULL OR all_permission = 1, '全部', sop_type) AS sop_type
     , emails
FROM permission_detail pd
         LEFT JOIN dim.dim_permission_platform_pos pp
                   ON pd.plat_pos = pp.record_id
         LEFT JOIN dim.dim_permission_department_group dept
                   ON pd.ad_dept = dept.record_id
         LEFT JOIN dim.dim_permission_department_group dept2
                   ON pd.sale_dept = dept2.record_id
WHERE emails IS NOT NULL