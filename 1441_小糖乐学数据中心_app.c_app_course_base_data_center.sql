SET mapred.job.name="c_app_course_base_data_center#${datebuf}";
USE app;
CREATE EXTERNAL TABLE IF NOT EXISTS app.c_app_course_base_data_center
(
    d_date               string COMMENT '日期',
    type                 string COMMENT '类型',
    full_name            string COMMENT 'CRM名称',
    long_name            string COMMENT '中文名称',
    short_name           string COMMENT '缩写名称',
    parent_platform      string COMMENT '版位归属渠道',
    remark               string COMMENT '备注',
    create_time          string COMMENT '创建时间'
)
    COMMENT '培训主题数仓-小糖乐学数据中心'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_base_data_center';

SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=8192;
SET mapreduce.reduce.memory.mb=8192;
SET hive.execution.engine = tez;
INSERT OVERWRITE TABLE app.c_app_course_base_data_center PARTITION(dt = '${datebuf}')
SELECT  '${datebuf}' as d_date
          ,CASE WHEN a.type = 'agent' THEN '代理'
             WHEN a.type = 'category' THEN '品类'
             WHEN a.type = 'company' THEN '主体'
             WHEN a.type = 'link_type' THEN '链路类型'
             WHEN a.type = 'mobile_show' THEN '收集手机号类型'
             WHEN a.type = 'platform' AND a.parent_name = '' THEN '渠道'
             WHEN a.type = 'platform' AND a.parent_name <> '' THEN '版位' END AS type
       ,a.full_name
       ,a.long_name
       ,a.short_name
       ,coalesce(b.long_name,'')                                            AS parent_platform
       ,'' as remark
       ,a.create_time
FROM ods.ods_place_config a
LEFT JOIN ods.ods_place_config b
ON a.parent_name = b.full_name
where a.type not in ('area')