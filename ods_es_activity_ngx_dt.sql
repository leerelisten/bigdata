-- 表单H5页面nginx日志
CREATE TABLE IF NOT EXISTS ods.ods_es_activity_ngx_dt
(
    http_host    varchar(200) COMMENT '请求地址',
    size         varchar(200) COMMENT 'size',
    body         string COMMENT '请求体',
    referer      string COMMENT 'referer',
    backtime     string COMMENT '返回响应时间',
    agent        string COMMENT 'user_agent',
    host         string COMMENT 'host',
    log_path     string COMMENT '日志文件地址',
    clientip     string COMMENT 'clientip',
    xff          string COMMENT '客户端公网IP',
    responsetime string COMMENT '响应时间',
    request      string COMMENT '请求体',
    request_json string COMMENT '请求体JSON',
    request_time timestamp COMMENT '请求时间',
    version      string COMMENT '版本',
    status       string COMMENT '请求状态',
    row_json     string COMMENT '原始json'

)
    COMMENT 'activity项目ngx日志'
    PARTITIONED BY (dt string COMMENT '分区日期')
    STORED AS ORC;


DROP TABLE IF EXISTS activity_ngx_data;
CREATE TEMPORARY TABLE activity_ngx_data
(
    ngx_json string COMMENT 'nginx中json字符串'
);

DROP TEMPORARY FUNCTION IF EXISTS parse_url2;
CREATE TEMPORARY FUNCTION parse_url2 AS 'com.td.bigdata.udf.UrlToJsonUDF' USING JAR 'hdfs:///user/admin/parse_url-3.0-SNAPSHOT.jar';


LOAD DATA INPATH '/user/wuhanlexue/es/activity_ngx_bak/${datebuf}/*' OVERWRITE INTO TABLE activity_ngx_data;


INSERT OVERWRITE TABLE ods.ods_es_activity_ngx_dt PARTITION (dt = '${datebuf}')
SELECT GET_JSON_OBJECT(ngx_json, '$.http_host')           AS http_host
     , GET_JSON_OBJECT(ngx_json, '$.size')                AS size
     , GET_JSON_OBJECT(ngx_json, '$.body')                AS body
     , GET_JSON_OBJECT(ngx_json, '$.referer')             AS referer
     , GET_JSON_OBJECT(ngx_json, '$.backtime')            AS backtime
     , GET_JSON_OBJECT(ngx_json, '$.agent')               AS agent
     , GET_JSON_OBJECT(ngx_json, '$.host')                AS host
     , GET_JSON_OBJECT(ngx_json, '$.path')                AS log_path
     , GET_JSON_OBJECT(ngx_json, '$.clientip')            AS clientip
     , GET_JSON_OBJECT(ngx_json, '$.xff')                 AS xff
     , GET_JSON_OBJECT(ngx_json, '$.responsetime')        AS responsetime
     , GET_JSON_OBJECT(ngx_json, '$.request')             AS request
     , parse_url2(GET_JSON_OBJECT(ngx_json, '$.request')) AS request_json
     , FROM_UNIXTIME(
        UNIX_TIMESTAMP(REGEXP_EXTRACT(ngx_json, '"@timestamp": "(.*?)"', 1), 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''),
        'yyyy-MM-dd HH:mm:ss')                            AS request_time
     , REGEXP_EXTRACT(ngx_json, '"@version": "(.*?)"', 1) AS version
     , GET_JSON_OBJECT(ngx_json, '$.status')              AS STATUS
     , ngx_json                                           AS row_json
FROM activity_ngx_data;