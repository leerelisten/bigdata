
CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_lexue_log_text
(
    json_str STRING -- 假设request字段是JSON字符串
    ,request string
    , event_id string
) PARTITIONED BY (dt string )
    STORED AS TEXTFILE
;


DROP TEMPORARY FUNCTION IF EXISTS parse_url2;
CREATE TEMPORARY FUNCTION parse_url2 AS 'com.td.bigdata.udf.UrlToJsonUDF' USING JAR 'hdfs:///user/admin/parse_url-3.0-SNAPSHOT.jar';
INSERT OVERWRITE TABLE  ods.ods_lexue_log_text PARTITION (dt = '${datebuf}')
SELECT
    json_str
     ,request
     ,event_id
from (
         SELECT json_str
              , request
              , GET_JSON_OBJECT(request, '$.params.event_id') AS event_id
         FROM (
                  SELECT json_str
                       , parse_url2(request) AS request
                  FROM (
                           SELECT GET_JSON_OBJECT(json_str, '$.request') AS request
                                , json_str
                           FROM dw.lexue_log_text_row_data
                           WHERE dt = '${datebuf}'
                       ) a
              ) t
     )t
WHERE event_id IN
      (
       'e_dadan_course_notify_ck', 'e_dadan_course_notify_sw', 'e_dadan_course_sw', 'e_dadan_course_ck'
          )