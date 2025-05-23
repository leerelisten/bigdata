CREATE TABLE IF NOT EXISTS dw.ods_es_db_tdlive_history
(
    detail string COMMENT '变更记录'
)
    COMMENT 'tdlive变更记录'
    PARTITIONED BY (month STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/olap/db/ods_es_db_tdlive_history';



DFS -cp /user/wuhanlexue/flume/topic_db_tdlive/history_json /user/wuhanlexue/flume/topic_db_tdlive_bak/history_json;



LOAD DATA INPATH '/user/wuhanlexue/flume/topic_db_tdlive_bak/history_json/crm_member_history_202406.json' OVERWRITE INTO TABLE dw.ods_es_db_tdlive_history PARTITION (month = '202406');
LOAD DATA INPATH '/user/wuhanlexue/flume/topic_db_tdlive_bak/history_json/crm_member_history_202407.json' OVERWRITE INTO TABLE dw.ods_es_db_tdlive_history PARTITION (month = '202407');
LOAD DATA INPATH '/user/wuhanlexue/flume/topic_db_tdlive_bak/history_json/crm_member_history_202408.json' OVERWRITE INTO TABLE dw.ods_es_db_tdlive_history PARTITION (month = '202408');
LOAD DATA INPATH '/user/wuhanlexue/flume/topic_db_tdlive_bak/history_json/crm_member_history_202409.json' OVERWRITE INTO TABLE dw.ods_es_db_tdlive_history PARTITION (month = '202409');
LOAD DATA INPATH '/user/wuhanlexue/flume/topic_db_tdlive_bak/history_json/crm_member_history_202410.json' OVERWRITE INTO TABLE dw.ods_es_db_tdlive_history PARTITION (month = '202410');
LOAD DATA INPATH '/user/wuhanlexue/flume/topic_db_tdlive_bak/history_json/crm_member_history_202411.json' OVERWRITE INTO TABLE dw.ods_es_db_tdlive_history PARTITION (month = '202411');
LOAD DATA INPATH '/user/wuhanlexue/flume/topic_db_tdlive_bak/history_json/crm_member_history_202412.json' OVERWRITE INTO TABLE dw.ods_es_db_tdlive_history PARTITION (month = '202412');