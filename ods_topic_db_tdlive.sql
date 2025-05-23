CREATE TABLE IF NOT EXISTS dw.ods_topic_db_tdlive
(
    detail string COMMENT '变更记录'
)
    COMMENT 'tdlive变更记录'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/olap/db/ods_topic_db_tdlive';

DFS -cp /user/wuhanlexue/flume/topic_db_tdlive/${datebuf} /user/wuhanlexue/flume/topic_db_tdlive_bak/${datebuf};



LOAD DATA INPATH '/user/wuhanlexue/flume/topic_db_tdlive_bak/${datebuf}/*' OVERWRITE INTO TABLE dw.ods_topic_db_tdlive PARTITION (dt = '${datebuf}');