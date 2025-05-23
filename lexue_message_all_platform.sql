CREATE TABLE IF NOT EXISTS dw.lexue_message_all_platform_row_data
(
    detail string COMMENT '详情'
)
    COMMENT '全平台聊天信息json数据'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/olap/db/lexue_message_all_platform_row_data';


LOAD DATA INPATH '/lexue/message_all_platform_bak/${datebuf}/*' OVERWRITE INTO TABLE dw.lexue_message_all_platform_row_data PARTITION (dt = '${datebuf}');


INSERT OVERWRITE TABLE dw.lexue_message_all_platform PARTITION (dt = '${datebuf}')
SELECT GET_JSON_OBJECT(detail, '$.botId')                  AS botId
     , GET_JSON_OBJECT(detail, '$.imBotId')                AS imBotId
     , GET_JSON_OBJECT(detail, '$.botUserId')              AS botUserId
     , GET_JSON_OBJECT(detail, '$.chatId')                 AS chatId
     , GET_JSON_OBJECT(detail, '$.avatar')                 AS avatar
     , GET_JSON_OBJECT(detail, '$.isSelf')                 AS isSelf
     , GET_JSON_OBJECT(detail, '$.imContactId')            AS imContactId
     , GET_JSON_OBJECT(detail, '$.externalUserId')         AS externalUserId
     , GET_JSON_OBJECT(detail, '$.contactName')            AS contactName
     , GET_JSON_OBJECT(detail, '$.contactType')            AS contactType
     , GET_JSON_OBJECT(detail, '$.imRoomId')               AS imRoomId
     , GET_JSON_OBJECT(detail, '$.roomWecomChatId')        AS roomWecomChatId
     , GET_JSON_OBJECT(detail, '$.roomTopic')              AS roomTopic
     , GET_JSON_OBJECT(detail, '$.messageId')              AS messageId
     , GET_JSON_OBJECT(detail, '$.uniqueId')               AS uniqueId
     , GET_JSON_OBJECT(detail, '$.customerExternalUserId') AS customerExternalUserId
     , GET_JSON_OBJECT(detail, '$.c_corpid')               AS c_corpid
     , GET_JSON_OBJECT(detail, '$.c_platform')             AS c_platform
     , GET_JSON_OBJECT(detail, '$.source')                 AS source
     , GET_JSON_OBJECT(detail, '$.timestamp')              AS `timestamp`
     , GET_JSON_OBJECT(detail, '$.messageType')            AS messageType
     , GET_JSON_OBJECT(detail, '$.groupId')                AS groupId
     , GET_JSON_OBJECT(detail, '$.payload')                AS payload
     , GET_JSON_OBJECT(detail, '$.timestamp_date')         AS timestamp_date
     , GET_JSON_OBJECT(detail, '$.ext')                    AS ext
FROM dw.lexue_message_all_platform_row_data
where dt = '${datebuf}'