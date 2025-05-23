CREATE TABLE IF NOT EXISTS dw.lexue_message_row_data
(
    detail string COMMENT '详情'
)
    COMMENT '句子聊天信息json数据'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/olap/db/lexue_message_row_data';


LOAD DATA INPATH '/lexue/message_bak/${datebuf}/*' OVERWRITE INTO TABLE dw.lexue_message_row_data PARTITION (dt = '${datebuf}');


INSERT OVERWRITE TABLE dw.lexue_message PARTITION (dt = '${datebuf}')
SELECT GET_JSON_OBJECT(detail, '$.botId')              AS botId
     , GET_JSON_OBJECT(detail, '$.contactType')        AS contactType
     , GET_JSON_OBJECT(detail, '$.imRoomId')           AS imRoomId
     , GET_JSON_OBJECT(detail, '$.roomWecomChatId')    AS roomWecomChatId
     , GET_JSON_OBJECT(detail, '$.roomTopic')          AS roomTopic
     , GET_JSON_OBJECT(detail, '$.messageId')          AS messageId
     , GET_JSON_OBJECT(detail, '$.source')             AS source
     , GET_JSON_OBJECT(detail, '$.timestamp')          AS `timestamp`
     , GET_JSON_OBJECT(detail, '$.messageType')        AS messageType
     , GET_JSON_OBJECT(detail, '$.payload')            AS payload
     , GET_JSON_OBJECT(detail, '$.datetime')           AS datetime
     , GET_JSON_OBJECT(detail, '$.sourcedata')         AS sourcedata
     , GET_JSON_OBJECT(detail, '$.sourcetype')         AS sourcetype
     , GET_JSON_OBJECT(detail, '$.td_role')            AS td_role
     , GET_JSON_OBJECT(detail, '$.td_botUserId')       AS td_botUserId
     , GET_JSON_OBJECT(detail, '$.td_imBotId')         AS td_imBotId
     , GET_JSON_OBJECT(detail, '$.td_externalUserId')  AS td_externalUserId
     , GET_JSON_OBJECT(detail, '$.td_imContactId')     AS td_imContactId
     , GET_JSON_OBJECT(detail, '$.td_contactName')     AS td_contactName
     , GET_JSON_OBJECT(detail, '$.td_userContactName') AS td_userContactName
     , GET_JSON_OBJECT(detail, '$.utime')              AS utime
FROM dw.lexue_message_row_data
WHERE dt = '${datebuf}'
;

