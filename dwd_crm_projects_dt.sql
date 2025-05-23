CREATE TABLE IF NOT EXISTS dwd.dwd_crm_projects_dt
(
    id               bigint,
    name             STRING COMMENT 'project name',
    business         string COMMENT '业务线',
    training_camp_id string COMMENT '训练营id crm_class.xiaoe_special_id',
    userids          string COMMENT '托管用户id',
    relate_userid    string COMMENT '关联的用户ID',
    original_userid  string COMMENT '原始用户id',
    start_time       string COMMENT '上线时间',
    end_time         string COMMENT '下线时间',
    state            tinyint COMMENT '状态',
    creator_id       bigint COMMENT 'creator id',
    created_at       timestamp COMMENT 'created time',
    updated_at       timestamp COMMENT 'updated time',
    deleted_at       timestamp COMMENT 'delete time',
    room             string COMMENT '项目托管账户绑定企微群',
    course_config    string COMMENT '课程相关配置',
    search_tag       string COMMENT '流程中tag查询条件过滤',
    is_debug         tinyint COMMENT 'debug项目 0 否 1 是',
    ai_version       int COMMENT 'AI版本',
    room_emoji       string COMMENT '群表情'
)
    COMMENT 'DWD层AI接量销售账号表'
    PARTITIONED BY (dt string COMMENT '分区日期')
    STORED AS ORC;


CREATE TEMPORARY TABLE crm_projects AS
SELECT a.id
     , a.name
     , a.business
     , a.training_camp_id
     , a.userids
     , IF(a.relate_userid = '', NULL, a.relate_userid)     AS relate_userid
     , IF(a.original_userid = '', NULL, a.original_userid) AS original_userid
     , a.start_time
     , a.end_time
     , a.state
     , a.creator_id
     , FROM_UNIXTIME(a.created_at)                         AS created_at
     , FROM_UNIXTIME(a.updated_at)                         AS updated_at
     , FROM_UNIXTIME(a.deleted_at)                         AS deleted_at
     , IF(a.room = '', NULL, a.room)                       AS room
     , IF(a.course_config = '', NULL, a.course_config)     AS course_config
     , IF(a.search_tag = '', NULL, a.search_tag)           AS search_tag
     , a.is_debug
     --0412期江诗韵 需要调整成AI 并且ai版本不能归属a或者b 这里临时置为8
     , CASE
           WHEN a.training_camp_id = 'p_67ed0975e4b0694c5ab7f029' AND a.userids = 'jiangshiyun01_3681' THEN 8
           ELSE a.ai_version END                           AS ai_version
     , IF(a.room_emoji = '', NULL, a.room_emoji)           AS room_emoji
FROM ods.ods_crm_projects a
         INNER JOIN
     dwd.dwd_xiaoe_special b
     ON a.training_camp_id = b.special_id
WHERE a.name NOT LIKE '%测试%'
  -- 25.4.20日 跟尉中哲确定，去掉删除的数据,去掉end_time在期次之后的数据
  AND a.deleted_at = 0
  -- state=3不生效，is_debug=1为测试
--   AND state = 1
  AND (a.is_debug = 0 OR (a.training_camp_id = 'p_67dcfed0e4b0694c5aaea2e9' AND a.userids IN (
                                                                                              'shenwei_3897',
                                                                                              'zhangjing_4251',
                                                                                              'huyanan_4248',
                                                                                              'tangyaping_3017',
                                                                                              'wangziheng_2972'
    ))
    OR (
           --0412期江诗韵 需要调整成AI
           a.training_camp_id = 'p_67ed0975e4b0694c5ab7f029'
               AND a.userids = 'jiangshiyun01_3681'
           )
    );



INSERT OVERWRITE TABLE dwd.dwd_crm_projects_dt PARTITION (dt = '${datebuf}')
SELECT id
     , name
     , business
     , training_camp_id
     , userids
     , relate_userid
     , original_userid
     , start_time
     , end_time
     , state
     , creator_id
     , created_at
     , updated_at
     , deleted_at
     , room
     , course_config
     , search_tag
     , is_debug
     , ai_version
     , room_emoji
FROM (SELECT id
           , name
           , business
           , training_camp_id
           , userids
           , relate_userid
           , original_userid
           , start_time
           , end_time
           , state
           , creator_id
           , created_at
           , updated_at
           , deleted_at
           , room
           , course_config
           , search_tag
           , is_debug
           , ai_version
           , room_emoji
           , ROW_NUMBER() OVER (PARTITION BY training_camp_id,name ORDER BY id DESC) AS rnk
      FROM crm_projects) a
WHERE rnk = 1