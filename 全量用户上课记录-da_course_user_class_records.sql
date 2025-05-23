-- alter table da.da_course_user_behavior change column col1 ifcollect string comment '是否填写问卷';
-- drop table da.da_course_user_behavior;
SET mapred.job.name="da_course_user_class_records#${datebuf}";
USE da;
CREATE EXTERNAL TABLE IF NOT EXISTS da.da_course_user_class_records
(
    id                  int COMMENT '用户ID',
    xe_id               string COMMENT '小鹅通ID',
    unionid             string COMMENT 'unionid',
    contact_ex_nickname string COMMENT '用户名',
    mobile              string COMMENT '联系电话',
    goods_id            string COMMENT '训练营id',
    goods_name          string COMMENT '训练营名称',
    goods_name_period   string COMMENT '训练营期次',
    coursetime_d0       int COMMENT 'D0导学课时长',
    total_play_time_d0  int COMMENT 'D0导学课上课时长',
    ifcome0             int COMMENT 'D0导学课是否到课',
    ifok0               int COMMENT 'D0导学课是否完课',
    coursetime_d1       int COMMENT 'D1课程时长',
    total_play_time_d1  int COMMENT 'D1上课时长',
    ifcome1             int COMMENT 'D1是否到课',
    ifok1               int COMMENT 'D1是否完课',
    ifwork1             int COMMENT 'D1是否交作业',
    coursetime_d2       int COMMENT 'D2课程时长',
    total_play_time_d2  int COMMENT 'D2上课时长',
    ifcome2             int COMMENT 'D2是否到课',
    ifok2               int COMMENT 'D2是否完课',
    ifwork2             int COMMENT 'D2是否交作业',
    coursetime_d3       int COMMENT 'D3课程时长',
    total_play_time_d3  int COMMENT 'D3上课时长',
    ifcome3             int COMMENT 'D3是否到课',
    ifok3               int COMMENT 'D3是否完课',
    ifwork3             int COMMENT 'D3是否交作业',
    coursetime_d4       int COMMENT 'D4课程时长',
    total_play_time_d4  int COMMENT 'D4上课时长',
    ifcome4             int COMMENT 'D4是否到课',
    ifok4               int COMMENT 'D4是否完课',
    ifwork4             int COMMENT 'D4是否交作业',
    coursetime_d5       int COMMENT 'D5课程时长',
    total_play_time_d5  int COMMENT 'D5上课时长',
    ifcome5             int COMMENT 'D5是否到课',
    ifok5               int COMMENT 'D5是否完课',
    ifwork5             int COMMENT 'D5是否交作业',
    coursetime_d6       int COMMENT 'D6课程时长',
    total_play_time_d6  int COMMENT 'D6上课时长',
    ifcome6             int COMMENT 'D6是否到课',
    ifok6               int COMMENT 'D6是否完课',
    ifwork6             int COMMENT 'D6是否交作业',
    coursetime_d7       int COMMENT 'D7课程时长',
    total_play_time_d7  int COMMENT 'D7上课时长',
    ifcome7             int COMMENT 'D7是否到课',
    ifok7               int COMMENT 'D7是否完课',
    ifwork7             int COMMENT 'D7是否交作业'--,
)
    COMMENT '培训主题数仓-用户上课记录'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/dw/da/da_course_user_class_records';



SET hive.exec.compress.output=true;
SET mapred.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
SET mapreduce.map.memory.mb=2048;
SET mapreduce.reduce.memory.mb=2048;
SET hive.execution.engine = tez;


WITH base AS
    ( -- 获取基础用户信息
        SELECT member_id
             , xe_id
             , unionid
             , contact_ex_nickname
             , mobile
             , special_id                               goods_id --关联xiaoe_relation的goods_id(大课) 获取resource_id(子课)
             , goods_name
             , SPLIT(SPLIT(goods_name, '】')[0], '【')[1] goods_name_period
             , ifcollect
        FROM dw.dwd_xt_user
        WHERE dt = '${datebuf}')
   , course_raw AS
    ( -- 获取课程时长 -- courseid, courseday, coursetime
        SELECT a.courseid
             , b.courseday
             , a.coursetime
             , a.alive_start_at
        FROM (SELECT xe_id                                                          courseid --小课
                   , UNIX_TIMESTAMP(alive_stop_at) - UNIX_TIMESTAMP(alive_start_at) coursetime
                   , TO_DATE(alive_start_at)                                        alive_start_at
              FROM ods.ods_xiaoe_live
              WHERE is_deleted = 0) a
                 LEFT JOIN
             (SELECT xe_id                                            courseid
                   , REPLACE(REPLACE(label_name, 'L', 'D'), 'A', 'D') courseday
              FROM ods.ods_xiaoe_special
              GROUP BY xe_id
                     , REPLACE(REPLACE(label_name, 'L', 'D'), 'A', 'D')) b
             ON a.courseid = b.courseid
        GROUP BY a.courseid
               , b.courseday
               , a.coursetime
               , a.alive_start_at)
   , course AS
    (SELECT t1.goods_id -- 一天可能有多节课
          , SUM(IF(t3.courseday = 'D0', coursetime, 0))                coursetime_d0
          , SUM(IF(t3.courseday = 'D1', coursetime, 0))                coursetime_d1
          , SUM(IF(t3.courseday = 'D2', coursetime, 0))                coursetime_d2
          , SUM(IF(t3.courseday = 'D3', coursetime, 0))                coursetime_d3
          , SUM(IF(t3.courseday = 'D4', coursetime, 0))                coursetime_d4
          , SUM(IF(t3.courseday = 'D5', coursetime, 0))                coursetime_d5
          , SUM(IF(t3.courseday = 'D6', coursetime, 0))                coursetime_d6
          , SUM(IF(t3.courseday = 'D7', coursetime, 0))                coursetime_d7
          , MIN(IF(t3.courseday = 'D1', alive_start_at, '9999-99-99')) begindt
     FROM (SELECT goods_id
                , resource_id courseid
           FROM ods.ods_xiaoe_relation
           WHERE is_deleted = 0
           GROUP BY goods_id
                  , resource_id) t1
              JOIN course_raw t3 --courseid, courseday, coursetime
                   ON t1.courseid = t3.courseid
     GROUP BY t1.goods_id)
   , watch_raw AS
    (SELECT member_id
          , special_xe_id        goods_id
          , resource_id          courseid
          , MAX(total_play_time) total_play_time
          , MAX(homework_status) homework_status
     FROM ods.ods_xiaoe_special_course
     GROUP BY member_id
            , special_xe_id
            , resource_id)
   , watch AS
    (SELECT member_id
          , goods_id
          , SUM(IF(t3.courseday = 'D0', total_play_time, 0)) total_play_time_d0
          , SUM(IF(t3.courseday = 'D1', total_play_time, 0)) total_play_time_d1
          , SUM(IF(t3.courseday = 'D2', total_play_time, 0)) total_play_time_d2
          , SUM(IF(t3.courseday = 'D3', total_play_time, 0)) total_play_time_d3
          , SUM(IF(t3.courseday = 'D4', total_play_time, 0)) total_play_time_d4
          , SUM(IF(t3.courseday = 'D5', total_play_time, 0)) total_play_time_d5
          , SUM(IF(t3.courseday = 'D6', total_play_time, 0)) total_play_time_d6
          , SUM(IF(t3.courseday = 'D7', total_play_time, 0)) total_play_time_d7
          , SUM(IF(t3.courseday = 'D1', homework_status, 0)) homework_status_d1
          , SUM(IF(t3.courseday = 'D2', homework_status, 0)) homework_status_d2
          , SUM(IF(t3.courseday = 'D3', homework_status, 0)) homework_status_d3
          , SUM(IF(t3.courseday = 'D4', homework_status, 0)) homework_status_d4
          , SUM(IF(t3.courseday = 'D5', homework_status, 0)) homework_status_d5
          , SUM(IF(t3.courseday = 'D6', homework_status, 0)) homework_status_d6
          , SUM(IF(t3.courseday = 'D7', homework_status, 0)) homework_status_d7 --1未完成，2已完成
     FROM (SELECT t1.member_id
                , t1.goods_id
                , t1.courseid
                , t1.total_play_time
                , t1.homework_status
                , t2.courseday
                , t2.coursetime
           FROM watch_raw t1
                    JOIN course_raw t2
                         ON t1.courseid = t2.courseid
           GROUP BY t1.member_id
                  , t1.goods_id
                  , t1.courseid
                  , t1.total_play_time
                  , t1.homework_status
                  , t2.courseday
                  , t2.coursetime) t3
     GROUP BY member_id
            , goods_id)
   , r1 AS
    (-- 到课完课
        SELECT t1.member_id
             , t1.xe_id
             , t1.unionid
             , t1.contact_ex_nickname
             , t1.mobile
             , t1.goods_id
             , t1.goods_name
             , t1.goods_name_period
             , ifcollect
             , t2.coursetime_d0                                                                  -- 课程时长 coursetime_d0, total_play_time_d0, ifcome0, ifcome0, coursetime_d1, total_play_time_d1, ifcome1, ifok1, ifwork1, begindt
             , t3.total_play_time_d0                                                             -- d0观看时长
             , IF(total_play_time_d0 >= 60, 1, 0)                                        ifcome0 -- d0到课
             , IF(total_play_time_d0 >= coursetime_d0 * 0.6 AND coursetime_d0 > 0, 1, 0) ifok0   -- d0完课
             , t2.coursetime_d1
             , t3.total_play_time_d1
             , IF(total_play_time_d1 >= 60, 1, 0)                                        ifcome1
             , IF(total_play_time_d1 >= coursetime_d1 * 0.6 AND coursetime_d1 > 0, 1, 0) ifok1
             , IF(homework_status_d1 = 2, 1, 0)                                          ifwork1
             , t2.coursetime_d2
             , t3.total_play_time_d2
             , IF(total_play_time_d2 >= 60, 1, 0)                                        ifcome2
             , IF(total_play_time_d2 >= coursetime_d2 * 0.6 AND coursetime_d2 > 0, 1, 0) ifok2
             , IF(homework_status_d2 = 2, 1, 0)                                          ifwork2
             , t2.coursetime_d3
             , t3.total_play_time_d3
             , IF(total_play_time_d3 >= 60, 1, 0)                                        ifcome3
             , IF(total_play_time_d3 >= coursetime_d3 * 0.6 AND coursetime_d3 > 0, 1, 0) ifok3
             , IF(homework_status_d3 = 2, 1, 0)                                          ifwork3
             , t2.coursetime_d4
             , t3.total_play_time_d4
             , IF(total_play_time_d4 >= 60, 1, 0)                                        ifcome4
             , IF(total_play_time_d4 >= coursetime_d4 * 0.6 AND coursetime_d4 > 0, 1, 0) ifok4
             , IF(homework_status_d4 = 2, 1, 0)                                          ifwork4
             , t2.coursetime_d5
             , t3.total_play_time_d5
             , IF(total_play_time_d5 >= 60, 1, 0)                                        ifcome5
             , IF(total_play_time_d5 >= coursetime_d5 * 0.6 AND coursetime_d5 > 0, 1, 0) ifok5
             , IF(homework_status_d5 = 2, 1, 0)                                          ifwork5
             , t2.coursetime_d6
             , t3.total_play_time_d6
             , IF(total_play_time_d6 >= 60, 1, 0)                                        ifcome6
             , IF(total_play_time_d6 >= coursetime_d6 * 0.6 AND coursetime_d6 > 0, 1, 0) ifok6
             , IF(homework_status_d6 = 2, 1, 0)                                          ifwork6
             , t2.coursetime_d7
             , t3.total_play_time_d7
             , IF(total_play_time_d7 >= 60, 1, 0)                                        ifcome7
             , IF(total_play_time_d7 >= coursetime_d7 * 0.6 AND coursetime_d7 > 0, 1, 0) ifok7
             , IF(homework_status_d7 = 2, 1, 0)                                          ifwork7
             , t2.begindt
        FROM base t1 --id, xe_id, unionid, contact_ex_nickname, mobile, goods_id, goods_name, goods_name_period
                 LEFT JOIN course t2 -- 增加begindt
                           ON t1.goods_id = t2.goods_id
                 LEFT JOIN watch t3
                           ON t1.member_id = t3.member_id AND t1.goods_id = t3.goods_id)

INSERT
OVERWRITE
TABLE
da.da_course_user_class_records
PARTITION
(
dt = '${datebuf}'
)
SELECT member_id
     , xe_id
     , unionid
     , contact_ex_nickname
     , mobile
     , goods_id
     , goods_name
     , goods_name_period
     , coursetime_d0
     , total_play_time_d0
     , ifcome0
     , ifok0
     , coursetime_d1
     , total_play_time_d1
     , ifcome1
     , ifok1
     , ifwork1
     , coursetime_d2
     , total_play_time_d2
     , ifcome2
     , ifok2
     , ifwork2
     , coursetime_d3
     , total_play_time_d3
     , ifcome3
     , ifok3
     , ifwork3
     , coursetime_d4
     , total_play_time_d4
     , ifcome4
     , ifok4
     , ifwork4
     , coursetime_d5
     , total_play_time_d5
     , ifcome5
     , ifok5
     , ifwork5
     , coursetime_d6
     , total_play_time_d6
     , ifcome6
     , ifok6
     , ifwork6
     , coursetime_d7
     , total_play_time_d7
     , ifcome7
     , ifok7
     , ifwork7
FROM r1
GROUP BY member_id
       , xe_id
       , unionid
       , contact_ex_nickname
       , mobile
       , goods_id
       , goods_name
       , goods_name_period
       , coursetime_d0
       , total_play_time_d0
       , ifcome0
       , ifok0
       , coursetime_d1
       , total_play_time_d1
       , ifcome1
       , ifok1
       , ifwork1
       , coursetime_d2
       , total_play_time_d2
       , ifcome2
       , ifok2
       , ifwork2
       , coursetime_d3
       , total_play_time_d3
       , ifcome3
       , ifok3
       , ifwork3
       , coursetime_d4
       , total_play_time_d4
       , ifcome4
       , ifok4
       , ifwork4
       , coursetime_d5
       , total_play_time_d5
       , ifcome5
       , ifok5
       , ifwork5
       , coursetime_d6
       , total_play_time_d6
       , ifcome6
       , ifok6
       , ifwork6
       , coursetime_d7
       , total_play_time_d7
       , ifcome7
       , ifok7
       , ifwork7
;



DFS -touchz /dw/da/da_course_user_class_records/dt=${datebuf}/_SUCCESS;


