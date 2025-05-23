CREATE TABLE IF NOT EXISTS dws.dws_ai_sales_dt
(
    id              bigint COMMENT 'crm_projects表ID',
    name            string COMMENT 'crm_projects表name',
    xe_app_id       string COMMENT '业务线',
    speical_id      string COMMENT '期次ID',
    goods_name      string COMMENT '期次',
    cat             string COMMENT '品类',
    corp_userid     string COMMENT '员工企业微信user_id',
    ai_version      int COMMENT 'AI版本',
    sales_id        int COMMENT 'place_sales表id',
    sales_name      string COMMENT 'place_sales表name',
    sales_real_name string COMMENT '销售真实姓名',
    corp_id         string COMMENT '企业微信主体ID',
    corp_name       string COMMENT '企业微信主体name',
    crm_sale_name   string COMMENT 'CRM销售名称',
    ai_type         string COMMENT '分辨AI类型'
) COMMENT 'DWS层AI销售清单对应CRM销售名字表'
    PARTITIONED BY (dt string COMMENT '分区日期')
    STORED AS ORC;



INSERT OVERWRITE TABLE dws.dws_ai_sales_dt PARTITION (dt = '${datebuf}')
SELECT id
     , name
     , business                                           AS xe_app_id
     , speical_id
     , goods_name
     , cat
     , corp_userid
     , ai_version
     , sales_id
     , sales_name
     , sales_real_name
     , corpid                                             AS corp_id
     , corp_name
     , CONCAT(sales_name, '(', sales_id, ')_', corp_name) AS crm_sale_name
     , CASE SUBSTR(goods_name, 2, 6) >= '250223'
           WHEN ai_version IN (1, 3, 7,16) THEN 'A'
           WHEN ai_version in (6,11) THEN 'B'
           ELSE '其他'
    END                                                   AS ai_type
FROM (SELECT a.id
           , a.name
           , a.business
           , a.training_camp_id              AS speical_id
           , xs.goods_name
           , xs.cat
           , a.userids                       AS corp_userid
           , a.ai_version
           , b.id                            AS sales_id
           , b.name                          AS sales_name
           , SPLIT(ps.clean_name, '_')[0]    AS sales_real_name
           , b.corpid
           , REPLACE(c.name, '小糖乐学', '') AS corp_name
      FROM (SELECT * FROM dwd.dwd_crm_projects_dt WHERE dt = '${datebuf}') a
               -- innerjoin去除测试
               INNER JOIN
           dwd.dwd_xiaoe_special xs
           ON a.training_camp_id = xs.special_id
               -- innerjoin去除测试
               INNER JOIN
           dwd.dwd_place_sales b
           ON a.userids = b.corp_userid
               LEFT JOIN
           dim.dim_crm_wxwork_corps c
           ON b.corpid = c.corpid
               LEFT JOIN
           dwd.dwd_place_sales ps
           ON ps.id = b.id

--              if(substr(goods_name,2,6) ) a.creator_id in ('978','1018')  --陈雪萍和中哲

         -- 约定如果结束时间大于第四天的21:30才算正常，否则算测试AI员工
--       WHERE end_time IS NULL
--          OR (a.end_time >=
--              CONCAT(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTR(XS.goods_name, 2, 6), 'yyMMdd')), 3), ' 21:30:00'))
     ) aa