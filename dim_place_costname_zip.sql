
CREATE TABLE IF NOT EXISTS dwd.dim_place_costname_zip
(
    id                  int COMMENT 'ID自增',
    name_type           string COMMENT '类型(NEW为新增,MODIFIED为修改)',
    main_account        string COMMENT '主体',
    open_agent          string COMMENT '开户代理商',
    operation_agent     string COMMENT '运营代理商',
    material_agent      string COMMENT '素材代理商',
    ad_method           string COMMENT '投放方式',
    d_cat               string COMMENT '品类',
    d_platform          string COMMENT '渠道',
    d_pos               string COMMENT '版位',
    age                 string COMMENT '年龄',
    price               string COMMENT '价格',
    collect_phone_type  string COMMENT '收集手机号类型',
    d_linktype          string COMMENT '链路类型',
    sort_number         string COMMENT '账户序号',
    full_version_name   string COMMENT '完整版名称',
    simple_version_name string COMMENT '简单版名称',
    cost_id             string COMMENT '广告账户id',
    old_name            string COMMENT '旧名称',
    operator_man        string COMMENT '操作人',
    start_time          string COMMENT '生效时间',
    end_time            string COMMENT '失效时间'
)
    COMMENT 'dwd层-历史全部账户名称拉链表'
    STORED AS orcfile;


DROP TABLE IF EXISTS aaa;
CREATE TEMPORARY TABLE aaa AS
SELECT id
     , name_type
     , main_account
     , open_agent
     , operation_agent
     , material_agent
     , ad_method
     , d_cat
     , d_platform
     , d_pos
     , age
     , price
     , collect_phone_type
     , d_linktype
     , sort_number
     , full_version_name
     , simple_version_name
     , cost_id
     , old_name
     , operator_man
     , DATE_FORMAT(updated_at, 'yyyy-MM-dd HH:mm:ss') as updated_at
FROM dwd.dim_place_costname_change_record
WHERE id IS NOT NULL
-- 使用union进行去重
UNION
SELECT id
     , name_type
     , main_account
     , open_agent
     , operation_agent
     , material_agent
     , ad_method
     , d_cat
     , d_platform
     , d_pos
     , age
     , price
     , collect_phone_type
     , d_linktype
     , sort_number
     , full_version_name
     , simple_version_name
     , cost_id
     , old_name
     , operator_man
     , DATE_FORMAT(updated_at, 'yyyy-MM-dd HH:mm:ss') as updated_at
FROM ods.ods_dim_place_costname
;



INSERT OVERWRITE TABLE dwd.dim_place_costname_zip
SELECT id
     , name_type
     , main_account
     , open_agent
     , operation_agent
     , material_agent
     , ad_method
     , d_cat
     , d_platform
     , d_pos
     , age
     , price
     , collect_phone_type
     , d_linktype
     , sort_number
     , full_version_name
     , simple_version_name
     , cost_id
     , old_name
     , operator_man
     , updated_at               AS start_time
     , IF(LAG(updated_at) OVER (PARTITION BY id ORDER BY updated_at DESC ) IS NOT NULL,
          LAG(updated_at) OVER (PARTITION BY id ORDER BY updated_at DESC ),
          '9999-01-01 00:00:00') AS end_time --下一次时间
FROM (SELECT id
           , name_type
           , main_account
           , open_agent
           , operation_agent
           , material_agent
           , ad_method
           , d_cat
           , d_platform
           , d_pos
           , age
           , price
           , collect_phone_type
           , d_linktype
           , sort_number
           , full_version_name
           , simple_version_name
           , cost_id
           , old_name
           , operator_man
           , updated_at
      FROM aaa) a
;