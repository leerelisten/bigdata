CREATE TABLE IF NOT EXISTS dim.dim_place_costname
(
    `id`                  bigint,
    `name_type`           string COMMENT '类型 NEW  MODIFIED',
    `main_account`        string COMMENT '主体',
    `open_agent`          string COMMENT '开户代理商',
    `operation_agent`     string COMMENT '运营代理商',
    `material_agent`      string COMMENT '素材代理商',
    `ad_method`           string COMMENT '投放方式',
    `d_cat`               string COMMENT '品类',
    `d_platform`          string COMMENT '渠道',
    `d_pos`               string COMMENT '版位',
    `age`                 string COMMENT '年龄',
    `price`               string COMMENT '价格',
    `collect_phone_type`  string COMMENT '收集手机号类型',
    `d_linktype`          string COMMENT '链路类型: ',
    `sort_number`         string COMMENT '账户序号',
    `full_version_name`   string COMMENT '完整版名称',
    `simple_version_name` string COMMENT '简单版名称',
    `cost_id`             string COMMENT '广告账户id',
    `old_name`            string COMMENT '旧名称',
    `operator_man`        string COMMENT '操作人',
    `created_at`          timestamp COMMENT '添加时间',
    `updated_at`          timestamp COMMENT '修改时间',
    `area`                string COMMENT '地区：武汉、广州',
    `ad_department`       string COMMENT '投放部门'
) COMMENT '历史全部账户名称表'
    STORED AS ORC;


INSERT OVERWRITE TABLE dim.dim_place_costname
SELECT a.id
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
     , IF(operator_man = '', NULL, operator_man)   AS operator_man
     , created_at
     , updated_at
     , area
     , IF(b.name = '', NULL, b.name) AS ad_department
FROM ods.ods_dim_place_costname a
         LEFT JOIN
     (SELECT id, name
      FROM ods.ods_crm_department_group
      WHERE parentid = 700) b
     ON a.ad_department = b.id