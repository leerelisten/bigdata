CREATE TABLE IF NOT EXISTS dim.dim_crm_product_types
(
    id                INT COMMENT 'id',
    name              STRING COMMENT '商品名称',
    business          STRING COMMENT '业务线',
    order_coefficient FLOAT COMMENT '订单系数',
    order_amount      FLOAT COMMENT '订单金额',
    default_affiliation STRING COMMENT '默认归属',
    tags              STRING COMMENT '标签',
    creator_id        INT COMMENT 'creator id',
    created_at        INT COMMENT 'created time',
    updated_at        INT COMMENT 'updated time',
    deleted_at        INT COMMENT 'delete time',
    allocation_mode   INT COMMENT '分配模式 1 随机 2 定向同班 3与前一班主任保持一致',
    category          STRING COMMENT '品类',
    user_agreement_id INT COMMENT '用户协议ID',
    indate            STRING COMMENT '服务周期',
    conversion_stage  STRING COMMENT '转化阶段'

) COMMENT 'CRM商品类型'
    STORED AS ORC;


INSERT OVERWRITE TABLE dim.dim_crm_product_types
SELECT   id
        ,name
        ,business
        ,order_coefficient
        ,order_amount
        ,default_affiliation
        ,tags
        ,creator_id
        ,created_at
        ,updated_at
        ,deleted_at
        ,allocation_mode
        ,category
        ,user_agreement_id
        ,indate
        ,case when name='太极筑基营' then '二转'
              when name='太极炼气营' then '三转'
              when name='太极联学营' then '三转'
              when name='炼神营线上班' then '四转'
              when name='炼神营全栈班' then '四转'
              when name='炼神营续费班' then '四转'
              when name='筑基营(精简版)' then '二转'
              when name='太极筑基营【海内外同修】' then '二转'
              when name='筑基营精简版【海内外同修】' then '二转'
              when name='太极养正营' then '三转'
              when name='太极炼气营【精选版】' then '三转'
              when name='太极炼气营【海内外同修】' then '三转'
              when name='太极炼气营精简版【海内外同修】' then '三转'
         end as conversion_stage
    FROM ods.ods_crm_product_types;
