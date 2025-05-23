CREATE TABLE if NOT EXISTS dim.dim_crm_wxwork_corps
(
    id         bigint,
    name       string,
    corpid     string,
    secret     string,
    creator_id bigint,
    created_at timestamp,
    updated_at timestamp,
    deleted_at timestamp
) COMMENT '' STORED AS ORC;


INSERT OVERWRITE TABLE dim.dim_crm_wxwork_corps
SELECT id
     , name
     , corpid
     , secret
     , creator_id
     , from_unixtime(created_at) as created_at
     , from_unixtime(updated_at) as updated_at
     , from_unixtime(deleted_at) as deleted_at
FROM ods.ods_crm_wxwork_corps;
