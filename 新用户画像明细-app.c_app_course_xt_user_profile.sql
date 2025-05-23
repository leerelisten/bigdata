SET mapred.job.name="c_app_course_xt_user_profile#${datebuf}";
USE app;
CREATE TABLE IF NOT EXISTS app.c_app_course_xt_user_profile
(
    d_date              string COMMENT '创建日期',        --用户最新的领课时间
    cat                 string COMMENT '品类',
    member_id           int COMMENT '用户ID',
    contact_ex_nickname string COMMENT '用户名',
    phone               string COMMENT '联系电话',
    goods_name          string COMMENT '专栏',
    department          string COMMENT '部门',
    user_group          string COMMENT '组',
    sales_name          string COMMENT '销售姓名',
    cost_id             string COMMENT '广告账户',
    wx_rel_status       string COMMENT '微信关系',
    is_get_ticket       string COMMENT '是否领券',
    xe_id               string COMMENT '小鹅通ID',
    h5_id               int COMMENT 'h5_id',
    ad_department       string COMMENT '投放部门',        --20250313新增投放部门
    platform_name       string COMMENT '渠道名称',
    pos                 string COMMENT '版位',
    price               string COMMENT '价格',
    mobile              string COMMENT '手机号',
    link_type_v2        string COMMENT '链路类型',
    wx_add_time         string COMMENT '加微时间',
    pay_num             string COMMENT '一转订单数(单)',
    pay_sum             string COMMENT '一转GMV(元)',
    collect_time        string COMMENT '问卷填写时间',
    form_name           string COMMENT '问卷名称',
    form_cat            string COMMENT '问卷品类',
    extra               string COMMENT '问卷json',
    sex                 string COMMENT '性别',
    address             string COMMENT '地址',
    city_level          string COMMENT '城市等级',
    age                 string COMMENT '年龄',
    age_level           string COMMENT '年龄层',
    work                string COMMENT '职业',
    taiji_exp           string COMMENT '太极-学习历史',
    taiji_basic         string COMMENT '太极-学习基础',
    taiji_hope          string COMMENT '太极-核心问题',
    taiji_cause         string COMMENT '太极-学习原因',   -- 20240626新增维度
    taiji_interest      string COMMENT '太极-了解和兴趣', -- 20240919新增维度
    taiji_influence     string COMMENT '太极-健康问题的影响',
    is_abroad           string COMMENT '海外/国内'
)
    COMMENT '培训主题数仓-app层-用户画像'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION '/dw/app/c_app_course_xt_user_profile';



CREATE TEMPORARY TABLE base_user AS
SELECT TO_DATE(created_at)                                          AS d_date
     , cat
     , member_id
     , contact_ex_nickname
     , phone
     , REGEXP_REPLACE(TRIM(goods_name), '【2024', '【24')                goods_name
     , department
     , user_group
     , SPLIT(REGEXP_REPLACE(TRIM(sales_name), '[0-9]', ''), '（')[0] AS sales_name
     , CONCAT('id_', COALESCE(cost_id, 'other'))                       cost_id
     , wx_rel_status
     , is_get_ticket
     , xe_id
     , h5_id
     , ad_department
     , platform_name
     , pos
     , price
     , mobile
     , link_type_v2
     , wx_add_time
     , special_id
FROM dw.dwd_xt_user
WHERE dt = '${datebuf}'
  -- -- 25.03.10 由于数据量过大，取最近30天的数据进行展示。

--   AND TO_DATE(created_at) >= DATE_SUB('${datebuf}', 30)
  AND member_status = 1
  AND trade_state IN ('SUCCESS', 'PREPARE')
  AND sales_id > 0
  AND (platform_name != '小糖私域' OR pos != '私域群活码');

-- 20241113 剔除私域群活码

-- 25.03.10 由于数据量过大，取每个品类最近10期数据进行展示。
-- CREATE TEMPORARY TABLE goods_list AS
-- SELECT *
-- FROM (SELECT is_abroad
--            , cat
--            , goods_name
--            , ROW_NUMBER() OVER (PARTITION BY is_abroad,cat ORDER BY SUBSTR(goods_name, 2, 6) DESC) AS rank
--       FROM (SELECT *
--             FROM dwd.dwd_xiaoe_special
--             WHERE order_coefficient IS NULL
--               AND price_high = 0
--             ORDER BY created_at DESC) a) aa
-- WHERE rank <= 10;


INSERT
    OVERWRITE
    TABLE app.c_app_course_xt_user_profile
    PARTITION
    (dt = '${datebuf}')
SELECT t1.d_date
     , t1.cat
     , t1.member_id
     , t1.contact_ex_nickname
     , t1.phone
     , t1.goods_name
     , t1.department
     , t1.user_group
     , t1.sales_name
     , t1.cost_id
     , t1.wx_rel_status
     , t1.is_get_ticket
     , t1.xe_id
     , t1.h5_id
     , t1.ad_department
     , t1.platform_name
     , t1.pos
     , t1.price
     , t1.mobile
     , t1.link_type_v2
     , t1.wx_add_time
     , NVL(t3.pay_num, 0)                      pay_num
     , NVL(t3.pay_sum, 0)                      pay_sum
     , NVL(t2.collect_time, '未填写')          collect_time
     , NVL(t2.form_name, '未填写')             form_name
     , NVL(t2.form_cat, '未填写')              form_cat
     , NVL(t2.extra_original, '未填写')        extra_original
     , NVL(t2.sex, '未填写')                   sex
     , NVL(t2.address, '未填写')               address
     , NVL(t2.city_level, '未填写')            city_level
     , NVL(t2.age, '未填写')                   age
     , NVL(t2.age_level, '未填写')             age_level
     , NVL(t2.work, '未填写')                  work
     , NVL(t2.taiji_exp, '未填写')             taiji_exp
     , NVL(t2.taiji_basic, '未填写')           taiji_basic
     , NVL(t2.taiji_hope, '未填写')            taiji_hope
     , NVL(t2.taiji_cause, '未填写')           taiji_cause
     , NVL(t2.taiji_interest, '未填写')        taiji_interest
     , NVL(t2.taiji_influence, '未填写')       taiji_influence
     , IF(t4.is_abroad = 1, '海外', '国内') AS is_abroad
FROM base_user t1
         -- 25.03.10 由于数据量过大，取每个品类最近10期数据进行展示。
--          INNER JOIN
--      goods_list gl
--      ON gl.goods_name = t1.goods_name
         LEFT JOIN
         (SELECT * FROM dw.dws_sale_questionnaire_day WHERE dt = '${datebuf}') t2
         ON t1.xe_id = t2.xe_id AND t1.cat = t2.form_cat
         LEFT JOIN
         (SELECT * FROM dws.dws_sale_buy_course_day WHERE dt = '${datebuf}') t3
         ON t1.xe_id = t3.user_id AND t1.special_id = t3.owner_class
         LEFT JOIN dwd.dwd_xiaoe_special t4
                   ON t1.special_id = t4.special_id
;

DFS -touchz /dw/app/c_app_course_xt_user_profile/dt=${datebuf}/_SUCCESS;