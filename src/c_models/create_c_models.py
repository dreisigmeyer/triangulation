import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


def clean_c_models_table(fh):
    """

    """
    fh.write(
        f'''
WITH subquery (
    {columns.prdn.name}
) AS
(
    SELECT DISTINCT
        {table_names.c_models}.{columns.prdn.name}
    FROM
        {table_names.c_models},
        {table_names.c_model_info}
    WHERE
        {table_names.c_models}.{columns.pik.name} = {table_names.c_model_info}.{columns.pik.name} AND
        {table_names.c_models}.{columns.emp_yr.name} = {table_names.c_model_info}.{columns.emp_yr.name} AND
        {table_names.c_models}.{columns.firmid.name} = {table_names.c_model_info}.{columns.firmid.name}
)
DELETE FROM {table_names.c_models}
WHERE EXISTS (
    SELECT *
    FROM subquery
    WHERE
        {table_names.c_models}.{columns.prdn.name} = subquery.{columns.prdn.name}
);
    ''')


def create_c_models_table(fh):
    """
    """
    fh.write(
        f'''
CREATE TABLE {table_names.c_models} AS
SELECT DISTINCT
    {table_names.pik_data}.{columns.prdn.name},
    {table_names.pik_data}.{columns.grant_yr.name},
    {table_names.pik_data}.{columns.app_yr.name},
    {table_names.pik_data}.{columns.inv_seq.name},
    {table_names.pik_data}.{columns.pik.name},
    {table_names.pik_data}.{columns.firmid.name},
    {table_names.pik_data}.{columns.emp_yr.name},
    abs({table_names.pik_data}.{columns.emp_yr.name} - {table_names.pik_data}.{columns.app_yr.name}) AS {columns.abs_yr_diff.name},
    ({table_names.pik_data}.{columns.emp_yr.name} - {table_names.pik_data}.{columns.app_yr.name}) AS {columns.yr_diff.name}
FROM
    {table_names.pik_data}
WHERE
    {table_names.pik_data}.{columns.firmid.name} != '';

CREATE INDEX c_model_p_is_ayd_yd
ON {table_names.c_models} (
    {columns.prdn.name},
    {columns.inv_seq.name},
    {columns.abs_yr_diff.name},
    {columns.yr_diff.name}
);

CREATE INDEX
    idx_c_models
ON
    {table_names.c_models}(
        {columns.prdn.name},
        {columns.pik.name},
        {columns.firmid.name},
        {columns.emp_yr.name}
    );

-- Only want PRDNs without any assignee information: These are the ones that never
-- had a chance to be A models when they grew up.
DELETE FROM {table_names.c_models}
WHERE {columns.prdn.name} IN (
    SELECT DISTINCT {columns.prdn.name}
    FROM {table_names.ein_data}
);
    ''')


def create_c1_model_table(fh):
    """
    model is 'C1'
    Uses a window function and requires SQLite >=v3.25.0
    """
    tbl_name = table_names.c1_models
    fh.write(
        f'''
DROP TABLE IF EXISTS subquery1;
CREATE TABLE subquery1
AS
-- uses a window function to order by closest to grant year in window ( 0 , -1, 1, -2, 2)
SELECT
    {table_names.c_models}.{columns.prdn.name},
    {table_names.c_models}.{columns.firmid.name},
    {table_names.c_models}.{columns.emp_yr.name},
    {table_names.c_models}.{columns.grant_yr.name},
    RANK() OVER (
        PARTITION BY
            {table_names.c_models}.{columns.prdn.name}
        ORDER BY
            {table_names.c_models}.{columns.abs_yr_diff.name},
            {table_names.c_models}.{columns.yr_diff.name}
    ) AS rnk
FROM
    {table_names.c_models},
    {table_names.c_model_info}
WHERE
    {table_names.c_models}.{columns.pik.name} = {table_names.c_model_info}.{columns.pik.name} AND
    {table_names.c_models}.{columns.emp_yr.name} = {table_names.c_model_info}.{columns.emp_yr.name} AND
    {table_names.c_models}.{columns.firmid.name} = {table_names.c_model_info}.{columns.firmid.name};

CREATE INDEX subquery1_r_p
ON subquery1 (rnk, {columns.prdn.name});

DROP TABLE IF EXISTS firmid_count;
CREATE TABLE firmid_count
AS
-- only want prdn+assg_seq with a unique firmid
SELECT
    {columns.prdn.name},
    {columns.firmid.name},
    {columns.emp_yr.name},
    {columns.grant_yr.name},
    COUNT(DISTINCT {columns.firmid.name}) AS firmid_count
FROM
    subquery1
WHERE
    rnk = 1
GROUP BY
    {columns.prdn.name};
CREATE INDEX firmid_count_fc_p
ON firmid_count (firmid_count, {columns.prdn.name});

-- The actual models
CREATE TABLE {tbl_name} AS
SELECT
    firmid_count.{columns.prdn.name},
    {table_names.assignee_info}.{columns.assg_seq.name} AS  {columns.assg_seq.name},
    firmid_count.{columns.firmid.name},
    {table_names.prdn_metadata}.{columns.app_yr.name},
    firmid_count.{columns.grant_yr.name},
    {table_names.assignee_info}.{columns.assg_type.name},
    {table_names.assignee_info}.{columns.assg_st.name},
    {table_names.assignee_info}.{columns.assg_ctry.name},
    0 AS {columns.us_assg_flag.name},
    0 AS {columns.foreign_assg_flag.name},
    {table_names.prdn_metadata}.{columns.us_inv_flag.name},
    {table_names.prdn_metadata}.{columns.num_assg.name},
    "" AS {columns.cw_yr.name},
    firmid_count.{columns.emp_yr.name},
    "C1" AS {columns.model.name},
    "" AS {columns.uniq_firmid.name},
    "" AS {columns.num_inv.name}
FROM
    {table_names.assignee_info},
    {table_names.prdn_metadata},
    firmid_count
WHERE
    firmid_count.firmid_count = 1 AND
    firmid_count.{columns.prdn.name} = {table_names.assignee_info}.{columns.prdn.name} AND
    firmid_count.{columns.prdn.name} = {table_names.prdn_metadata}.{columns.prdn.name};

-- a state => US assignee
UPDATE {tbl_name}
SET {columns.us_assg_flag.name} = 1
WHERE {columns.assg_st.name} != "";
-- no state + country => foreign assignee
UPDATE {tbl_name}
SET {columns.foreign_assg_flag.name} = 1
WHERE
    {columns.us_assg_flag.name} != 1 AND
    {columns.assg_ctry.name} != "";
    ''')


def create_c2_model_table(fh):
    """

    """
    tbl_name = table_names.c2_models
    fh.write(
        f'''
CREATE TABLE {table_names.pik_emp_and_app_yr} AS
SELECT *
FROM {table_names.assignee_info}
WHERE
    {table_names.assignee_info}.{columns.emp_yr.name} = {table_names.assignee_info}.{columns.app_yr.name};
    
CREATE INDEX pik_emp_and_app_yr_idx
ON {table_names.pik_emp_and_app_yr} (
    {columns.prdn.name},
    {columns.pik.name},
    {columns.firmid.name}
);

CREATE TABLE c2_models_holder AS
SELECT DISTINCT
    {table_names.c_models}.{columns.prdn.name},
    {table_names.c_models}.{columns.firmid.name}
FROM 
    {table_names.c_models}, 
    {table_names.c_model_info}
WHERE
    {table_names.c_models}.{columns.emp_yr.name} = {table_names.c_model_info}.{columns.emp_yr.name} AND
    {table_names.c_models}.{columns.firmid.name} = {table_names.c_model_info}.{columns.firmid.name};

CREATE INDEX
    temp_idx_c2_models_holder
ON
    c2_models_holder(
        {columns.prdn.name},
        {columns.firmid.name}
    );
    
DELETE FROM c2_models_holder
WHERE c2_models_holder.{columns.prdn.name} IN (
    SELECT subquery_1.{columns.prdn.name}
    FROM (
        SELECT {columns.prdn.name}, count(*) AS counter
        FROM c2_models_holder
        GROUP BY {columns.prdn.name}
    ) subquery_1 
    WHERE subquery_1.counter > 1
);

CREATE TABLE c2_models_pik_data AS
SELECT 
    {table_names.pik_emp_and_app_yr}.{columns.prdn.name},                                                                                                                                                          
    {table_names.pik_emp_and_app_yr}.{columns.app_yr.name},                                                                                                                                                     
    {table_names.pik_emp_and_app_yr}.{columns.inv_seq.name},                                                                                                                                                    
    {table_names.pik_emp_and_app_yr}.{columns.pik.name},                                                                                                                                                           
    {table_names.pik_emp_and_app_yr}.{columns.firmid.name},
    {table_names.pik_emp_and_app_yr}.{columns.grant_yr.name}
FROM 
    {table_names.pik_emp_and_app_yr},
    c2_models_holder
WHERE
    {table_names.pik_emp_and_app_yr}.{columns.prdn.name} = c2_models_holder.{columns.prdn.name} AND
    {table_names.pik_emp_and_app_yr}.{columns.firmid.name} = c2_models_holder.{columns.firmid.name};
    
ALTER TABLE c2_models_pik_data
ADD COLUMN ui_firm_id TEXT;

CREATE INDEX
    temp_idx_c2_models_pik_data
ON
    c2_models_pik_data(
        {columns.prdn.name},
        {columns.inv_seq.name},
        {columns.pik.name},
        {columns.firmid.name},
        ui_firm_id
    );

CREATE TABLE {tbl_name} (
    {columns.prdn.cmd},
    {columns.firmid.cmd},
    {columns.pik.cmd}
    PRIMARY KEY (    
        {columns.prdn.name},
        {columns.firmid.name},
        {columns.pik.name}
    )
);

INSERT OR IGNORE INTO {tbl_name}
    (
        {columns.prdn.name},
        {columns.firmid.name},
        {columns.pik.name}
    )
SELECT DISTINCT
        {columns.prdn.name},
        {columns.firmid.name},
        {columns.pik.name}
FROM 
    c2_models_pik_data;
    
INSERT OR IGNORE INTO {tbl_name}
    (
        {columns.prdn.name},
        {columns.firmid.name},
        {columns.pik.name}
    )
SELECT DISTINCT
    {columns.prdn.name},
    ui_firm_id,
    {columns.pik.name}
FROM 
    c2_models_pik_data;

CREATE TABLE {table_names.c2_models_out} AS
SELECT *, count(*) AS count
FROM {tbl_name}
GROUP BY 
    {columns.prdn.name},
    {columns.pik.name}
HAVING count = 1;

DROP TABLE {tbl_name};

---- this is for the C3 models
CREATE INDEX
    idx_c2_models_out_prdn
ON
    {table_names.c2_models_out}({columns.prdn.name});

CREATE TABLE {tbl_name} AS
SELECT
    {table_names.c2_models_out}.{columns.prdn.name},
    {table_names.assignee_info}.{columns.assg_seq.name} AS  {columns.assg_seq.name},
    {table_names.c2_models_out}.{columns.firmid.name},
    {table_names.prdn_metadata}.{columns.app_yr.name},
    {table_names.prdn_metadata}.{columns.grant_yr.name},
    {table_names.assignee_info}.{columns.assg_type.name},
    {table_names.assignee_info}.{columns.assg_st.name},
    {table_names.assignee_info}.{columns.assg_ctry.name},
    0 AS {columns.us_assg_flag.name},
    0 AS {columns.foreign_assg_flag.name},
    {table_names.prdn_metadata}.{columns.us_inv_flag.name},
    {table_names.prdn_metadata}.{columns.num_assg.name},
    "" AS {columns.cw_yr.name},
    {table_names.prdn_metadata}.{columns.app_yr.name} AS {columns.emp_yr.name},
    "C2" AS {columns.model.name},
    "" AS {columns.uniq_firmid.name},
    "" AS {columns.num_inv.name},
    "" AS {columns.us_assg_flag.name},
    "" AS {columns.foreign_assg_flag.name}
FROM 
    {table_names.c2_models_out},
    {table_names.prdn_metadata},
    {table_names.assignee_info}
WHERE
    {table_names.c2_models_out}.{columns.prdn.name} = {table_names.prdn_metadata}.{columns.prdn.name} AND
    {table_names.c2_models_out}.{columns.prdn.name} = {table_names.assignee_info}.{columns.prdn.name}
ORDER BY
    {table_names.c2_models_out}.{columns.prdn.name};
    ''')


def generate_c_model_sql_script(sql_script_fn):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        shared_code.in_data_tables(f, 'C')
        create_c_models_table(f)
        create_c1_model_table(f)
        shared_code.output_distinct_data(f, f'{table_names.c1_models}', f'{file_names.c1_models}')
        clean_c_models_table(f)
        create_c2_model_table(f)
        shared_code.output_distinct_data(f, f'{table_names.c2_models}', f'{file_names.c2_models}')
