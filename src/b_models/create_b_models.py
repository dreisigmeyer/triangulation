import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


def clean_b_models_table(fh):
    """

    """
    fh.write(
        f'''
WITH subquery (
    {columns.prdn.name},
    {columns.assg_seq.name}
) AS
(
    SELECT
        {table_names.b_models}.{columns.prdn.name},
        {table_names.b_models}.{columns.assg_seq.name}
    FROM
        {table_names.b_models},
        {table_names.b_model_info}
    WHERE
        {table_names.b_models}.{columns.firmid.name} = {table_names.b_model_info}.{columns.firmid.name} AND
        {table_names.b_models}.{columns.cw_yr.name} = {table_names.b_model_info}.{columns.cw_yr.name}
)
DELETE FROM {table_names.b_models}
WHERE EXISTS (
    SELECT *
    FROM subquery
    WHERE
        {table_names.b_models}.{columns.prdn.name} = subquery.{columns.prdn.name} AND
        {table_names.b_models}.{columns.assg_seq.name} = subquery.{columns.assg_seq.name}
);
    ''')


def create_b_models_table(fh):
    """
    """
    fh.write(
        f'''
CREATE TABLE {table_names.b_models} AS
SELECT DISTINCT
    {table_names.ein_data}.{columns.prdn.name},
    {table_names.ein_data}.{columns.assg_seq.name},
    {table_names.ein_data}.{columns.firmid.name},
    {table_names.ein_data}.{columns.cw_yr.name},
    {table_names.ein_data}.{columns.grant_yr.name},
    abs({table_names.ein_data}.{columns.cw_yr.name} - {table_names.ein_data}.{columns.grant_yr.name}) AS {columns.abs_yr_diff.name},
    ({table_names.ein_data}.{columns.cw_yr.name} - {table_names.ein_data}.{columns.grant_yr.name}) AS {columns.yr_diff.name}
FROM
    {table_names.ein_data},
    (
        SELECT
            {columns.prdn.name},
            {columns.assg_seq.name},
            min({columns.pass_num.name}) AS min_pass_num
        FROM {table_names.ein_data}
        GROUP BY {columns.prdn.name}, {columns.assg_seq.name}
    ) AS subquery1
WHERE
    {table_names.ein_data}.{columns.firmid.name} != '' AND
    {table_names.ein_data}.{columns.prdn.name} = subquery1.{columns.prdn.name} AND
    {table_names.ein_data}.{columns.assg_seq.name} = subquery1.{columns.assg_seq.name} AND
    {table_names.ein_data}.{columns.pass_num.name} = subquery1.min_pass_num;


-- Only want PRDNs without any inventor information: These are the ones that never
-- had a chance to be A models when they grew up.
DELETE FROM {table_names.b_models}
WHERE {columns.prdn.name} IN (
    SELECT DISTINCT {columns.prdn.name}
    FROM {table_names.pik_data}
);
    ''')


def create_bK_models_table(fh, model):
    """
    model is 'B1' or 'B2'
    Uses a window function and requires SQLite >=v3.25.0
    """
    if model == 'B1':
        tbl_name = table_names.b1_models
    else:
        tbl_name = table_names.b2_models

    fh.write(
        f'''
-- CTE tables
DROP TABLE IF EXISTS subquery1;
CREATE TABLE subquery1
AS
-- uses a window function to order by closest to grant year in window ( 0 , -1, 1, -2, 2)
SELECT
    {table_names.b_models}.{columns.prdn.name},
    {table_names.b_models}.{columns.assg_seq.name},
    {table_names.b_models}.{columns.firmid.name},
    {table_names.b_models}.{columns.grant_yr.name},
    {table_names.b_models}.{columns.cw_yr.name},
    RANK() OVER (
        PARTITION BY
            {table_names.b_models}.{columns.prdn.name},
            {table_names.b_models}.{columns.assg_seq.name}
        ORDER BY
            {table_names.b_models}.{columns.abs_yr_diff.name},
            {table_names.b_models}.{columns.yr_diff.name}
    ) AS rnk
FROM
    {table_names.b_models}''')
    if model == 'B1':
        fh.write(
            f''',
    {table_names.b_model_info}
WHERE
    {table_names.b_models}.{columns.firmid.name} = {table_names.b_model_info}.{columns.firmid.name} AND
    {table_names.b_models}.{columns.cw_yr.name} = {table_names.b_model_info}.{columns.cw_yr.name};
CREATE INDEX subquery1_r_p_as
ON subquery1 (rnk, {columns.prdn.name}, {columns.assg_seq.name});
''')
    fh.write(
        f'''
DROP TABLE IF EXISTS firmid_count;
CREATE TABLE firmid_count
AS
-- only want prdn+assg_seq with a unique firmid
SELECT
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.firmid.name},
    {columns.grant_yr.name},
    {columns.cw_yr.name},
    COUNT(DISTINCT {columns.firmid.name}) AS firmid_count
FROM
    subquery1
WHERE
    rnk = 1
GROUP BY
    {columns.prdn.name},
    {columns.assg_seq.name};
CREATE INDEX firmid_count_fc_p_as
ON firmid_count (firmid_count, {columns.prdn.name}, {columns.assg_seq.name});

-- The actual models
CREATE TABLE {tbl_name} AS
SELECT
    firmid_count.{columns.prdn.name},
    firmid_count.{columns.assg_seq.name},
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
    firmid_count.{columns.cw_yr.name},
    "" AS {columns.emp_yr.name},
    "{model}" AS {columns.model.name},
    "" AS {columns.uniq_firmid.name},
    "" AS {columns.num_inv.name}
FROM
    {table_names.assignee_info},
    {table_names.prdn_metadata},
    firmid_count
WHERE
    firmid_count.firmid_count = 1 AND
    firmid_count.{columns.prdn.name} = {table_names.assignee_info}.{columns.prdn.name} AND
    firmid_count.{columns.assg_seq.name} = {table_names.assignee_info}.{columns.assg_seq.name} AND
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


def generate_b_model_sql_script(sql_script_fn):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        create_b_models_table(f)
        create_bK_models_table(f, 'B1')
        shared_code.output_distinct_data(f, f'{table_names.b1_models}', f'{file_names.b1_models}')
        clean_b_models_table(f)
        create_bK_models_table(f, 'B2')
        shared_code.output_distinct_data(f, f'{table_names.b2_models}', f'{file_names.b2_models}')
