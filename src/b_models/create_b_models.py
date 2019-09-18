import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


def b_model_header(fh):
    """

    """
    fh.write(
        f'''
.mode csv
pragma temp_store = MEMORY;
    ''')


def clean_b_models_table(fh):
    """

    """
    fh.write(
        f'''
WITH subquery AS (
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
)
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
    {table_names.ein_data}.{columns.grant_yr.name}
FROM
    {table_names.ein_data},
    (
        SELECT
            {columns.prdn.name},
            {columns.assg_seq.name},
            min({columns.pass_num.name})
        FROM {table_names.ein_data}
        GROUP BY {columns.prdn.name}, {columns.assg_seq.name}
    ) AS subquery1
WHERE
    {table_names.ein_data}.{columns.firmid.name} != '' AND
    {table_names.ein_data}.{columns.prdn.name} = subquery1.{columns.prdn.name} AND
    {table_names.ein_data}.{columns.assg_seq.name} = subquery1.{columns.assg_seq.name} AND
    {table_names.ein_data}.{columns.pass_num.name} = subquery1.{columns.pass_num.name};


-- Only want PRDNs without any inventor information: These are the ones that never
-- had a chance to be A models when they grew up.
DELETE FROM {table_names.b_models}
WHERE {columns.prdn.name} IN (
    SELECT DISTINCT {columns.prdn.name}
    FROM {table_names.pik_data}
);
    ''')


def create_b1_models_table(fh):
    """
    Uses a window function and requires SQLite >=v3.25.0
    """
    fh.write(
        f'''
-- CTE tables
WITH subquery1 AS
-- uses a window function to order by closest to grant year in window ( 0 , -1, 1, -2, 2)
(
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
                abs({table_names.b_models}.{columns.cw_yr.name} - {table_names.b_models}.{columns.grant_yr.name}),
                ({table_names.b_models}.{columns.cw_yr.name} - {table_names.b_models}.{columns.grant_yr.name})
        ) AS rnk
    FROM
        {table_names.b_models},
        {table_names.b_model_info}
    WHERE
        {table_names.b_models}.{columns.firmid.name} = {table_names.b_model_info}.{columns.firmid.name} AND
        {table_names.b_models}.{columns.cw_yr.name} = {table_names.b_model_info}.{columns.cw_yr.name}
),
firmid_count AS
-- only want prdn+assg_seq with a unique firmid
(
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
        {columns.assg_seq.name}
)
-- The actual B1 models
CREATE TABLE {table_names.b1_models} AS
SELECT
    firmid_count.{columns.prdn.name},
    "" AS count,
    firmid_count.{columns.assg_seq.name},
    firmid_count.{columns.cw_yr.name},
    "" AS emp_yr,
    firmid_count.{columns.firmid.name},
    firmid_count.{columns.grant_yr.name},
    {table_names.prdn_metadata}.{columns.app_yr.name},
    {table_names.assignee_info}.{columns.assg_ctry.name},
    {table_names.assignee_info}.{columns.assg_st.name},
    {table_names.assignee_info}.{columns.assg_type.name},
    {table_names.prdn_metadata}.{columns.us_inv_flag.name},
    {table_names.prdn_metadata}.{columns.num_assg.name},
    "B1" AS model,
    "" AS uniq_firmid,
FROM
    {table_names.assignee_info},
    {table_names.prdn_metadata},
    firmid_count
WHERE
    firmid_count.firmid_count = 1 AND
    firmid_count.{columns.prdn.name} = {table_names.assignee_info}.{columns.prdn.name} AND
    firmid_count.{columns.assg_seq.name} = {table_names.assignee_info}.{columns.assg_seq.name} AND
    firmid_count.{columns.prdn.name} = {table_names.prdn_metadata}.{columns.prdn.name}
    ''')


def generate_b_model_sql_script(sql_script_fn):
    """

    """
    with open(sql_script_fn, 'w') as f:
        b_model_header(f)
        create_b_models_table(f)
        create_b1_models_table(f)
        shared_code.output_data(f, f'{table_names.b1_models}', f'{file_names.b1_models}')
        clean_b_models_table(f)
        shared_code.output_data(f, f'{table_names.b2_models}', f'{file_names.b2_models}')
