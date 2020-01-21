import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


def create_e_models_prdns_table(fh):
    """
    """
    fh.write(
        f'''
CREATE TABLE {table_names.e_models_prdns} AS
SELECT DISTINCT {columns.prdn.name}
FROM {table_names.ein_data}
INTERSECT
SELECT DISTINCT {columns.prdn.name}
FROM {table_names.pik_data};

CREATE INDEX
    e_models_prdn_idx
ON
    {table_names.e_models_prdns}(
        {columns.prdn.name}
    );

-- Only want PRDNs with any assignee and inventor information that
-- are not A models
DELETE FROM {table_names.e_models_prdns}
WHERE {columns.prdn.name} IN (
    SELECT DISTINCT {columns.prdn.name}
    FROM {table_names.e_model_info}
);
    ''')


def create_e1_models_table(fh):
    """
    """
    fh.write(
        f'''
DROP TABLE IF EXISTS {table_names.e_models};
CREATE TABLE {table_names.e_models} AS
SELECT DISTINCT
    {table_names.ein_data}.{columns.prdn.name},
    {table_names.ein_data}.{columns.assg_seq.name},
    {table_names.ein_data}.{columns.firmid.name},
    {table_names.ein_data}.{columns.grant_yr.name},
    {table_names.ein_data}.{columns.cw_yr.name},
    abs({table_names.ein_data}.{columns.cw_yr.name} - {table_names.ein_data}.{columns.grant_yr.name}) AS {columns.abs_yr_diff.name},
    ({table_names.ein_data}.{columns.cw_yr.name} - {table_names.ein_data}.{columns.grant_yr.name}) AS {columns.yr_diff.name}
FROM
    {table_names.ein_data}
WHERE
    {columns.firmid.name} != '';

CREATE INDEX
    e_models_idx
ON
    {table_names.e_models}(
        {columns.prdn.name},
        {columns.assg_seq.name},
        {columns.yr_diff.name}
    );

DROP TABLE IF EXISTS firmid_count;
CREATE TABLE firmid_count
AS
SELECT
    {columns.prdn.name},
    {columns.assg_seq.name},
    COUNT(DISTINCT {columns.firmid.name}) AS firmid_count
FROM
    {table_names.e_models}
GROUP BY
    {columns.assg_seq.name},
    {columns.prdn.name};

CREATE INDEX firmid_count_p_as
ON firmid_count(
        {columns.prdn.name},
        {columns.assg_seq.name}
    );

DELETE FROM {table_names.e_models}
WHERE EXISTS (
    SELECT *
    FROM firmid_count
    WHERE
        firmid_count.firmid_count > 1 AND
        {table_names.e_models}.{columns.assg_seq.name} = firmid_count.{columns.assg_seq.name} AND
        {table_names.e_models}.{columns.prdn.name} = firmid_count.{columns.prdn.name}
);

DROP TABLE IF EXISTS {table_names.e_model_subquery};
CREATE TABLE {table_names.e_model_subquery}
AS
-- uses a window function to order by closest to grant year in window ( 0 , -1, 1, -2, 2)
SELECT
    {table_names.e_models}.{columns.prdn.name},
    {table_names.e_models}.{columns.assg_seq.name},
    {table_names.e_models}.{columns.firmid.name},
    {table_names.e_models}.{columns.grant_yr.name},
    {table_names.e_models}.{columns.cw_yr.name},
    RANK() OVER (
        PARTITION BY
            {table_names.e_models}.{columns.prdn.name},
            {table_names.e_models}.{columns.assg_seq.name}
        ORDER BY
            {table_names.e_models}.{columns.abs_yr_diff.name},
            {table_names.e_models}.{columns.yr_diff.name}
    ) AS rnk
FROM
    {table_names.e_models};

CREATE INDEX {table_names.e_model_subquery}_r_p
ON {table_names.e_model_subquery} (
    rnk,
    {columns.prdn.name},
    {columns.assg_seq.name});

-- The actual models
DROP TABLE IF EXISTS {table_names.e_final_models};
CREATE TABLE {table_names.e_final_models} AS
SELECT
    {table_names.e_model_subquery}.{columns.prdn.name},
    {table_names.assignee_info}.{columns.assg_seq.name},
    {table_names.e_model_subquery}.{columns.firmid.name},
    {table_names.prdn_metadata}.{columns.app_yr.name},
    {table_names.e_model_subquery}.{columns.grant_yr.name},
    {table_names.assignee_info}.{columns.assg_type.name},
    {table_names.assignee_info}.{columns.assg_st.name},
    {table_names.assignee_info}.{columns.assg_ctry.name},
    0 AS {columns.us_assg_flag.name},
    0 AS {columns.foreign_assg_flag.name},
    {table_names.prdn_metadata}.{columns.us_inv_flag.name},
    {table_names.prdn_metadata}.{columns.num_assg.name},
    {table_names.e_model_subquery}.{columns.cw_yr.name},
    "" AS {columns.emp_yr.name},
    "E1" AS {columns.model.name},
    "" AS {columns.uniq_firmid.name},
    "" AS {columns.num_inv.name}
FROM
    {table_names.assignee_info},
    {table_names.prdn_metadata},
    {table_names.e_model_subquery}
WHERE
    {table_names.e_model_subquery}.rnk = 1 AND
    {table_names.e_model_subquery}.{columns.prdn.name} = {table_names.assignee_info}.{columns.prdn.name} AND
    {table_names.e_model_subquery}.{columns.prdn.name} = {table_names.prdn_metadata}.{columns.prdn.name} AND
    {table_names.e_model_subquery}.{columns.assg_seq.name} = {table_names.assignee_info}.{columns.assg_seq.name};

-- a state => US assignee
UPDATE {table_names.e_final_models}
SET {columns.us_assg_flag.name} = 1
WHERE {columns.assg_st.name} != "";
-- no state + country => foreign assignee
UPDATE {table_names.e_final_models}
SET {columns.foreign_assg_flag.name} = 1
WHERE
    {columns.us_assg_flag.name} != 1 AND
    {columns.assg_ctry.name} != "";
    ''')


def create_e2_models_table(fh):
    """
    """
    fh.write(
        f'''
DROP TABLE IF EXISTS {table_names.e_models};
CREATE TABLE {table_names.e_models} AS
SELECT DISTINCT
    {table_names.pik_data}.{columns.prdn.name},
    {table_names.pik_data}.{columns.firmid.name},
    {table_names.pik_data}.{columns.app_yr.name},
    {table_names.pik_data}.{columns.emp_yr.name},
    abs({table_names.pik_data}.{columns.emp_yr.name} - {table_names.pik_data}.{columns.app_yr.name}) AS {columns.abs_yr_diff.name},
    ({table_names.pik_data}.{columns.emp_yr.name} - {table_names.pik_data}.{columns.app_yr.name}) AS {columns.yr_diff.name}
FROM
    {table_names.pik_data}
WHERE
    {columns.firmid.name} != '';

CREATE INDEX
    e_models_idx
ON
    {table_names.e_models}(
        {columns.prdn.name},
        {columns.yr_diff.name}
    );

DROP TABLE IF EXISTS firmid_count;
CREATE TABLE firmid_count
AS
SELECT
    {columns.prdn.name},
    COUNT(DISTINCT {columns.firmid.name}) AS firmid_count
FROM
    {table_names.e_models}
GROUP BY
    {columns.prdn.name};

CREATE INDEX firmid_count_p_as
ON firmid_count(
        {columns.prdn.name}
    );

DELETE FROM {table_names.e_models}
WHERE EXISTS (
    SELECT *
    FROM firmid_count
    WHERE
        firmid_count.firmid_count > 1 AND
        {table_names.e_models}.{columns.prdn.name} = firmid_count.{columns.prdn.name}
);

DROP TABLE IF EXISTS {table_names.e_model_subquery};
CREATE TABLE {table_names.e_model_subquery}
AS
-- uses a window function to order by closest to grant year in window ( 0 , -1, 1, -2, 2)
SELECT
    {table_names.e_models}.{columns.prdn.name},
    {table_names.e_models}.{columns.firmid.name},
    {table_names.e_models}.{columns.app_yr.name},
    {table_names.e_models}.{columns.emp_yr.name},
    RANK() OVER (
        PARTITION BY
            {table_names.e_models}.{columns.prdn.name}
        ORDER BY
            {table_names.e_models}.{columns.abs_yr_diff.name},
            {table_names.e_models}.{columns.yr_diff.name}
    ) AS rnk
FROM
    {table_names.e_models};

CREATE INDEX {table_names.e_model_subquery}_r_p
ON {table_names.e_model_subquery} (
    rnk,
    {columns.prdn.name}
);

-- The actual models
DROP TABLE IF EXISTS {table_names.e_final_models};
CREATE TABLE {table_names.e_final_models} AS
SELECT
    {table_names.e_model_subquery}.{columns.prdn.name},
    {table_names.assignee_info}.{columns.assg_seq.name},
    {table_names.e_model_subquery}.{columns.firmid.name},
    {table_names.prdn_metadata}.{columns.app_yr.name},
    {table_names.assignee_info}.{columns.grant_yr.name},
    {table_names.assignee_info}.{columns.assg_type.name},
    {table_names.assignee_info}.{columns.assg_st.name},
    {table_names.assignee_info}.{columns.assg_ctry.name},
    0 AS {columns.us_assg_flag.name},
    0 AS {columns.foreign_assg_flag.name},
    {table_names.prdn_metadata}.{columns.us_inv_flag.name},
    {table_names.prdn_metadata}.{columns.num_assg.name},
    "" AS {columns.cw_yr.name}
    {table_names.e_model_subquery}.{columns.emp_yr.name},
    "E2" AS {columns.model.name},
    "" AS {columns.uniq_firmid.name},
    "" AS {columns.num_inv.name}
FROM
    {table_names.assignee_info},
    {table_names.prdn_metadata},
    {table_names.e_model_subquery}
WHERE
    {table_names.e_model_subquery}.rnk = 1 AND
    {table_names.e_model_subquery}.{columns.prdn.name} = {table_names.assignee_info}.{columns.prdn.name} AND
    {table_names.e_model_subquery}.{columns.prdn.name} = {table_names.prdn_metadata}.{columns.prdn.name};

-- a state => US assignee
UPDATE {table_names.e_final_models}
SET {columns.us_assg_flag.name} = 1
WHERE {columns.assg_st.name} != "";
-- no state + country => foreign assignee
UPDATE {table_names.e_final_models}
SET {columns.foreign_assg_flag.name} = 1
WHERE
    {columns.us_assg_flag.name} != 1 AND
    {columns.assg_ctry.name} != "";
    ''')


def generate_e_model_sql_script(sql_script_fn):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        shared_code.in_data_tables(f, 'E')
        create_e_models_prdns_table(f)
        prep_for_e1_models(f)
        create_e1_models_table(f)
        shared_code.output_distinct_data(f, f'{table_names.e_final_models}', f'{file_names.e1_models}')
        prep_for_e2_models(f)
        create_e2_models_table(f)
        shared_code.output_distinct_data(f, f'{table_names.e_final_models}', f'{file_names.e2_models}')


def prep_for_e1_models(fh):
    """
    """
    fh.write(
        f'''
DELETE FROM {table_names.ein_data}
WHERE {columns.prdn.name} NOT IN (
    SELECT {columns.prdn.name}
    FROM {table_names.e_models_prdns}
);
    ''')


def prep_for_e2_models(fh):
    """
    """
    fh.write(
        f'''
DELETE FROM {table_names.pik_data}
WHERE {columns.prdn.name} NOT IN (
    SELECT DISTINCT {columns.prdn.name}
    FROM {table_names.e_models_prdns}
);

DELETE FROM {table_names.pik_data}
WHERE {columns.prdn.name} IN (
    SELECT DISTINCT {columns.prdn.name}
    FROM {table_names.e_final_models}
);
    ''')
