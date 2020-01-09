import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


def clean_c_models_table(fh):
    """

    """
    fh.write(
        f'''
CREATE INDEX {table_names.c_model_subquery}_p
ON {table_names.c_model_subquery} (prdn);
CREATE INDEX c_models_p
ON {table_names.c_models} (prdn);

-- Don't consider for a C2 model anything that was considered for a C1 model
DELETE FROM {table_names.c_models}
WHERE EXISTS (
    SELECT *
    FROM {table_names.c_model_subquery}
    WHERE
        {table_names.c_model_subquery}.prdn = {table_names.c_models}.prdn
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
DROP TABLE IF EXISTS {table_names.c_model_subquery};
CREATE TABLE {table_names.c_model_subquery}
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

CREATE INDEX {table_names.c_model_subquery}_r_p
ON {table_names.c_model_subquery} (rnk, {columns.prdn.name});

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
    {table_names.c_model_subquery}
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
    {table_names.assignee_info}.{columns.assg_seq.name},
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
FROM {table_names.pik_data}
WHERE
    {table_names.pik_data}.{columns.emp_yr.name} = {table_names.pik_data}.{columns.app_yr.name};

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
        SELECT {columns.prdn.name},
        count(*) AS counter
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
        {columns.ui_firm_id.name}
    );

INSERT INTO c2_models_pik_data (
    {columns.prdn.name},
    {columns.app_yr.name},
    {columns.inv_seq.name},
    {columns.pik.name},
    {columns.ui_firm_id.name},
    {columns.grant_yr.name}
)
SELECT
    {table_names.pik_emp_and_app_yr}.{columns.prdn.name},
    {table_names.pik_emp_and_app_yr}.{columns.app_yr.name},
    {table_names.pik_emp_and_app_yr}.{columns.inv_seq.name},
    {table_names.pik_emp_and_app_yr}.{columns.pik.name},
    {table_names.pik_emp_and_app_yr}.{columns.firmid.name},
    {table_names.pik_emp_and_app_yr}.{columns.grant_yr.name}
FROM
    {table_names.pik_emp_and_app_yr},
    c2_models_pik_data
WHERE
    {table_names.pik_emp_and_app_yr}.{columns.prdn.name} = c2_models_pik_data.{columns.prdn.name} AND
    {table_names.pik_emp_and_app_yr}.{columns.pik.name} = c2_models_pik_data.{columns.pik.name} AND
    {table_names.pik_emp_and_app_yr}.{columns.firmid.name} != c2_models_pik_data.{columns.firmid.name};


CREATE TABLE {tbl_name} (
    {columns.prdn.cmd},
    {columns.firmid.cmd},
    {columns.pik.cmd},
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
    "" AS {columns.num_inv.name}
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


def create_c3_model_table(fh):
    """

    """
    fh.write(
        f'''
DROP TABLE IF EXISTS {table_names.c_model_subquery};
CREATE TABLE {table_names.c_model_subquery}
AS
SELECT
    {columns.prdn.name},
    {columns.firmid.name},
    {columns.emp_yr.name},
    {columns.app_yr.name},
    COUNT(DISTINCT {columns.firmid.name}) AS counter
FROM (
-- uses a window function to order by closest to grant year in window ( 0 , -1, 1, -2, 2)
    SELECT
        {table_names.c_models}.{columns.prdn.name},
        {table_names.c_models}.{columns.firmid.name},
        {table_names.c_models}.{columns.emp_yr.name},
        {table_names.c_models}.{columns.app_yr.name},
        RANK() OVER (
            PARTITION BY
                {table_names.c_models}.{columns.prdn.name}
            ORDER BY
                {table_names.c_models}.{columns.abs_yr_diff.name},
                {table_names.c_models}.{columns.yr_diff.name}
        ) AS rnk
    FROM
        {table_names.c_models}
)
WHERE
    rnk = 1
GROUP BY
    {columns.prdn.name},
    {columns.emp_yr.name},
    {columns.app_yr.name};

CREATE INDEX
    temp_idx_c3_first_row
ON
    {table_names.c_models}(
        {columns.prdn.name},
        {columns.emp_yr.name},
        {columns.app_yr.name}
    );

CREATE TABLE {table_names.c3_models} AS
SELECT
    {table_names.c_model_subquery}.{columns.prdn.name},
    {table_names.assignee_info}.{columns.assg_seq.name} AS  {columns.assg_seq.name},
    {table_names.c_model_subquery}.{columns.firmid.name},
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
    {table_names.c_model_subquery}.{columns.emp_yr.name},
    "C3" AS {columns.model.name},
    "" AS {columns.uniq_firmid.name},
    "" AS {columns.num_inv.name}
FROM
    {table_names.c_model_subquery},
    {table_names.prdn_metadata},
    {table_names.assignee_info}
WHERE
    {table_names.c_model_subquery}.counter = 1 AND
    {table_names.c_model_subquery}.{columns.prdn.name} = {table_names.assignee_info}.{columns.prdn.name} AND
    {table_names.c_model_subquery}.{columns.prdn.name} = {table_names.prdn_metadata}.{columns.prdn.name}
ORDER BY
    {table_names.c_model_subquery}.{columns.prdn.name};

-- a state => US assignee
UPDATE {table_names.c3_models}
SET {columns.us_assg_flag.name} = 1
WHERE {columns.assg_st.name} != "";
-- no state + country => foreign assignee
UPDATE {table_names.c3_models}
SET {columns.foreign_assg_flag.name} = 1
WHERE
    {columns.us_assg_flag.name} != 1 AND
    {columns.assg_ctry.name} != "";
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
        remake_c_model_table(f)
        create_c3_model_table(f)
        shared_code.output_distinct_data(f, f'{table_names.c3_models}', f'{file_names.c3_models}')


def remake_c_model_table(fh):
    """

    """
    fh.write(
        f'''
DROP TABLE {table_names.c_models};
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
    {table_names.pik_data};


CREATE INDEX
    idx_c_models
ON
    {table_names.c_models}(
        {columns.prdn.name},
        {columns.inv_seq.name},
        {columns.pik.name},
        {columns.firmid.name},
        {columns.emp_yr.name}
    );

DELETE FROM {table_names.c_models}
WHERE {columns.prdn.name} IN (
    SELECT DISTINCT {columns.prdn.name}
    FROM {table_names.ein_data}
);

---- Only delete if a C1 model
DELETE FROM {table_names.c_models}
WHERE {columns.prdn.name} IN (
    SELECT DISTINCT {columns.prdn.name}
    FROM {table_names.c1_models}
);

---- Only delete if a C2 model
DELETE FROM {table_names.c_models}
WHERE {columns.prdn.name} IN (
    SELECT DISTINCT {columns.prdn.name}
    FROM {table_names.c2_models_out}
);

---- Remove prdn-inv_seq pairs if PIK is not unique
CREATE TABLE to_delete_from_c_models AS
SELECT
    {columns.prdn.name},
    {columns.inv_seq.name}
FROM
(
    SELECT DISTINCT
        {columns.prdn.name},
        {columns.inv_seq.name},
        {columns.pik.name}
    FROM {table_names.c_models}
) subquery
GROUP BY
    {columns.prdn.name},
    {columns.inv_seq.name}
HAVING COUNT(*) > 1;

CREATE INDEX
    idx_to_delete_from_c_models
ON
    to_delete_from_c_models(
        {columns.prdn.name},
        {columns.inv_seq.name}
    );

DELETE FROM {table_names.c_models}
WHERE EXISTS
(
    SELECT *
    FROM to_delete_from_c_models
    WHERE
        {table_names.c_models}.{columns.prdn.name} = to_delete_from_c_models.{columns.prdn.name} AND
        {table_names.c_models}.{columns.inv_seq.name} = to_delete_from_c_models.{columns.inv_seq.name}
);

DROP TABLE to_delete_from_c_models;
    ''')
