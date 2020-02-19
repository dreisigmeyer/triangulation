import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


def delete_previous_models(fh, model):
    """
    """
    previous_models = [
        table_names.a1_models,
        table_names.a2_models,
        table_names.a3_models,
        table_names.b1_models,
        table_names.b2_models,
        table_names.c1_models,
        table_names.c2_models,
        table_names.c3_models,
        table_names.e1_models,
        table_names.e2_models,
    ]

    for previous_model in previous_models:
        fh.write(
            f'''
DELETE FROM {table_names.possible_d_models}
WHERE EXISTS (
    SELECT *
    FROM {previous_model}
    WHERE
        {previous_model}.{columns.prdn.name} = {table_names.possible_d_models}.{columns.prdn.name}
);
        ''')

    if model == 'D2':
        fh.write(
            f'''
DELETE FROM {table_names.possible_d_models}
WHERE NOT EXISTS (
    SELECT *
    FROM {table_names.name_match_prdn_assg_num}
    WHERE
        {table_names.name_match_prdn_assg_num}.{columns.prdn.name} = {table_names.possible_d_models}.{columns.prdn.name}
        AND {table_names.name_match_prdn_assg_num}.{columns.assg_seq.name} = {table_names.possible_d_models}.{columns.assg_seq.name}
);

CREATE TABLE {table_names.big_firm_names}
AS
    SELECT {columns.assg_name.name}
    FROM (
        SELECT
            {columns.assg_name.name},
            COUNT(DISTINCT {columns.prdn.name}) AS name_count
        FROM {table_names.possible_d_models}
        GROUP BY {columns.assg_name.name}
    ) subquery_1
    WHERE subquery_1.name_count > 499;

DELETE FROM {table_names.possible_d_models}
WHERE EXISTS (
    SELECT *
    FROM {table_names.d1_models}
    WHERE
        {table_names.d1_models}.{columns.prdn.name} = {table_names.possible_d_models}.{columns.prdn.name}
);
        ''')


def make_aux_tables(fh):
    """

    """
    fh.write(
        f'''
CREATE TABLE {table_names.name_match_prdn_assg_num} (
    {columns.prdn.cmd},
    {columns.assg_seq.cmd},
    UNIQUE (
        {columns.prdn.name},
        {columns.assg_seq.name}
    )
);

CREATE TABLE {table_names.firmid_seq_data} (
    {columns.prdn.cmd},
    {columns.assg_seq.cmd},
    {columns.firmid.cmd},
    UNIQUE (
        {columns.prdn.name},
        {columns.assg_seq.name},
        {columns.firmid.name}
    )
);

CREATE TABLE {table_names.firmid_name_data} (
    {columns.firm_name.cmd},
    {columns.firmid.cmd},
    UNIQUE (
        {columns.firm_name.name},
        {columns.firmid.name}
    )
);

CREATE TABLE {table_names.assg_name_firmid} (
    {columns.firm_name.cmd},
    {columns.year.cmd},
    {columns.firmid.cmd},
    UNIQUE(
        {columns.firm_name.name},
        {columns.year.name},
        {columns.firmid.name}
    )
);

INSERT INTO {table_names.name_match_prdn_assg_num}
SELECT DISTINCT
    {columns.xml_pat_num.name},
    {columns.assg_seq.name}
FROM
    {table_names.name_match};

INSERT INTO {table_names.firmid_seq_data}
SELECT DISTINCT
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.firmid.name}
FROM {table_names.a1_models};

INSERT INTO {table_names.firmid_seq_data}
SELECT DISTINCT
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.firmid.name}
FROM {table_names.a2_models};

INSERT INTO {table_names.firmid_seq_data}
SELECT DISTINCT
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.firmid.name}
FROM {table_names.a3_models};

INSERT INTO {table_names.firmid_name_data}
SELECT DISTINCT
    {table_names.assignee_name_data}.{columns.assg_name.name},
    {table_names.firmid_seq_data}.{columns.firmid.name}
FROM
    {table_names.assignee_name_data},
    {table_names.firmid_seq_data}
WHERE
    {table_names.assignee_name_data}.{columns.xml_pat_num.name} = {table_names.firmid_seq_data}.{columns.prdn.name} AND
    {table_names.assignee_name_data}.{columns.assg_seq.name} = {table_names.firmid_seq_data}.{columns.assg_seq.name};

DELETE FROM {table_names.firmid_name_data}
WHERE {columns.firm_name.name} IN (
    SELECT {columns.firm_name.name}
    FROM {table_names.firmid_name_data}
    GROUP BY {columns.firm_name.name}
    HAVING COUNT(*) > 1
);

DELETE FROM {table_names.firmid_name_data}
WHERE {columns.firm_name.name} IN (
    SELECT {columns.firm_name.name}
    FROM {table_names.firmid_name_data}
    WHERE {columns.firmid.name} = ''
);
    ''')


def make_possible_d_models(fh, model):
    """

    """
    fh.write(
        f'''
DROP TABLE IF EXISTS {table_names.possible_d_models};
CREATE TABLE {table_names.possible_d_models} (
    {columns.prdn.cmd},
    {columns.assg_seq.cmd},
    {columns.app_yr.cmd},
    {columns.grant_yr.cmd},
    {columns.assg_type.cmd},
    {columns.assg_st.cmd},
    {columns.assg_ctry.cmd},
    {columns.us_inv_flag.cmd},
    {columns.assg_name.cmd},
    {columns.num_assg.cmd}
);

INSERT INTO {table_names.possible_d_models} (
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.grant_yr.name},
    {columns.app_yr.name},
    {columns.assg_ctry.name},
    {columns.assg_st.name},
    {columns.assg_type.name},
    {columns.us_inv_flag.name},
    {columns.assg_name.name},
    {columns.num_assg.name}
)
SELECT
    {table_names.assignee_name_data}.{columns.xml_pat_num.name},
    {table_names.assignee_name_data}.{columns.assg_seq.name},
    {table_names.assignee_name_data}.{columns.grant_yr.name},
    {table_names.prdn_metadata}.{columns.app_yr.name},
    {table_names.assignee_info}.{columns.assg_ctry.name},
    {table_names.assignee_info}.{columns.assg_st.name},
    {table_names.assignee_info}.{columns.assg_type.name},
    {table_names.prdn_metadata}.{columns.us_inv_flag.name},
    {table_names.assignee_name_data}.{columns.assg_name.name},
    {table_names.prdn_metadata}.{columns.num_assg.name}
FROM
    {table_names.assignee_name_data},
    {table_names.assignee_info},
    {table_names.prdn_metadata}
WHERE
    ''')

    if model == 'D1':
        fh.write(
            f'''
    {table_names.assignee_name_data}.{columns.assg_name.name} IN (
    SELECT {columns.firm_name.name} FROM {table_names.firmid_name_data}) AND
        ''')
    fh.write(
        f'''
    {table_names.assignee_name_data}.{columns.xml_pat_num.name} = {table_names.assignee_info}.{columns.prdn.name} AND
    {table_names.assignee_name_data}.{columns.assg_seq.name} = {table_names.assignee_info}.{columns.assg_seq.name} AND
    {table_names.assignee_name_data}.{columns.xml_pat_num.name} = {table_names.prdn_metadata}.{columns.prdn.name};
    ''')


def make_possible_d1_models(fh):
    """

    """
    make_possible_d_models(fh, 'D1')

    fh.write(
        f'''
ALTER TABLE {table_names.possible_d_models}
ADD COLUMN {columns.firmid_null.cmd};

UPDATE {table_names.possible_d_models}
SET {columns.firmid.name} = (
    SELECT {table_names.firmid_name_data}.{columns.firmid.name}
    FROM {table_names.firmid_name_data}
    WHERE {table_names.possible_d_models}.{columns.assg_name.name} = {table_names.firmid_name_data}.{columns.firm_name.name}
);

CREATE INDEX possible_d_models_indx
ON {table_names.possible_d_models} (
    {columns.prdn.name},
    {columns.assg_seq.name}
);

DELETE FROM {table_names.possible_d_models}
WHERE NOT EXISTS (
    SELECT *
    FROM {table_names.name_match_prdn_assg_num}
    WHERE
        {table_names.name_match_prdn_assg_num}.{columns.prdn.name} = {table_names.possible_d_models}.{columns.prdn.name} AND
        {table_names.name_match_prdn_assg_num}.{columns.assg_seq.name} = {table_names.possible_d_models}.{columns.assg_seq.name}
);
    ''')


def make_possible_d2_models(fh):
    """

    """
    make_possible_d_models(fh, 'D2')

    fh.write(
        f'''
.import {file_names.out_data_path}{file_names.d1_models} {table_names.d1_models}

CREATE INDEX d1_final_indx
ON {table_names.d1_models} (
    {columns.prdn.name},
    {columns.firmid.name},
    {columns.assg_seq.name}
);
    ''')


def make_output_d1_models(fh):
    """

    """
    fh.write(
        f'''
DROP TABLE IF EXISTS {table_names.d_final_models};
CREATE TABLE {table_names.d_final_models} AS
SELECT
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.firmid.name},
    {columns.app_yr.name},
    {columns.grant_yr.name},
    {columns.assg_type.name},
    {columns.assg_st.name},
    {columns.assg_ctry.name},
    0 AS {columns.us_assg_flag.name},
    0 AS {columns.foreign_assg_flag.name},
    {columns.us_inv_flag.name},
    {columns.num_assg.name},
    "" AS {columns.cw_yr.name},
    "" AS {columns.emp_yr.name},
    "D1" AS {columns.model.name},
    "" AS {columns.uniq_firmid.name},
    "" AS {columns.num_inv.name}
FROM
    {table_names.possible_d_models}
WHERE
    {columns.firmid.name} IS NOT NULL;

-- a state => US assignee
UPDATE {table_names.d_final_models}
SET {columns.us_assg_flag.name} = 1
WHERE {columns.assg_st.name} != "";
-- no state + country => foreign assignee
UPDATE {table_names.d_final_models}
SET {columns.foreign_assg_flag.name} = 1
WHERE
    {columns.us_assg_flag.name} != 1 AND
    {columns.assg_ctry.name} != "";
    ''')


def make_output_d2_models(fh):
    """

    """
    fh.write(
        f'''
DROP TABLE IF EXISTS {table_names.d_final_models};
CREATE TABLE {table_names.d_final_models} AS
SELECT
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.firmid.name},
    {columns.app_yr.name},
    {columns.grant_yr.name},
    {columns.assg_type.name},
    {columns.assg_st.name},
    {columns.assg_ctry.name},
    0 AS {columns.us_assg_flag.name},
    0 AS {columns.foreign_assg_flag.name},
    {columns.us_inv_flag.name},
    {columns.num_assg.name},
    "" AS {columns.cw_yr.name},
    "" AS {columns.emp_yr.name},
    "D2" AS {columns.model.name},
    "" AS {columns.uniq_firmid.name},
    "" AS {columns.num_inv.name}
FROM
    {table_names.possible_d_models},
    {table_names.assg_name_firmid}
WHERE
    {table_names.assg_name_firmid}.{columns.firmid.name} IS NOT NULL AND
    {table_names.possible_d_models}.{columns.assg_name.name} IN (SELECT {columns.assg_name.name} FROM {table_names.big_firm_names}) AND
    {table_names.possible_d_models}.{columns.grant_yr.name} = {table_names.assg_name_firmid}.{columns.year.name} AND
    {table_names.possible_d_models}.{columns.assg_name.name} = {table_names.assg_name_firmid}.name;

-- a state => US assignee
UPDATE {table_names.d_final_models}
SET {columns.us_assg_flag.name} = 1
WHERE {columns.assg_st.name} != "";
-- no state + country => foreign assignee
UPDATE {table_names.d_final_models}
SET {columns.foreign_assg_flag.name} = 1
WHERE
    {columns.us_assg_flag.name} != 1 AND
    {columns.assg_ctry.name} != "";
    ''')


def generate_d_model_sql_script(sql_script_fn, assignee_years):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        shared_code.in_data_tables(f, 'D', assignee_years)
        make_aux_tables(f)
        make_possible_d1_models(f)
        delete_previous_models(f, 'D1')
        make_output_d1_models(f)
        shared_code.output_distinct_data(f, f'{table_names.d_final_models}', f'{file_names.d1_models}')
        make_possible_d2_models(f)
        delete_previous_models(f, 'D2')
        make_output_d2_models(f)
        shared_code.output_distinct_data(f, f'{table_names.d_final_models}', f'{file_names.d2_models}')
