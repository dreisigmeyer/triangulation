import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


def alter_closed_loop_table(fh, tbl_name):
    """

    """
    fh.write(
        f'''
ALTER TABLE
    {tbl_name}
ADD COLUMN
    {columns.assg_ctry.name};
UPDATE
    {tbl_name}
SET
    {columns.assg_ctry.name} =
    (
        SELECT {columns.assg_ctry.name}
        FROM {table_names.assignee_info}
        WHERE
            {table_names.assignee_info}.{columns.prdn.name} = {tbl_name}.{columns.assg_prdn.name} AND
            {table_names.assignee_info}.{columns.assg_seq.name} = {tbl_name}.{columns.assg_seq.name}
    );
ALTER TABLE
    {tbl_name}
ADD COLUMN
    {columns.assg_st.name};
UPDATE
    {tbl_name}
SET
    {columns.assg_st.name} =
    (
        SELECT {columns.assg_st.name}
        FROM {table_names.assignee_info}
        WHERE
            {table_names.assignee_info}.{columns.prdn.name} = {tbl_name}.{columns.assg_prdn.name} AND
            {table_names.assignee_info}.{columns.assg_seq.name} = {tbl_name}.{columns.assg_seq.name}
    );
ALTER TABLE
    {tbl_name}
ADD COLUMN
    {columns.assg_type.name};
UPDATE
    {tbl_name}
SET
    {columns.assg_type.name} =
    (
        SELECT {columns.assg_type.name}
        FROM {table_names.assignee_info}
        WHERE
            {table_names.assignee_info}.{columns.prdn.name} = {tbl_name}.{columns.assg_prdn.name} AND
            {table_names.assignee_info}.{columns.assg_seq.name} = {tbl_name}.{columns.assg_seq.name}
    );
ALTER TABLE
    {tbl_name}
ADD COLUMN
    {columns.us_inv_flag.name};
UPDATE
    {tbl_name}
SET
    {columns.us_inv_flag.name} =
    (
        SELECT {columns.us_inv_flag.name}
        FROM {table_names.prdn_metadata}
        WHERE
            {table_names.prdn_metadata}.{columns.prdn.name} = {tbl_name}.{columns.assg_prdn.name}
    );
ALTER TABLE
    {tbl_name}
ADD COLUMN
    {columns.mult_assg_flag.name};
UPDATE
    {tbl_name}
SET
    {columns.mult_assg_flag.name} =
    (
        SELECT {columns.num_assg.name}
        FROM {table_names.prdn_metadata}
        WHERE
            {table_names.prdn_metadata}.{columns.prdn.name} = {tbl_name}.{columns.assg_prdn.name}
    );
    ''')


def a_model_postprocess(fh):
    '''
    '''
    fh.write(
        f'''
        ''')


def create_aux_table(fh):
    """

    """
    fh.write(
        f'''
-- For B models
CREATE TABLE {table_names.b_model_info}
(
    {columns.firmid.name},
    {columns.cw_yr.name},
    CONSTRAINT firmid_cwyr_unique UNIQUE (
        {columns.firmid.name},
        {columns.cw_yr.name}
    )
);
-- For C models: replaces Amodel_pik_year_firmid.pl
CREATE TABLE {table_names.c_model_info}
(
    {columns.pik.name},
    {columns.emp_yr.name},
    {columns.firmid.name},
    CONSTRAINT pik_empyr_firmid_unique UNIQUE (
        {columns.pik.name},
        {columns.emp_yr.name},
        {columns.firmid.name}
    )
);
-- For E models
CREATE TABLE {table_names.e_model_info}
(
    {columns.prdn.name},
    CONSTRAINT prdn_unique UNIQUE (
        {columns.prdn.name}
    )
);
    ''')


def create_closed_loop_table(fh, tbl_name, join_cols):
    """

    """

    # Create the table
    fh.write(
        f'''
CREATE TABLE {tbl_name} AS
SELECT
    {table_names.pik_data}.{columns.prdn.name} AS {columns.pik_prdn.name},
    {table_names.ein_data}.{columns.prdn.name} AS {columns.assg_prdn.name},
    {table_names.pik_data}.{columns.app_yr.name},
    {table_names.ein_data}.{columns.grant_yr.name},
    {table_names.ein_data}.{columns.assg_seq.name},
    {table_names.pik_data}.{columns.inv_seq.name},
    {table_names.pik_data}.{columns.pik.name},
    {table_names.ein_data}.{columns.cw_yr.name},
    {table_names.pik_data}.{columns.emp_yr.name},
    {table_names.pik_data}.{columns.ein.name} AS {columns.pik_ein.name},
    {table_names.ein_data}.{columns.ein.name} AS {columns.assg_ein.name},
    {table_names.pik_data}.{columns.firmid.name} AS {columns.pik_firmid.name},
    {table_names.ein_data}.{columns.firmid.name} AS {columns.assg_firmid.name},
    {table_names.ein_data}.{columns.pass_num.name}
FROM {table_names.pik_data}
INNER JOIN {table_names.ein_data}
USING ({",".join([x.name for x in join_cols])})
WHERE {table_names.ein_data}.{columns.firmid.name} != \'\';

DROP INDEX IF EXISTS {tbl_name}_idx;
CREATE INDEX
    {tbl_name}_idx
ON
    {tbl_name}(
        {columns.assg_prdn.name},
        {columns.assg_seq.name},
        {columns.cw_yr.name},
        {columns.emp_yr.name},
        {columns.assg_firmid.name}
    );
    ''')


def generate_a_model_sql_script(sql_script_fn):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        shared_code.in_data_tables(f, 'A')
        create_aux_table(f)

        # A1 models
        tbl_name = 'closed_paths'
        join_cols = [columns.prdn, columns.ein, columns.firmid]
        create_closed_loop_table(f, tbl_name, join_cols)
        alter_closed_loop_table(f, tbl_name)
        output_a_models(f, tbl_name, file_names.a1_models, 'A1')
        update_b_model_info(f)
        update_c_model_info(f, tbl_name)
        update_e_model_info(f, tbl_name)
        postprocess_database(f, tbl_name)

        # A2 models
        join_cols = [columns.prdn, columns.firmid]
        create_closed_loop_table(f, tbl_name, join_cols)
        alter_closed_loop_table(f, tbl_name)
        output_a_models(f, tbl_name, file_names.a2_models, 'A2')
        update_b_model_info(f)
        update_c_model_info(f, tbl_name)
        update_e_model_info(f, tbl_name)
        postprocess_database(f, tbl_name)

        # A3 models
        join_cols = [columns.prdn, columns.ein]
        create_closed_loop_table(f, tbl_name, join_cols)
        alter_closed_loop_table(f, tbl_name)
        output_a_models(f, tbl_name, file_names.a3_models, 'A3')
        update_b_model_info(f)
        update_c_model_info(f, tbl_name)
        update_e_model_info(f, tbl_name)
        postprocess_database(f, tbl_name)

        # Final post-processing
        a_model_postprocess(f)


def output_a_models(fh, tbl_name, csv_file, model):
    """
    Extract the final closed loops.
    This replaces the extract_A_paths.pl and extract_A_paths.sh scripts.
    Uses a window function and requires SQLite >=v3.25.0
    """
    fh.write(
        f'''
CREATE TABLE inv_counts AS
SELECT
    COUNT(DISTINCT {columns.inv_seq.name}) AS {columns.num_inv.name},
    {columns.assg_prdn.name},
    {columns.assg_seq.name},
    ABS({columns.cw_yr.name} - {columns.grant_yr.name}) AS {columns.abs_cw_yr.name},
    {columns.cw_yr.name},
    ABS({columns.emp_yr.name} - {columns.app_yr.name})  AS {columns.abs_emp_yr.name},
    {columns.emp_yr.name},
    {columns.assg_firmid.name},
    {columns.grant_yr.name},
    {columns.app_yr.name},
    {columns.assg_ctry.name},
    {columns.assg_st.name},
    {columns.assg_type.name},
    {columns.us_inv_flag.name},
    {columns.mult_assg_flag.name}
FROM {tbl_name}
-- grouping to find the number of inventors at the firmid for a
-- given |cw_yr - grant_yr|, cw_yr, |emp_yr - app_yr| and emp_yr
-- for each prdn+assg_seq pair.
GROUP BY
    {columns.assg_prdn.name},
    {columns.assg_seq.name},
    ABS({columns.cw_yr.name} - {columns.grant_yr.name}),
    {columns.cw_yr.name},
    ABS({columns.emp_yr.name} - {columns.app_yr.name}),
    {columns.emp_yr.name},
    {columns.assg_firmid.name};

DROP INDEX IF EXISTS inv_counts_idx;
CREATE INDEX
    inv_counts_idx
ON
    inv_counts(
        {columns.assg_prdn.name},
        {columns.assg_seq.name},
        {columns.abs_cw_yr.name},
        {columns.cw_yr.name},
        {columns.abs_emp_yr.name},
        {columns.emp_yr.name},
        {columns.num_inv.name}
    );

CREATE TABLE {table_names.closed_loops} AS
SELECT
    {columns.assg_prdn.name},
    {columns.assg_seq.name},
    {columns.assg_firmid.name},
    {columns.app_yr.name},
    {columns.grant_yr.name},
    {columns.assg_type.name},
    {columns.assg_st.name},
    {columns.assg_ctry.name},
    0 AS {columns.us_assg_flag.name},
    0 AS {columns.foreign_assg_flag.name},
    {columns.us_inv_flag.name},
    {columns.mult_assg_flag.name},
    {columns.cw_yr.name},
    {columns.emp_yr.name},
    "{model}" AS {columns.model.name},
    0 AS {columns.uniq_firmid.name},
    {columns.num_inv.name}
FROM (
    SELECT
        {columns.num_inv.name},
        {columns.assg_prdn.name},
        {columns.assg_seq.name},
        {columns.abs_cw_yr.name},
        {columns.cw_yr.name},
        {columns.abs_emp_yr.name},
        {columns.emp_yr.name},
        {columns.assg_firmid.name},
        {columns.grant_yr.name},
        {columns.app_yr.name},
        {columns.assg_ctry.name},
        {columns.assg_st.name},
        {columns.assg_type.name},
        {columns.us_inv_flag.name},
        {columns.mult_assg_flag.name},
        -- for each prdn+assg_seq pair sort by |cw_yr - grant_yr|,
        -- cw_yr, |emp_yr - app_yr|, emp_yr and num_inv and take the
        -- first row(s)
        RANK() OVER (
            PARTITION BY
                {columns.assg_prdn.name},
                {columns.assg_seq.name}
            ORDER BY
                {columns.abs_cw_yr.name},
                {columns.cw_yr.name},
                {columns.abs_emp_yr.name},
                {columns.emp_yr.name},
                {columns.num_inv.name} DESC
        ) AS rnk
    FROM inv_counts
)
WHERE rnk = 1;

DROP INDEX IF EXISTS {table_names.closed_loops}_idx;
CREATE INDEX
    {table_names.closed_loops}_idx
ON
    {table_names.closed_loops}(
        {columns.assg_prdn.name},
        {columns.assg_seq.name},
        {columns.cw_yr.name},
        {columns.emp_yr.name},
        {columns.assg_firmid.name}
    );

DROP TABLE inv_counts;

-- a state => US assignee
UPDATE {table_names.closed_loops}
SET {columns.us_assg_flag.name} = 1
WHERE {columns.assg_st.name} != "";
-- no state + country => foreign assignee
UPDATE {table_names.closed_loops}
SET {columns.foreign_assg_flag.name} = 1
WHERE
    {columns.us_assg_flag.name} != 1 AND
    {columns.assg_ctry.name} != "";

UPDATE {table_names.closed_loops} AS outer_tbl
SET {columns.uniq_firmid.name} = 1
WHERE
    (
        SELECT COUNT(*)
        FROM {table_names.closed_loops} AS inner_tbl
        WHERE
            outer_tbl.{columns.assg_prdn.name} = inner_tbl.{columns.assg_prdn.name} AND
            outer_tbl.{columns.assg_seq.name} = inner_tbl.{columns.assg_seq.name}
    ) > 1;
    ''')
    shared_code.output_data(fh, f'{table_names.closed_loops}', csv_file)


def postprocess_database(fh, tbl_name):
    """

    """
    fh.write(
        f'''
DROP TABLE {table_names.closed_loops};
CREATE TABLE prdn_as_Amodel AS
SELECT DISTINCT {columns.pik_prdn.name}, {columns.assg_seq.name}
FROM {tbl_name};

DROP INDEX IF EXISTS indx_2;
CREATE INDEX
    indx_2
ON
    prdn_as_Amodel({columns.pik_prdn.name}, {columns.assg_seq.name});

DELETE FROM {table_names.ein_data}
WHERE EXISTS (
    SELECT *
    FROM prdn_as_Amodel
    WHERE prdn_as_Amodel.{columns.pik_prdn.name} = {table_names.ein_data}.{columns.prdn.name}
    AND prdn_as_Amodel.{columns.assg_seq.name} = {table_names.ein_data}.{columns.assg_seq.name}
);

DROP TABLE prdn_as_Amodel;
DROP TABLE {tbl_name};
    ''')


def update_b_model_info(fh):
    """

    """
    fh.write(
        f'''
INSERT OR IGNORE INTO {table_names.b_model_info}
SELECT DISTINCT
    {columns.assg_firmid.name},
    {columns.cw_yr.name}
FROM
    {table_names.closed_loops};
    ''')


def update_c_model_info(fh, tbl_name):
    """
    This replaces Amodel_pik_year_firmid.pl
    """
    fh.write(
        f'''
INSERT OR IGNORE INTO {table_names.c_model_info}
SELECT DISTINCT
    {tbl_name}.{columns.pik.name},
    {tbl_name}.{columns.emp_yr.name},
    {tbl_name}.{columns.assg_firmid.name}
FROM
    {tbl_name},
    {table_names.closed_loops}
WHERE
    {tbl_name}.{columns.assg_prdn.name} = {table_names.closed_loops}.{columns.assg_prdn.name} AND
    {tbl_name}.{columns.assg_seq.name} = {table_names.closed_loops}.{columns.assg_seq.name} AND
    {tbl_name}.{columns.cw_yr.name} = {table_names.closed_loops}.{columns.cw_yr.name} AND
    {tbl_name}.{columns.emp_yr.name} = {table_names.closed_loops}.{columns.emp_yr.name} AND
    {tbl_name}.{columns.assg_firmid.name} = {table_names.closed_loops}.{columns.assg_firmid.name};
    ''')


def update_e_model_info(fh, tbl_name):
    """
    This replaces Amodel_pik_year_firmid.pl
    """
    fh.write(
        f'''
INSERT OR IGNORE INTO {table_names.e_model_info}
SELECT DISTINCT
    {tbl_name}.{columns.prdn.name}
FROM
    {tbl_name}
   ''')
