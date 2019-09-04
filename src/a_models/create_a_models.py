import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names
import sqlite3


def alter_closed_loop_table(cur, tbl_name):
    """

    """
    cp_new_columns = f'''
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
                    {table_names.assignee_info}.{columns.prdn.name} = {tbl_name}.{columns.ein_prdn.name} AND
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
                    {table_names.assignee_info}.{columns.prdn.name} = {tbl_name}.{columns.ein_prdn.name} AND
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
                    {table_names.assignee_info}.{columns.prdn.name} = {tbl_name}.{columns.ein_prdn.name} AND
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
                    {table_names.prdn_metadata}.{columns.prdn.name} = {tbl_name}.{columns.ein_prdn.name}
            );
        ALTER TABLE
            {tbl_name}
        ADD COLUMN
            {columns.mult_assg_flag};
        UPDATE
            {tbl_name}
        SET
            {columns.mult_assg_flag} =
            (
                SELECT {columns.num_assg.name}
                FROM {table_names.prdn_metadata}
                WHERE
                    {table_names.prdn_metadata}.{columns.prdn.name} = {tbl_name}.{columns.ein_prdn.name}
            );
    '''
    cur.executescript(cp_new_columns)


def create_closed_loop_table(cur, tbl_name, join_cols):
    """

    """

    # Create the table
    cp_create = f'''
        CREATE TABLE {tbl_name} AS
        SELECT
            {table_names.pik_data}.{columns.prdn.name} AS {columns.pik_prdn.name},
            {table_names.ein_data}.{columns.prdn.name} AS {columns.ein_prdn.name},
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
            {table_names.ein_data}.{columns.pass_no.name}
        FROM {table_names.pik_data}
        INNER JOIN {table_names.ein_data}
        USING ({",".join([x.name for x in join_cols])})
        WHERE {table_names.ein_data}.{columns.firmid.name} != \'\';
    '''
    cur.execute(cp_create)

    # Put an index on it
    cp_indx = f'''
        CREATE INDEX
            {tbl_name}_idx
        ON
            {tbl_name}({columns.ein_prdn.name}, {columns.assg_seq.name});
    '''
    cur.execute(cp_indx)


def in_data_tables(cur, database_name):
    """

    """

    # Create all of the tables
    cur.execute(f'''
        CREATE TABLE {table_names.pik_data} (
            {columns.prdn.cmd},
            {columns.grant_yr.cmd},
            {columns.app_yr.cmd},
            {columns.inv_seq.cmd},
            {columns.pik.cmd},
            {columns.ein.cmd},
            {columns.firmid.cmd},
            {columns.emp_yr.cmd}
        );
        CREATE TABLE {table_names.ein_data} (
            {columns.prdn.cmd},
            {columns.grant_yr.cmd},
            {columns.assg_seq.cmd},
            {columns.ein.cmd},
            {columns.firmid.cmd},
            {columns.cw_yr.cmd},
            {columns.pass_no.cmd}
        );
        CREATE TABLE {table_names.assignee_info} (
            {columns.prdn.cmd},
            {columns.assg_seq.cmd},
            {columns.assg_type.cmd},
            {columns.assg_st.cmd},
            {columns.assg_ctry.cmd}
        );
        CREATE TABLE {table_names.prdn_metadata} (
            {columns.prdn.cmd},
            {columns.grant_yr.cmd},
            {columns.app_yr.cmd},
            {columns.num_assg.cmd},
            {columns.us_inv_flag.name.cmd}
        );
    ''')

    # Load the data in
    shared_code.import_data(database_name, table_names.pik_data, file_names.pik_data_csvfile)
    shared_code.import_data(database_name, table_names.ein_data, file_names.ein_data_csvfile)
    shared_code.import_data(database_name, table_names.assignee_info, file_names.assignee_info_csvfile)
    shared_code.import_data(database_name, table_names.prdn_metadata, file_names.prdn_metadata_csvfile)

    # Make the indexes
    cur.execute(f'''
        CREATE INDEX
            ein_idx_prdn_ein_firmid_idx
        ON
            {table_names.ein_data}({columns.prdn.name}, {columns.ein.name}, {columns.firmid.name});
        CREATE INDEX
            pik_idx_prdn_ein_firmid_idx
        ON
            {table_names.pik_data}({columns.prdn.name}, {columns.ein.name}, {columns.firmid.name});
        CREATE INDEX
            ein_idx_prdn_as_idx
        ON
            {table_names.ein_data}({columns.prdn.name}, {columns.assg_seq.name});
        CREATE INDEX
            assg_info_prdn_as_idx
        ON
            {table_names.assignee_info}({columns.prdn.name}, {columns.assg_seq.name});
        CREATE INDEX
            prdn_metadata_main_idx
        ON
            {table_names.prdn_metadata}({columns.prdn.name});
    ''')


def make_a_models(database_name):
    """

    """
    conn = sqlite3.connect(database_name)
    cur = conn.cursor()
    cur.execute('pragma temp_store = MEMORY;')  # /tmp was filling up - the PRAGMA seems to take care of that
    in_data_tables(cur, database_name)

    # A1 models
    tbl_name = 'closed_paths'
    join_cols = [columns.prdn, columns.ein, columns.firmid]
    create_closed_loop_table(cur, tbl_name, join_cols)
    alter_closed_loop_table(cur, tbl_name)
    output_a_models(cur, database_name, tbl_name, file_names.a1_models, 'A1')
    postprocess_database(cur, tbl_name)

    # A2 models
    join_cols = [columns.prdn, columns.ein, columns.firmid]
    create_closed_loop_table(cur, tbl_name, join_cols)
    alter_closed_loop_table(cur, tbl_name)
    output_a_models(cur, database_name, tbl_name, file_names.a2_models, 'A2')
    postprocess_database(cur, tbl_name)

    # A3 models
    join_cols = [columns.prdn, columns.ein, columns.firmid]
    create_closed_loop_table(cur, tbl_name, join_cols)
    alter_closed_loop_table(cur, tbl_name)
    output_a_models(cur, database_name, tbl_name, file_names.a3_models, 'A3')
    postprocess_database(cur, tbl_name)

    # Final post-processing
    cur.execute('DROP INDEX ein_idx_prdn_as;')


#-------------------------------------------------------------------------------------------------------------
# !!! Need to update names here
def output_a_models(cur, database_name, tbl_name, csv_file, model):
    """
    Extract the final closed loops.
    This replaces the extract_A_paths.pl and extract_A_paths.sh scripts.
    Uses a window function and requires SQLite >=v3.25.0
    """
    cp_closed_loops = f'''
        CREATE TABLE inv_counts AS
        SELECT
            COUNT(DISTINCT {columns.inv_seq.name}) AS {columns.num_inv.name},
            {columns.prdn.name},
            {columns.assg_seq.name},
            ABS({columns.cw_yr.name} - {columns.grant_yr.name}) AS {columns.abs_cw_yr.name},
            {columns.cw_yr.name},
            ABS({columns.emp_yr.name} - {columns.app_yr.name})  AS {columns.abs_emp_yr.name},
            {columns.emp_yr.name},
            {columns.firmid.name},
            {columns.grant_yr.name},
            {columns.app_yr.name},
            {columns.assg_ctry.name},
            {columns.assg_st.name},
            {columns.assg_type.name},
            {columns.us_inv_flag.name},
            {columns.mult_assg_flag.name}
        FROM {file_names}
        -- grouping to find the number of inventors at the firmid for a
        -- given |cw_yr - grant_yr|, cw_yr, |emp_yr - app_yr| and emp_yr
        -- for each prdn+assg_seq pair.
        GROUP BY
            {columns.prdn.name},
            {columns.assg_seq.name},
            ABS({columns.cw_yr.name} - {columns.grant_yr.name}),
            {columns.cw_yr.name},
            ABS({columns.emp_yr.name} - {columns.app_yr.name}),
            {columns.emp_yr.name},
            {columns.firmid.name};

        CREATE TABLE {names.closed_loops_TB} AS
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
            {columns.mult_assg_flag.name},
            {columns.cw_yr.name},
            {columns.emp_yr.name},
            {model},
            0 AS {columns.uniq_firmid.name},
            {columns.num_inv.name}
        FROM (
            SELECT
                {columns.num_inv.name},
                {columns.prdn.name},
                {columns.assg_seq.name},
                {columns.abs_cw_yr.name},
                {columns.cw_yr.name},
                {columns.abs_emp_yr.name},
                {columns.emp_yr.name},
                {columns.firmid.name},
                {columns.grant_yr.name},
                {columns.app_yr.name},
                {columns.assg_ctry.name},
                {columns.assg_st.name},
                {columns.assg_type.name},
                {columns.us_inv_flag.name},
                {columns.mult_assg_flag.name},
                {model}
                -- for each prdn+assg_seq pair sort by |cw_yr - grant_yr|,
                -- cw_yr, |emp_yr - app_yr|, emp_yr and num_inv and take the
                -- first row(s)
                RANK() OVER (
                    PARTITION BY
                        {columns.prdn.name},
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

        DROP TABLE inv_counts;

        -- a state => US assignee
        UPDATE {names.closed_loops_TB}
        SET {columns.us_assg_flag.name} = 1
        WHERE {columns.assg_st.name} != "";
        -- no state + country => foreign assignee
        UPDATE {names.closed_loops_TB}
        SET {columns.foreign_assg_flag.name} = 1
        WHERE
            {columns.us_assg_flag.name} != 1 AND
            {columns.assg_ctry.name} != "";

        UPDATE {names.closed_loops_TB} AS outer_tbl
        SET {columns.uniq_firmid.name} = 1
        WHERE
            (
                SELECT COUNT(*)
                FROM {names.closed_loops_TB} AS inner_tbl
                WHERE
                    outer_tbl.{columns.prdn.name} = inner_tbl.{columns.prdn.name} AND
                    outer_tbl.{columns.assg_seq.name} = inner_tbl.{columns.assg_seq.name}
            ) = 1;
    '''
    cur.executescript(cp_closed_loops)
    shared_code.output_data(database_name, tbl_name, csv_file)
#-------------------------------------------------------------------------------------------------------------


def postprocess_database(cur, tbl_name):
    """

    """
    postprocess_query = f'''
        CREATE TABLE prdn_as_Amodel AS
        SELECT DISTINCT {columns.pik_prdn.name}, {columns.assg_seq.name}
        FROM {tbl_name};

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
        VACUUM;
    '''
    cur.executescript(postprocess_query)
