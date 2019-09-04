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
    update_ein_data(cur, tbl_name)

    # A2 models
    join_cols = [columns.prdn, columns.ein, columns.firmid]
    create_closed_loop_table(cur, tbl_name, join_cols)
    alter_closed_loop_table(cur, tbl_name)
    update_ein_data(cur, tbl_name)

    # A3 models
    join_cols = [columns.prdn, columns.ein, columns.firmid]
    create_closed_loop_table(cur, tbl_name, join_cols)
    alter_closed_loop_table(cur, tbl_name)
    update_ein_data(cur, tbl_name)

    # Final post-processing
    cur.execute('DROP INDEX ein_idx_prdn_as;')


def output_a_models(database_name, tbl_name, csv_file):
    """

    """
    shared_code.output_data(database_name, tbl_name, csv_file)


def update_ein_data(cur, tbl_name):
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
