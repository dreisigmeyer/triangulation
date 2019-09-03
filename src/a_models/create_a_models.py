import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names
import sqlite3


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
