import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names
import sqlite3


def in_data_tables(cur, database_name):
    """

    """
    cur.execute(f'''
        CREATE TABLE {table_names.pik_data} (
            prdn TEXT NOT NULL,
            grant_yr INTEGER NOT NULL,
            app_yr INTEGER NOT NULL,
            inv_seq INTEGER NOT NULL,
            pik TEXT NOT NULL,
            ein TEXT NOT NULL,
            firmid TEXT NOT NULL,
            emp_yr INTEGER NOT NULL
        );
        CREATE TABLE {table_names.ein_data} (
            prdn TEXT NOT NULL,
            grant_yr INTEGER NOT NULL,
            ass_seq INTEGER NOT NULL,
            ein TEXT,
            firmid TEXT NOT NULL,
            crosswalk_yr INTEGER NOT NULL,
            pass_no INTEGER NOT NULL
        );
        CREATE TABLE {table_names.assignee_info} (
            prdn TEXT NOT NULL,
            ass_seq INTEGER NOT NULL,
            ass_type TEXT,
            ass_st TEXT,
            ass_ctry TEXT
        );
        CREATE TABLE {table_names.prdn_metadata} (
            prdn TEXT NOT NULL,
            grant_yr INTEGER NOT NULL,
            app_yr INTEGER,
            num_assg INTEGER,
            us_inventor_flag INTEGER
        );
    ''')
    shared_code.import_data(database_name, table_names.pik_data, file_names.pik_data_csvfile)
    shared_code.import_data(database_name, table_names.ein_data, file_names.ein_data_csvfile)
    shared_code.import_data(database_name, table_names.assignee_info, file_names.assignee_info_csvfile)
    shared_code.import_data(database_name, table_names.prdn_metadata, file_names.prdn_metadata_csvfile)


def make_a_models(database_name):
    """

    """
    conn = sqlite3.connect(database_name)
    cur = conn.cursor()
    in_data_tables(cur, database_name)
