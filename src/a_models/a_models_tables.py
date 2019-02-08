from triangulation.src.shared_sql_code import DatabaseTableInfo
from triangulation.src.shared_sql_names import database_name
import triangulation.src.shared_sql_names as shared_sql_names


pik_data_name = 'pik_data'
pik_data = DatabaseTableInfo(
    tbl_name=pik_data_name,
    db_name=database_name,
    csv_file=shared_sql_names.pik_data_csvfile,
    create_stmt='''
        CREATE TABLE {} (
            prdn TEXT NOT NULL,
            grant_yr INTEGER NOT NULL,
            app_yr INTEGER NOT NULL,
            inv_seq INTEGER NOT NULL,
            pik TEXT NOT NULL,
            ein TEXT NOT NULL,
            firmid TEXT NOT NULL,
            emp_yr INTEGER NOT NULL
        );'''
)

ein_data_name = 'ein_data'
ein_data = DatabaseTableInfo(
    tbl_name=ein_data_name,
    db_name=database_name,
    csv_file=shared_sql_names.ein_data_csvfile,
    create_stmt='''
        CREATE TABLE {} (
            prdn TEXT NOT NULL,
            grant_yr INTEGER NOT NULL,
            ass_seq INTEGER NOT NULL,
            ein TEXT,
            firmid TEXT NOT NULL,
            crosswalk_yr INTEGER NOT NULL,
            pass_no INTEGER NOT NULL
        );'''
)

assignee_info_name = 'assignee_info'
assignee_info = DatabaseTableInfo(
    tbl_name=assignee_info_name,
    db_name=database_name,
    csv_file=shared_sql_names.assignee_info_csvfile,
    create_stmt='''
        CREATE TABLE {} (
            prdn TEXT NOT NULL,
            ass_seq INTEGER NOT NULL,
            ass_type TEXT,
            ass_st TEXT,
            ass_ctry TEXT
        );'''
)

prdn_metadata_name = 'prdn_metadata'
prdn_metadata = DatabaseTableInfo(
    tbl_name=prdn_metadata_name,
    db_name=database_name,
    csv_file=shared_sql_names.prdn_metadata_csvfile,
    create_stmt='''
        CREATE TABLE {} (
            prdn TEXT NOT NULL,
            grant_yr INTEGER NOT NULL,
            app_yr INTEGER,
            num_assg INTEGER,
            us_inventor_flag INTEGER
        );'''
)
