import sqlite3
import triangulation.src.shared_sql_columns as columns
from triangulation.src.shared_sql_code import import_data, output_data
from triangulation.src.shared_sql_names import *


def closed_paths(cur, join_cols, model):
    '''Finds the closed paths for the A models.

    cur -- cursor into the SQLite database
    join_cols -- DatabaseColumn classes of columns to join on
    model -- model type as string
    '''

    # create the table
    cp_create = f'''
        CREATE TABLE closed_paths_TB AS
        SELECT
            pik_data.prdn AS pik_prdn,
            ein_data.prdn AS assg_prdn,
            pik_data.app_yr AS app_yr,
            ein_data.grant_yr AS grant_yr,
            ein_data.assg_seq AS assg_seq,
            pik_data.inv_seq AS inv_seq,
            pik_data.pik AS pik,
            ein_data.crosswalk_yr AS crosswalk_yr,
            pik_data.emp_yr AS emp_yr,
            pik_data.ein AS pik_ein,
            ein_data.ein AS assg_ein,
            pik_data.firmid AS pik_firmid,
            ein_data.firmid AS assg_firmid,
            ein_data.pass_no AS pass_no,
            pik_data.app_yr AS app_yr
        FROM pik_data
        INNER JOIN ein_data
        USING ({",".join([x.name in join_cols])})
        WHERE ein_data.firmid != \'\';
    '''
    cur.execute(cp_create)

    # put an index on it
    cp_indx = f'''
        CREATE INDEX
            closed_paths_TB_idx
        ON
            closed_paths_TB(assg_prdn, assg_seq);
    '''
    cur.execute(cp_indx)

    # create and populate new columns
    cp_new_columns = f'''
        ALTER TABLE
            closed_paths_TB
        ADD COLUMN
            assignee_country;
        UPDATE
            closed_paths_TB
        SET
            assignee_country =
            (
                SELECT assg_ctry
                FROM assignee_info
                WHERE
                    assignee_info.prdn = closed_paths_TB.assg_prdn AND
                    assignee_info.assg_seq = closed_paths_TB.assg_seq
            );
        ALTER TABLE
            closed_paths_TB
        ADD COLUMN
            assignee_state;
        UPDATE
            closed_paths_TB
        SET
            assignee_state =
            (
                SELECT assg_st
                FROM assignee_info
                WHERE
                    assignee_info.prdn = closed_paths_TB.assg_prdn AND
                    assignee_info.assg_seq = closed_paths_TB.assg_seq
            );
        ALTER TABLE
            closed_paths_TB
        ADD COLUMN
            assignee_type;
        UPDATE
            closed_paths_TB
        SET
            assignee_type =
            (
                SELECT assg_type
                FROM assignee_info
                WHERE
                    assignee_info.prdn = closed_paths_TB.assg_prdn AND
                    assignee_info.assg_seq = closed_paths_TB.assg_seq
            );
        ALTER TABLE
            closed_paths_TB
        ADD COLUMN
            us_inventor_flag;
        UPDATE
            closed_paths_TB
        SET
            us_inventor_flag =
            (
                SELECT us_inventor_flag
                FROM prdn_metadata
                WHERE
                    prdn_metadata.prdn = closed_paths_TB.assg_prdn
            );
        ALTER TABLE
            closed_paths_TB
        ADD COLUMN
            multiple_assignee_flag;
        UPDATE
            closed_paths_TB
        SET
            multiple_assignee_flag =
            (
                SELECT num_assg
                FROM prdn_metadata
                WHERE
                    prdn_metadata.prdn = closed_paths_TB.assg_prdn
            );
    '''
    cur.executescript(cp_new_columns)

    # write the results to a CSV file
    output_data(database_name, 'closed_paths_TB', f'closed_paths_{model}_TB')

    # portprocess the database
    postprocess_query = f'''
        CREATE TABLE prdn_as_Amodel AS
        SELECT DISTINCT assg_prdn AS prdn, assg_seq
        FROM closed_paths_TB;

        CREATE INDEX
            indx_2
        ON
            prdn_as_Amodel(prdn, assg_seq);

        DELETE FROM ein_data
        WHERE EXISTS (
            SELECT *
            FROM prdn_as_Amodel
            WHERE prdn_as_Amodel.prdn = ein_data.prdn
            AND prdn_as_Amodel.assg_seq = ein_data.assg_seq
        );

        DROP TABLE prdn_as_Amodel;
        DROP TABLE closed_paths_TB;
        VACUUM;
    '''
    cur.executescript(postprocess_query)


def find_closed_paths():
    '''Finds the closed paths for potential A models.
    '''

    # connect to the database
    conn = sqlite3.connect(shared_sql_names.database_name)
    cur = conn.cursor()

    # create the initial tables
    cur.execute(f'''
    CREATE TABLE {pik_data} (
        {columns.prdn.cmd},
        {columns.grant_yr.cmd},
        {columns.app_yr.cmd},
        {columns.inv_seq.cmd},
        {columns.pik.cmd},
        {columns.ein.cmd},
        {columns.firmid.cmd},
        {columns.emp_yr.cmd}
    );
    ''')
    cur.execute(f'''
    CREATE TABLE {ein_data} (
        {columns.prdn.cmd},
        {columns.grant_yr.cmd},
        {columns.assg_seq.cmd},
        {columns.ein.cmd},
        {columns.firmid.cmd},
        {columns.cw_yr.cmd},
        {columns.pass_no.cmd}
    );
    ''')
    cur.execute(f'''
    CREATE TABLE {assignee_info} (
        {columns.prdn.cmd},
        {columns.assg_seq.cmd},
        {columns.assg_type.cmd},
        {columns.assg_st.cmd},
        {columns.assg_ctry.cmd}
    );
    ''')
    cur.execute(f'''
    CREATE TABLE {prdn_metadata} (
        {columns.prdn.cmd},
        {columns.grant_yr.cmd},
        {columns.app_yr.cmd},
        {columns.num_assg.cmd},
        {columns.us_inventor_flag.cmd},
    );
    ''')

    # load the data files
    import_data(database_name, pik_data, pik_data_csvfile)
    import_data(database_name, ein_data, ein_data_csvfile)
    import_data(database_name, assignee_info, assignee_info_csvfile)
    import_data(database_name, prdn_metadata, prdn_metadata_csvfile)

    # index the tables
    cur.execute('pragma temp_store = MEMORY;')
    cur.execute(f'''
    CREATE INDEX
        ein_idx_prdn_ein_firmid
    ON
        {ein_data}({columns.prdn.name}, {columns.ein.name}, {columns.firmid.name});
    CREATE INDEX
        pik_idx_prdn_ein_firmid
    ON
        {pik_data}({columns.prdn.name}, {columns.ein.name}, {columns.firmid.name});
    CREATE INDEX
        ein_idx_prdn_as
    ON
        {ein_data}({columns.prdn.name}, {columns.assg_seq.name});
    CREATE INDEX
        assg_info_prdn_as
    ON
        {assignee_info}({columns.prdn.name}, {columns.assg_seq.name});
    CREATE INDEX
        prdn_metadata_main_idx
    ON
        {prdn_metadata}({columns.prdn.name});
    '''.replace("'", ""))

    # find the closed paths
    closed_paths(cur, [columns.prdn, columns.ein, columns.firmid], 'A1')  # A1 models
    closed_paths(cur, [columns.prdn, columns.firmid], 'A2')  # A2 models
    closed_paths(cur, [columns.prdn, columns.ein], 'A3')  # A3 models
