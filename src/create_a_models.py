import sqlite3
import triangulation.src.shared_sql_columns as columns
import triangulation.src.shared_sql_names as names
from triangulation.src.shared_sql_code import import_data, output_data


def closed_paths(cur, join_cols, model):
    '''Finds the closed paths for the A models.

    cur -- cursor into the SQLite database
    join_cols -- DatabaseColumn classes of columns to join on
    model -- model type as string
    '''

    # create the table
    cp_create = f'''
        CREATE TABLE {names.closed_paths_TB} AS
        SELECT
            {names.ein_data}.{columns.prdn.name},
            {names.pik_data}.{columns.app_yr.name},
            {names.ein_data}.{columns.grant_yr.name},
            {names.ein_data}.{columns.assg_seq.name},
            {names.pik_data}.{columns.inv_seq.name},
            {names.pik_data}.{columns.pik.name},
            {names.ein_data}.{columns.cw_yr.name},
            {names.pik_data}.{columns.emp_yr.name},
            {names.pik_data}.{columns.ein.name} AS {columns.pik_ein.name},
            {names.ein_data}.{columns.ein.name} AS {columns.assg_ein.name},
            {names.pik_data}.{columns.firmid.name} AS {columns.pik_firmid.name},
            {names.ein_data}.{columns.firmid.name} AS {columns.assg_firmid.name},
            {names.ein_data}.{columns.pass_no.name}
        FROM {names.pik_data}
        INNER JOIN {names.ein_data}
        USING ({",".join([x.name for x in join_cols])})
        WHERE {names.ein_data}.{columns.firmid.name} != \'\';
    '''
    cur.execute(cp_create)

    # put an index on it
    cp_indx = f'''
        CREATE INDEX
            {names.closed_paths_TB}_idx
        ON
            {names.closed_paths_TB}({columns.prdn.name}, {columns.assg_seq.name});
    '''
    cur.execute(cp_indx)

    # create and populate new columns
    cp_new_columns = f'''
        ALTER TABLE
            {names.closed_paths_TB}
        ADD COLUMN
            {columns.assg_ctry.name};
        UPDATE
            {names.closed_paths_TB}
        SET
            {columns.assg_ctry.name} =
            (
                SELECT {columns.assg_ctry.name}
                FROM {names.assignee_info}
                WHERE
                    {names.assignee_info}.{columns.prdn.name} = {names.closed_paths_TB}.{columns.prdn.name} AND
                    {names.assignee_info}.{columns.assg_seq.name} = {names.closed_paths_TB}.{columns.assg_seq.name}
            );
        ALTER TABLE
            {names.closed_paths_TB}
        ADD COLUMN
            {columns.assg_st.name};
        UPDATE
            {names.closed_paths_TB}
        SET
            {columns.assg_st.name} =
            (
                SELECT {columns.assg_st.name}
                FROM {names.assignee_info}
                WHERE
                    {names.assignee_info}.{columns.prdn.name} = {names.closed_paths_TB}.{columns.prdn.name} AND
                    {names.assignee_info}.{columns.assg_seq.name} = {names.closed_paths_TB}.{columns.assg_seq.name}
            );
        ALTER TABLE
            {names.closed_paths_TB}
        ADD COLUMN
            {columns.assg_type.name};
        UPDATE
            {names.closed_paths_TB}
        SET
            {columns.assg_type.name} =
            (
                SELECT {columns.assg_type.name}
                FROM {names.assignee_info}
                WHERE
                    {names.assignee_info}.{columns.prdn.name} = {names.closed_paths_TB}.{columns.prdn.name} AND
                    {names.assignee_info}.{columns.assg_seq.name} = {names.closed_paths_TB}.{columns.assg_seq.name}
            );
        ALTER TABLE
            {names.closed_paths_TB}
        ADD COLUMN
            {columns.us_inv_flag.name};
        UPDATE
            {names.closed_paths_TB}
        SET
            {columns.us_inv_flag.name} =
            (
                SELECT {columns.us_inv_flag.name}
                FROM {names.prdn_metadata}
                WHERE
                    {names.prdn_metadata}.{columns.prdn.name} = {names.closed_paths_TB}.{columns.prdn.name}
            );
        ALTER TABLE
            {names.closed_paths_TB}
        ADD COLUMN
            {names.mult_assg_flag};
        UPDATE
            {names.closed_paths_TB}
        SET
            {names.mult_assg_flag} =
            (
                SELECT {columns.num_assg.name}
                FROM {names.prdn_metadata}
                WHERE
                    {names.prdn_metadata}.{columns.prdn.name} = {names.closed_paths_TB}.{columns.prdn.name}
            );
    '''
    cur.executescript(cp_new_columns)

    # extract the final closed loops
    # this uses a window function and requires SQLite >=v3.25.0
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
        FROM {names.closed_paths_TB}
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

    # write the results to a CSV file
    output_data(names.database_name, names.closed_loops_TB, f'{model}{names.models}')

    # postprocess the database: prdn_as_Amodel is dropped so we don't have it in shared_sql_names
    postprocess_query = f'''
        DROP TABLE {names.closed_loops_TB};
        CREATE TABLE prdn_as_Amodel AS
        SELECT DISTINCT {columns.prdn.name}, {columns.assg_seq.name}
        FROM {names.closed_paths_TB};

        CREATE INDEX
            indx_2
        ON
            prdn_as_Amodel({columns.prdn.name}, {columns.assg_seq.name});

        DELETE FROM {names.ein_data}
        WHERE EXISTS (
            SELECT *
            FROM prdn_as_Amodel
            WHERE prdn_as_Amodel.{columns.prdn.name} = {names.ein_data}.{columns.prdn.name}
            AND prdn_as_Amodel.{columns.assg_seq.name} = {names.ein_data}.{columns.assg_seq.name}
        );

        DROP TABLE prdn_as_Amodel;
        DROP TABLE {names.closed_paths_TB};
        VACUUM;
    '''
    cur.executescript(postprocess_query)


def find_closed_paths():
    '''Finds the closed paths for potential A models.
    '''

    # connect to the database
    conn = sqlite3.connect(names.database_name)
    cur = conn.cursor()

    # create the initial tables
    cur.execute(f'''
    CREATE TABLE {names.pik_data} (
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
    CREATE TABLE {names.ein_data} (
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
    CREATE TABLE {names.assignee_info} (
        {columns.prdn.cmd},
        {columns.assg_seq.cmd},
        {columns.assg_type.cmd},
        {columns.assg_st.cmd},
        {columns.assg_ctry.cmd}
    );
    ''')
    cur.execute(f'''
    CREATE TABLE {names.prdn_metadata} (
        {columns.prdn.cmd},
        {columns.grant_yr.cmd},
        {columns.app_yr.cmd},
        {columns.num_assg.cmd},
        {columns.us_inv_flag.name.cmd},
    );
    ''')

    # load the data files
    import_data(names.database_name, names.pik_data, names.pik_data_csvfile)
    import_data(names.database_name, names.ein_data, names.ein_data_csvfile)
    import_data(names.database_name, names.assignee_info, names.assignee_info_csvfile)
    import_data(names.database_name, names.prdn_metadata, names.prdn_metadata_csvfile)

    # index the tables
    cur.execute('pragma temp_store = MEMORY;')
    cur.execute(f'''
    CREATE INDEX
        ein_idx_prdn_ein_firmid_idx
    ON
        {names.ein_data}({columns.prdn.name}, {columns.ein.name}, {columns.firmid.name});
    CREATE INDEX
        pik_idx_prdn_ein_firmid_idx
    ON
        {names.pik_data}({columns.prdn.name}, {columns.ein.name}, {columns.firmid.name});
    CREATE INDEX
        ein_idx_prdn_as_idx
    ON
        {names.ein_data}({columns.prdn.name}, {columns.assg_seq.name});
    CREATE INDEX
        assg_info_prdn_as_idx
    ON
        {names.assignee_info}({columns.prdn.name}, {columns.assg_seq.name});
    CREATE INDEX
        prdn_metadata_main_idx
    ON
        {names.prdn_metadata}({columns.prdn.name});
    '''.replace("'", ""))

    # find the closed paths
    closed_paths(cur, [columns.prdn, columns.ein, columns.firmid], 'A1')  # A1 models
    closed_paths(cur, [columns.prdn, columns.firmid], 'A2')  # A2 models
    closed_paths(cur, [columns.prdn, columns.ein], 'A3')  # A3 models
