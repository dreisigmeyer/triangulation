import sqlite3
import triangulation.src.shared_sql_columns as columns
import triangulation.src.shared_sql_names as names
from triangulation.src.shared_sql_code import import_data, output_distinct_data


def find_closed_loops():
    '''Finds the closed paths for potential A models.
    '''

    # connect to the database
    conn = sqlite3.connect(names.database_name)
    cur = conn.cursor()

    # create the initial tables
    cur.execute(f'''
        CREATE TABLE {names.name_match} (
            {columns.xml_pat_num.cmd},
            {columns.uspto_pat_num.cmd},
            {columns.assg_seq.cmd},
            {columns.grant_yr.cmd},
            {columns.zip3_flag.cmd},
            {columns.ein.cmd},
            {columns.firmid.cmd},
            {columns.pass_no.cmd},
            {columns.br_yr.cmd}
        );
        ''')
    import_data(names.database_name, names.name_match, names.name_match_csvfile)

    cur.execute(f'''
        DELETE FROM {names.name_match}
        WHERE
            ({columns.br_yr.name} < {columns.grant_yr.name} - 2 OR
                {columns.br_yr.name} > {columns.grant_yr.name} + 2) OR
            ({columns.ein.name} = "" AND {columns.firmid.name} = "") OR
            ({columns.ein.name} = "000000000" AND {columns.firmid.name} = "");
            ''')

    cur.execute(f'''
        CREATE TABLE {names.prdns_assgs}
        (
            {columns.prdn.cmd},
            {columns.ein.cmd},
            {columns.firmid.cmd},
            {columns.assg_seq.cmd},
            {columns.br_yr.cmd},
            {columns.pass_no.cmd},
            {columns.grant_yr.cmd}
        );
        ''')
    cur.execute(f'''
        INSERT INTO {names.prdns_assgs}
        SELECT
            {columns.xml_pat_num.name},
            {columns.ein.name},
            {columns.firmid.name},
            {columns.assg_seq.name},
            {columns.br_yr.name},
            {columns.pass_no.name},
            {columns.grant_yr.name}
        FROM {names.name_match};
        ''')
    cur.execute(f'DROP TABLE name_match;')

    cur.execute(f'''
        CREATE TABLE {names.prdn_eins} (
            {columns.prdn.cmd},
            {columns.grant_yr.cmd}
            {columns.assg_seq.cmd},
            {columns.ein.cmd},
            {columns.firmid.cmd},
            {columns.cw_yr.cmd},
            {columns.pass_no.cmd}
        );
        ''')
    cur.execute(f'''
        INSERT INTO {names.prdn_eins}
        SELECT
            {columns.prdn.name},
            {columns.grant_yr.name}
            {columns.assg_seq.name},
            "",
            {columns.firmid.name},
            {columns.br_yr.name},
            {columns.pass_no.name},
        FROM
            {names.prdns_assgs}
        WHERE
            {names.prdns_assgs}.{columns.ein.name} = "" OR
            {names.prdns_assgs}.{columns.ein.name} = "000000000";
    ''')
    cur.execute(f'''
        INSERT INTO {names.prdn_eins}
        SELECT
            {columns.prdn.name},
            {columns.grant_yr.name}
            {columns.assg_seq.name},
            {columns.ein.name},
            {columns.firmid.name},
            {columns.br_yr.name},
            {columns.pass_no.name},
        FROM
            {names.prdns_assgs}
        WHERE
            {names.prdns_assgs}.{columns.ein.name} != "" AND
            {names.prdns_assgs}.{columns.ein.name} != "000000000";
    ''')

    output_distinct_data(names.database_name, names.prdn_eins, names.ein_data_csvfile)
