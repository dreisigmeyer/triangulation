import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


def find_closed_loops(fh):
    '''Finds the closed paths for potential A models.
    '''

    # create the initial tables
    fh.write(
        f'''
CREATE TABLE {table_names.name_match} (
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
    shared_code.import_data(fh, file_names.name_match, file_names.name_match_csvfile)

    fh.write(
        f'''
DELETE FROM {table_names.name_match}
WHERE
    ({columns.br_yr.name} < {columns.grant_yr.name} - 2 OR
        {columns.br_yr.name} > {columns.grant_yr.name} + 2) OR
    ({columns.ein.name} = "" AND {columns.firmid.name} = "") OR
    ({columns.ein.name} = "000000000" AND {columns.firmid.name} = "");
CREATE TABLE {table_names.prdns_assgs}
(
    {columns.prdn.cmd},
    {columns.ein.cmd},
    {columns.firmid.cmd},
    {columns.assg_seq.cmd},
    {columns.br_yr.cmd},
    {columns.pass_no.cmd},
    {columns.grant_yr.cmd}
);
INSERT INTO {table_names.prdns_assgs}
SELECT
    {columns.xml_pat_num.name},
    {columns.ein.name},
    {columns.firmid.name},
    {columns.assg_seq.name},
    {columns.br_yr.name},
    {columns.pass_no.name},
    {columns.grant_yr.name}
FROM {table_names.name_match};
DROP TABLE name_match;
CREATE TABLE {table_names.prdn_eins} (
    {columns.prdn.cmd},
    {columns.grant_yr.cmd}
    {columns.assg_seq.cmd},
    {columns.ein.cmd},
    {columns.firmid.cmd},
    {columns.cw_yr.cmd},
    {columns.pass_no.cmd}
);
INSERT INTO {table_names.prdn_eins}
SELECT
    {columns.prdn.name},
    {columns.grant_yr.name}
    {columns.assg_seq.name},
    "",
    {columns.firmid.name},
    {columns.br_yr.name},
    {columns.pass_no.name},
FROM
    {table_names.prdns_assgs}
WHERE
    {table_names.prdns_assgs}.{columns.ein.name} = "" OR
    {table_names.prdns_assgs}.{columns.ein.name} = "000000000";
INSERT INTO {table_names.prdn_eins}
SELECT
    {columns.prdn.name},
    {columns.grant_yr.name}
    {columns.assg_seq.name},
    {columns.ein.name},
    {columns.firmid.name},
    {columns.br_yr.name},
    {columns.pass_no.name},
FROM
    {table_names.prdns_assgs}
WHERE
    {table_names.prdns_assgs}.{columns.ein.name} != "" AND
    {table_names.prdns_assgs}.{columns.ein.name} != "000000000";
    ''')

    shared_code.output_distinct_data(fh, file_names.prdn_eins, file_names.ein_data_csvfile)
