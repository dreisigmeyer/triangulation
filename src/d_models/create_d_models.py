import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


def make_aux_tables(fh):
    """

    """
    fh.write(
        f'''
CREATE TABLE {table_names.name_match_prdn_assg_num} (
    {columns.prdn.cmd},
    {columns.assg_seq.cmd},
    UNIQUE (
        {columns.prdn.name},
        {columns.assg_seq.name}
    )
);

CREATE TABLE {table_names.firmid_seq_data} (
    {columns.prdn.cmd},
    {columns.assg_seq.cmd},
    {columns.firmid.cmd},
    UNIQUE (
        {columns.prdn.name},
        {columns.assg_seq.name},
        {columns.firmid.name}
    )
);

CREATE TABLE {table_names.firmid_name_data} (
    {columns.firm_name.cmd},
    {columns.firmid.cmd},
    UNIQUE (
        {columns.firm_name.name},
        {columns.firmid.name}
    )
);

INSERT INTO name_match_prdn_assg_num
SELECT DISTINCT xml_pat_num, assg_num
FROM name_match;

INSERT INTO firmid_seq_data
SELECT DISTINCT prdn, assg_seq, firmid
FROM a1_models;

INSERT INTO firmid_seq_data
SELECT DISTINCT prdn, assg_seq, firmid
FROM a2_models;

INSERT INTO firmid_seq_data
SELECT DISTINCT prdn, assg_seq, firmid
FROM a3_models;

INSERT INTO firmid_name_data
SELECT DISTINCT assignee_name_data.name, firmid_seq_data.firmid
FROM assignee_name_data, firmid_seq_data
WHERE
    assignee_name_data.xml_pat_num = firmid_seq_data.prdn AND
    assignee_name_data.assg_num = firmid_seq_data.assg_seq;

DELETE FROM firmid_name_data
WHERE firm_name IN (
    SELECT firm_name
    FROM firmid_name_data
    GROUP BY firm_name
    HAVING COUNT(*) > 1
);

DELETE FROM firmid_name_data
WHERE firm_name IN (
    SELECT firm_name
    FROM firmid_name_data
    WHERE firmid = ''
);
    ''')


def generate_d_model_sql_script(sql_script_fn, assignee_years):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        shared_code.in_data_tables(f, 'D', assignee_years)
        make_aux_tables(f)
