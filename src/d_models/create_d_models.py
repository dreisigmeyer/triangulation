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

INSERT INTO {table_names.name_match_prdn_assg_num}
SELECT DISTINCT
    {columns.xml_pat_num.name},
    {columns.assg_seq.name}
FROM
    {table_names.name_match};

INSERT INTO {table_names.firmid_seq_data}
SELECT DISTINCT
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.firmid.name}
FROM {table_names.a1_models};

INSERT INTO {table_names.firmid_seq_data}
SELECT DISTINCT
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.firmid.name}
FROM {table_names.a2_models};

INSERT INTO {table_names.firmid_seq_data}
SELECT DISTINCT
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.firmid.name}
FROM {table_names.a3_models};

INSERT INTO {table_names.firmid_name_data}
SELECT DISTINCT
    {table_names.assignee_name_data}.{columns.assg_name.name},
    {table_names.firmid_seq_data}.{columns.firmid.name}
FROM
    {table_names.assignee_name_data},
    {table_names.firmid_seq_data}
WHERE
    {table_names.assignee_name_data}.{columns.xml_pat_num.name} = {table_names.firmid_seq_data}.{columns.prdn.name} AND
    {table_names.assignee_name_data}.{columns.assg_seq.name} = {table_names.firmid_seq_data}.{columns.assg_seq.name};

DELETE FROM {table_names.firmid_name_data}
WHERE {columns.firm_name.name} IN (
    SELECT {columns.firm_name.name}
    FROM {table_names.firmid_name_data}
    GROUP BY {columns.firm_name.name}
    HAVING COUNT(*) > 1
);

DELETE FROM {table_names.firmid_name_data}
WHERE {columns.firm_name.name} IN (
    SELECT {columns.firm_name.name}
    FROM {table_names.firmid_name_data}
    WHERE {columns.firmid.name} = ''
);
    ''')


def generate_d_model_sql_script(sql_script_fn, assignee_years):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        shared_code.in_data_tables(f, 'D', assignee_years)
        make_aux_tables(f)
