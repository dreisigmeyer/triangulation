import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


def b_model_header(fh):
    """

    """
    fh.write(
        f'''
.mode csv
pragma temp_store = MEMORY;
    ''')


def create_b_models_table(fh):
    """
    Uses a window function and requires SQLite >=v3.25.0
    """
    fh.write(
        f'''
CREATE TABLE {table_names.b_models} AS
SELECT DISTINCT
    {table_names.ein_data}.{columns.prdn.name},
    {table_names.ein_data}.{columns.assg_seq.name},
    {table_names.ein_data}.{columns.firmidname},
    {table_names.ein_data}.{columns.cw_yr.name},
    {table_names.ein_data}.{columns.grant_yr.name}
FROM
    {table_names.ein_data},
    (
        SELECT
            {columns.prdn.name} AS prdn_sq1,
            {columns.assg_seq.name} AS ass_seq_sq1,
            min({columns.pass_num.name}) AS pass_num_sq1
        FROM {table_names.ein_data}
        GROUP BY {columns.prdn.name}, {columns.assg_seq.name}
    ) AS subquery1
WHERE
    {table_names.ein_data}.{columns.firmid} != '' AND
    {table_names.ein_data}.{columns.prdn} = subquery1.prdn_sq1 AND
    {table_names.ein_data}.{columns.ass_seq} = subquery1.ass_seq_sq1 AND
    {table_names.ein_data}.{columns.pass_no} = subquery1.pass_num_sq1;


-- Only want PRDNs without any inventor information: These are the ones that never
-- had a chance to be A models when they grew up.
DELETE FROM {table_names.b_models}
WHERE {columns.prdn.name} IN (
    SELECT DISTINCT {columns.prdn.name}
    FROM {table_names.pik_data}
);
    ''')


def generate_b_model_sql_script(sql_script_fn):
    """

    """
    with open(sql_script_fn, 'w') as f:
        b_model_header(f)
        create_b_models_table(f)
