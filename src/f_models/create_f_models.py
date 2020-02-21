import os
import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


def create_d2_name_maps(fh):
    """

    """
    fh.write(
        f'''
CREATE TABLE {table_names.d2_prdn_seq_name} AS
SELECT
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.firmid.name},
    {columns.assg_name.name}
FROM
    {table_names.possible_d_models},
    {table_names.assg_name_firmid}
WHERE
    {table_names.assg_name_firmid}.{columns.firmid.name} IS NOT NULL AND
    {table_names.possible_d_models}.{columns.assg_name.name} IN (SELECT {columns.assg_name.name} FROM {table_names.big_firm_names}) AND
    {table_names.possible_d_models}.{columns.grant_yr.name} = {table_names.assg_name_firmid}.{columns.year.name} AND
    {table_names.possible_d_models}.{columns.assg_name.name} = {table_names.assg_name_firmid}.{columns.assg_name.name};

CREATE TABLE {table_names.d2_assg_info} (
    {columns.xml_pat_num.cmd},
    {columns.uspto_pat_num.cmd},
    {columns.grant_yr.cmd},
    {columns.app_yr.cmd},
    {columns.assg_name_xml.cmd},
    {columns.assg_seq.cmd},
    {columns.assg_type.cmd},
    {columns.assg_city.cmd},
    {columns.assg_st.cmd},
    {columns.assg_ctry.cmd},
    {columns.assg_name_raw.cmd},
    {columns.assg_name_clean.cmd},
    {columns.zip3.cmd},
    {columns.assg_st_inferred.cmd},
    {columns.assg_name_inferred.cmd}
);
    ''')

    for file in os.listdir(f'{file_names.assignee_out_data}'):
        if file.endswith(".csv"):
            file_name = os.path.join(f'{file_names.assignee_out_data}', file)
            shared_code.import_data(fh, f'{table_names.d2_assg_info}', file_name)


def generate_f_model_sql_script(sql_script_fn):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        create_d2_name_maps(f)
