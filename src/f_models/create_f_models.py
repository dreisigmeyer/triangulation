import os
import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


# def create_d2_name_maps(fh):
#     """

#     """
#     fh.write(
#         f'''
# CREATE TABLE {table_names.d2_prdn_seq_name} AS
# SELECT
#     {columns.prdn.name},
#     {columns.assg_seq.name},
#     {columns.firmid.name},
#     {columns.assg_name.name}
# FROM
#     {table_names.possible_d_models},
#     {table_names.assg_name_firmid}
# WHERE
#     {table_names.assg_name_firmid}.{columns.firmid.name} IS NOT NULL AND
#     {table_names.possible_d_models}.{columns.assg_name.name} IN (SELECT {columns.assg_name.name} FROM {table_names.big_firm_names}) AND
#     {table_names.possible_d_models}.{columns.grant_yr.name} = {table_names.assg_name_firmid}.{columns.year.name} AND
#     {table_names.possible_d_models}.{columns.assg_name.name} = {table_names.assg_name_firmid}.{columns.assg_name.name};

# CREATE TABLE {table_names.d2_assg_info} (
#     {columns.xml_pat_num.cmd},
#     {columns.uspto_pat_num.cmd},
#     {columns.grant_yr.cmd},
#     {columns.app_yr.cmd},
#     {columns.assg_name_xml.cmd},
#     {columns.assg_seq.cmd},
#     {columns.assg_type.cmd},
#     {columns.assg_city.cmd},
#     {columns.assg_st.cmd},
#     {columns.assg_ctry.cmd},
#     {columns.assg_name_raw.cmd},
#     {columns.assg_name_clean.cmd},
#     {columns.zip3.cmd},
#     {columns.assg_st_inferred.cmd},
#     {columns.assg_name_inferred.cmd}
# );
#     ''')

#     for file in os.listdir(f'{file_names.assignee_out_data}'):
#         if file.endswith(".csv"):
#             file_name = os.path.join(f'{file_names.assignee_out_data}', file)
#             shared_code.import_data(fh, f'{table_names.d2_assg_info}', file_name)


def create_name_information(fh, in_table, in_model_firmid, br_yr):
    """

    """
    fh.write(
        f'''
DROP TABLE IF EXISTS {table_names.name_information};
CREATE TABLE {table_names.name_information} AS
SELECT DISTINCT
    {table_names.prdn_assg_names}.{columns.xml_name.name} AS {columns.xml_name.name},
    {table_names.prdn_assg_names}.{columns.uspto_name.name} AS {columns.uspto_name.name},
    {table_names.prdn_assg_names}.{columns.corrected_name.name} AS {columns.corrected_name.name},
    {table_names.prdn_assg_names}.{columns.name_match_name.name} AS {columns.name_match_name.name},
    {table_names.prdn_assg_names}.{columns.assg_st.name} AS {columns.assg_st.name},
    {table_names.prdn_assg_names}.{columns.assg_ctry.name} AS {columns.assg_ctry.name},
    {in_table}.{br_yr} AS {columns.br_yr.name},
    {in_table}.{columns.firmid.name} AS {in_model_firmid}
FROM
    {table_names.prdn_assg_names},
    {in_table}
WHERE
    {table_names.prdn_assg_names}.{columns.prdn.name} = {in_table}.{columns.prdn.name} AND
    {table_names.prdn_assg_names}.{columns.assg_seq.name} = {in_table}.{columns.assg_seq.name};
        ''')


def create_standard_name_to_firmid(fh):
    """

    """
    fh.write(
        f'''
CREATE TABLE {table_names.standard_name_to_firmid} ()
    {columns.standard_name.cmd},
    {columns.alias_name.cmd},
    {columns.valid_yr.cmd},
    {columns.firmid.cmd},
    {columns.state.cmd},
    {columns.country.cmd},
    {columns.model_origin.cmd},
    {columns.sn_on_prdn_count.cmd},
    {columns.alias_on_prdn_count.cmd}
);
        ''')


def generate_f_model_sql_script(sql_script_fn, assignee_years):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        shared_code.in_data_tables(f, 'F', assignee_years)
        create_standard_name_to_firmid(f)
        create_name_information(f, table_names.a1_models, columns.a1_model_firmid.name, columns.br_yr.name)
        create_name_information(f, table_names.d2_models, columns.d2_model_firmid.name, columns.grant_yr.name)
