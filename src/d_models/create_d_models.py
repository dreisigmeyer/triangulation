import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


def generate_d_model_sql_script(sql_script_fn):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        shared_code.in_data_tables(f, 'D')
        import_other_models(f)


def import_other_models(fh, assignee_years):
    """
    """
    fh.write(
        f'''
.import {file_names.a1_models} {table_names.a1_models}
.import {file_names.a2_models} {table_names.a2_models}
.import {file_names.a3_models} {table_names.a3_models}
.import {file_names.b1_models} {table_names.b1_models}
.import {file_names.b2_models} {table_names.b2_models}
.import {file_names.c1_models} {table_names.c1_models}
.import {file_names.c2_models} {table_names.c2_models}
.import {file_names.c3_models} {table_names.c3_models}
.import {file_names.e1_models} {table_names.e1_models}
.import {file_names.e2_models} {table_names.e2_models}
.import {assignee_years} {table_names.assignee_name_data}
    ''')
