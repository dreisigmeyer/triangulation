import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


def generate_d_model_sql_script(sql_script_fn):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        import_other_models(f)


def import_other_models(fh, assignee_years):
    """
    """
    fh.write(
        f'''
.import {file_names.a1_models} a1_models
.import {file_names.a2_models} a2_models
.import {file_names.a3_models} a3_models
.import {file_names.b1_models} b1_models
.import {file_names.b2_models} b2_models
.import {file_names.c1_models} c1_models
.import {file_names.c2_models} c2_models
.import {file_names.c3_models} c3_models
.import {file_names.e1_models} e1_models
.import {file_names.e2_models} e2_models
.import {assignee_years} assignee_name_data
    ''')
