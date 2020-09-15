import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


models_and_tables = {
    file_names.a1_models: table_names.a1_models,
    file_names.a2_models: table_names.a2_models,
    file_names.a3_models: table_names.a3_models,
    file_names.b1_models: table_names.b1_models,
    file_names.b2_models: table_names.b2_models,
    file_names.c1_models: table_names.c1_models,
    file_names.c2_models: table_names.c2_models,
    file_names.c3_models: table_names.c3_models,
    file_names.d1_models: table_names.d1_models,
    file_names.d2_models: table_names.d2_models,
    file_names.e1_models: table_names.e1_models,
    file_names.e2_models: table_names.e2_models,
    file_names.f_models: table_names.f_models
}


def import_other_models(fh):
    """

    """
    for model, table in models_and_tables:
        fh.write(
            f'''
.import model table
            ''')


def generate_crosswalk_sql_script(sql_script_fn):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        import_other_models(f)