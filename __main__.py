import triangulation.src.create_loops as create_loops
import triangulation.src.a_models.create_a_models as a_models
import triangulation.src.b_models.create_b_models as b_models
import triangulation.src.c_models.create_c_models as c_models
import triangulation.src.shared_code.file_names as file_names
# import triangulation.src.shared_code.shared_code as shared_code


db_name = 'prdn.db'

# shared_code.unique_sort_with_replacement(file_names.prdn_metadata)
# shared_code.read_unique_csv_columns(file_names.iops, [0, 3], file_names.iops_unique)
# create_loops.generate_closed_loops_sql_script(file_names.create_loops_sql_script)
a_models.generate_a_model_sql_script(file_names.a_models_sql_script)
b_models.generate_b_model_sql_script(file_names.b_models_sql_script)
# c_models.generate_c_model_sql_script(file_names.c_models_sql_script)
