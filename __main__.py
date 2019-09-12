import triangulation.src.a_models.create_a_models as a_models
import triangulation.src.b_models.create_b_models as b_models
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code


db_name = 'prdn.db'

shared_code.unique_sort_with_replacement(file_names.prdn_metadata)
shared_code.read_unique_csv_columns(file_names.iops, [0, 3], file_names.iops_unique)
conn, cur = a_models.make_a_models(db_name)
b_models.make_a_models(db_name, cur)
