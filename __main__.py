# import triangulation.src.create_loops as create_loops
import argparse
import random
import string
import triangulation.src.a_models.create_a_models as a_models
import triangulation.src.b_models.create_b_models as b_models
import triangulation.src.c_models.create_c_models as c_models
import triangulation.src.d_models.create_d_models as d_models
import triangulation.src.e_models.create_e_models as e_models
import triangulation.src.shared_code.file_names as file_names
# import triangulation.src.shared_code.shared_code as shared_code


def randomString(stringLength=20):
    """Generate a random string of fixed length.

    stringLength: Length of string (default = 20)"""
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


# Command line arguments - flags needed
parser = argparse.ArgumentParser()
parser.add_argument("-a", "--assignee_years", dest="assignee_years", help="The assignee file.  For example: assignee_76_16.csv")
args = parser.parse_args()
# assignee_years should be located in the directory file_names.in_data_path
assignee_years = '{file_names.in_data_path}{args.assignee_years}'

# Local dummy variables - can feely change as needed
db_name = randomString()

# shared_code.unique_sort_with_replacement(file_names.prdn_metadata)
# shared_code.read_unique_csv_columns(file_names.iops, [0, 3], file_names.iops_unique)
# create_loops.generate_closed_loops_sql_script(file_names.create_loops_sql_script)
a_models.generate_a_model_sql_script(file_names.a_models_sql_script)
b_models.generate_b_model_sql_script(file_names.b_models_sql_script)
c_models.generate_c_model_sql_script(file_names.c_models_sql_script)
e_models.generate_e_model_sql_script(file_names.e_models_sql_script)
d_models.generate_d_model_sql_script(file_names.d_models_sql_script, assignee_years)
