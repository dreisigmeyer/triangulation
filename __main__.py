# import triangulation.src.create_loops as create_loops
import argparse
import random
import string
import triangulation.src.a_models.create_a_models as a_models
import triangulation.src.b_models.create_b_models as b_models
import triangulation.src.c_models.create_c_models as c_models
import triangulation.src.d_models.create_d_models as d_models
import triangulation.src.e_models.create_e_models as e_models
import triangulation.src.f_models.create_f_models as f_models
import triangulation.src.crosswalk.create_crosswalk as crosswalk
import triangulation.src.shared_code.file_names as file_names


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
assignee_years = f'{file_names.in_data_path}{args.assignee_years}'

# Local dummy variables - can feely change as needed
a_models.generate_a_model_sql_script(file_names.a_models_sql_script)
b_models.generate_b_model_sql_script(file_names.b_models_sql_script)
c_models.generate_c_model_sql_script(file_names.c_models_sql_script)
e_models.generate_e_model_sql_script(file_names.e_models_sql_script)
d_models.generate_d_model_sql_script(file_names.d_models_sql_script, assignee_years)
f_models.generate_f_model_sql_script(file_names.f_models_sql_script, assignee_years)
crosswalk.generate_crosswalk_sql_script(file_names.crosswalk_sql_script)
