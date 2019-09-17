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


def generate_b_model_sql_script(sql_script_fn):
    """

    """
    with open(sql_script_fn, 'w') as f:
        b_model_header(f)
