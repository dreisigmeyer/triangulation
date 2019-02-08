import triangulation.src.a_models.a_models_tables as a_models_tables
from triangulation.src.shared_sql_code import close_db
from triangulation.src.shared_sql_code import connect_to_db
from triangulation.src.shared_sql_code import DatabaseTable


conn, cursor = connect_to_db()

close_db(conn)
