import sqlite3
from odo import drop, odo
from triangulation.src.shared_sql_names import database_name


def close_db(conn):
    '''Closes the connection to a database

    conn -- the connection to the database
    '''
    assert(type(conn) is sqlite3.Connection, 'conn is not a sqlite3.Connection')
    conn.close()


def connect_to_db():
    '''Establishes a connection to a database and runs some preliminary setup commands.

    Returns the connection and cursor to the database.
    '''
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()
    # /tmp was filling up
    cursor.execute(f'''pragma temp_store = MEMORY;''')
    return conn, cursor


class DatabaseTableInfo():
    '''Collects together information for a table.  This is used as a helper class.
    '''

    def __init__(self, tbl_name, db_name, create_stmt, csv_file=''):
        '''
        create_stmt (string) -- CREATE TABLE statement.
            This should have a {} for the table name which will be formated using table_name.
        table_name (string) -- TABLE to load data into
        db_name (string) -- database to load the CSV file into
        csv_file (string, optional) -- CSV file to load into the database
        '''
        assert type(tbl_name) is str, f'{tbl_name} is not a str'
        assert type(db_name) is str, f'{db_name} is not a str'
        assert type(create_stmt) is str, f'{create_stmt} is not a str'
        assert type(csv_file) is str, f'{csv_file} is not a str'

        self.tbl_name = tbl_name
        self.db_name = db_name
        self.create_stmt = create_stmt.format(tbl_name)
        self.csv_file = csv_file


class DatabaseTable(DatabaseTableInfo):
    '''Collects together information for a table.  This actually does things with the table.
    '''

    def __init__(self, conn, cursor, dbti_cls):
        '''
        conn (sqlite3.Connection) -- database connection
        cursor (sqlite3.Cursor) -- cursor into the database
        dbti_cls -- a DatabaseTableInfo instance
        '''
        assert type(conn) is sqlite3.Connection, f'{conn} is not a sqlite3.Connection'
        assert type(cursor) is sqlite3.Cursor, f'{cursor} is not a sqlite3.Cursor'
        assert type(dbti_cls) is DatabaseTableInfo, f'{dbti_cls} is not a DatabaseTableInfo'

        self.conn = conn
        self.cursor = cursor
        super().__init__(dbti_cls.tbl_name, dbti_cls.db_name, dbti_cls.create_stmt, dbti_cls.csv_file)

    def create_table(self):
        '''Create a table.  If a CSV file is associated with the table it is loaded.
        '''
        self.cursor.execute(self.create_stmt)
        if self.csv_file:
            sqlite3_str = f'sqlite:///{self.db_name}::{self.tbl_name}'
            odo(self.csv_file, sqlite3_str)

    def drop_table(self):
        '''Drops a table from a database
        '''
        sqlite3_str = f'sqlite:///{self.db_name}::{self.table_name}'
        drop(sqlite3_str)
