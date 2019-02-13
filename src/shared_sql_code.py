import subprocess


def import_data(db_name, tbl_name, csv_file):
    '''Helper function to import data from a CSV file into a SQLite3 database.

    db_name -- database name
    tbl_name -- table in database to load data into
    csv_file -- CSV file
    '''
    subprocess.run([
        'sqlite3',
        db_name,
        '.mode csv',
        'pragma temp_store = MEMORY;',
        f'.import {csv_file} {tbl_name}'
    ])


def output_data(db_name, tbl_name, csv_file):
    '''Helper function to outport data to a CSV file from a SQLite3 database.

    db_name -- database name
    tbl_name -- table in database to load data into
    csv_file -- CSV file
    '''
    subprocess.run([
        'sqlite3',
        '--echo',
        '--csv',
        db_name,
        f'.output {csv_file}',
        f'SELECT * FROM {tbl_name};',
    ])
