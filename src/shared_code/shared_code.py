import itertools
import pandas as pd
import shutil
import subprocess
import uuid


"""
Code for working with CSV files
"""


def read_unique_csv_columns(in_filename, cols, out_file=None):
    """

    """
    df = pd.read_csv(in_filename, usecols=cols)
    if out_file:
        df.to_csv(out_file, sep=',', encoding='utf-8')
    else:
        return df.drop_duplicates()


def unique_sort_with_replacement(in_filename):
    """

    """
    name = str(uuid.uuid4())
    with open(in_filename, 'r') as f:
        with open(name, 'w') as holder:
            holder.writelines(unique_everseen(f))

    shutil.move(name, in_filename)


def unique_everseen(iterable, key=None):
    """
    List unique elements, preserving order. Remember all elements ever seen.
    Taken from: https://docs.python.org/3.7/library/itertools.html
    """
    seen = set()
    seen_add = seen.add
    if key is None:
        for element in itertools.filterfalse(seen.__contains__, iterable):
            seen_add(element)
            yield element
    else:
        for element in iterable:
            k = key(element)
            if k not in seen:
                seen_add(k)
                yield element


"""
Code for working with SQLite3 databases
"""


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
        f'SELECT * FROM {tbl_name};'
    ])


def output_distinct_data(db_name, tbl_name, csv_file):
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
        f'SELECT DISTINCT * FROM {tbl_name};'
    ])
