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


def create_indexes(fh):
    """

    """
    for model, table in models_and_tables.items():
        fh.write(
            f'''
CREATE INDEX {table}_indx
ON {table}(
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.firmid.name}
);
            ''')
    fh.write(
        f'''
CREATE INDEX {table_names.full_frame}_indx
ON {table_names.full_frame}(
    {columns.prdn.name},
    {columns.assg_seq.name}
);
        ''')


def import_other_models(fh):
    """

    """
    fh.write(
        f'''
.headers on''')
    for model, table in models_and_tables.items():
        fh.write(
            f'''
DROP TABLE IF EXISTS {table}
.import {model} {table}
ALTER TABLE {table} ADD COLUMN {columns.f_model.cmd}''')
    fh.write(
        f'''
.import {file_names.full_frame} {table_names.full_frame}
.headers off''')


def import_other_tables(fh):
    """

    """
    fh.write(
        f'''
.headers on
CREATE TABLE {table_names.iops} (
    {columns.prdn.cmd},
    {columns.assg_seq.cmd},
    UNIQUE(
        {columns.prdn.name},
        {columns.assg_seq.name}
    )
);
.import {file_names.iops_unique} {table_names.iops}

CREATE TABLE patent_metadata (
    {columns.prdn.cmd},
    {columns.grant_yr.cmd},
    {columns.app_yr.cmd},
    {columns.num_assg.cmd},
    {columns.us_inv_flag.cmd},
    UNIQUE (
        {columns.prdn.name}
    )
);
.import {file_names.prdn_metadata_csvfile} {table_names.prdn_metadata}
.headers on''')


def generate_crosswalk_sql_script(sql_script_fn):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        import_other_models(f)
        import_other_tables(f)
        create_indexes(f)
