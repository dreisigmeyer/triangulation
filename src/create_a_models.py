from sqlalchemy import create_engine, Index, Table, MetaData
import triangulation.src.shared_sql_names as shared_sql_names
import triangulation.src.shared_sql_columns as columns


metadata = MetaData()
engine = create_engine(f'sqlite:///{shared_sql_names.database_name}', echo=False)

pik_data = Table(
    'pik_data', metadata,
    columns.prdn,
    columns.grant_yr,
    columns.app_yr,
    columns.inv_seq,
    columns.pik,
    columns.ein,
    columns.firmid,
    columns.emp_yr,
    Index('pik_idx_prdn_ein_firmid', "prdn", "ein", "firmid"),
)

ein_data = Table(
    'ein_data', metadata,
    columns.prdn,
    columns.grant_yr,
    columns.assg_seq,
    columns.ein,
    columns.firmid,
    columns.cw_yr,
    columns.pass_no,
    Index('ein_idx_prdn_ein_firmid', "prdn", "ein", "firmid"),
    Index('ein_idx_prdn_as', "prdn", "assg_seq"),
)

assignee_info = Table(
    'assignee_info', metadata,
    columns.prdn,
    columns.assg_seq,
    columns.assg_type,
    columns.assg_st,
    columns.assg_ctry,
    Index('assg_idx_prdn_as', "prdn", "assg_seq"),
)

prdn_metadata = Table(
    'prdn_metadata', metadata,
    columns.prdn,
    columns.grant_yr,
    columns.app_yr,
    columns.num_assg,
    columns.us_inventor_flag,
    Index('prdn_metadata_main_idx', "prdn"),
)

metadata.create_all(engine)
conn = engine.connect()
conn.execute("pragma temp_store = MEMORY;")
