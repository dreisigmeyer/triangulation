# import os
from sqlalchemy import create_engine, Index, Table, MetaData
from sqlalchemy import Column, Integer, Text


# this_file_path = os.path.realpath(__file__)

database_name = 'prdn_db'

pik_data_csvfile = 'FAKE_prdn_piks.csv'

engine = create_engine('postgresql://postgres@/postgres?host=/run/postgresql')
conn = engine.connect()
conn.execute('commit')
conn.execute(f'create database {database_name}')

app_yr = Column('app_yr', Integer, nullable=False)
assg_ctry = Column('assg_ctry', Text)
assg_seq = Column('assg_seq', Integer, nullable=False)
assg_st = Column('assg_st', Text)
assg_type = Column('assg_type', Text)
cw_yr = Column('cw_yr', Integer, nullable=False)
ein = Column('ein', Text, nullable=False)
emp_yr = Column('emp_yr', Integer, nullable=False)
firmid = Column('firmid', Text, nullable=False)
grant_yr = Column('grant_yr', Integer, nullable=False)
inv_seq = Column('inv_seq', Integer, nullable=False)
num_assg = Column('num_assg', Integer)
pass_no = Column('pass_no', Integer, nullable=False)
pik = Column('pik', Text, nullable=False)
prdn = Column('prdn', Text, nullable=False)
us_inventor_flag = Column('us_inventor_flag', Integer)

metadata = MetaData()
engine = create_engine(f'postgresql://efw:efw@/{database_name}?host=/run/postgresql', echo=True)

pik_data = Table(
    'pik_data', metadata,
    prdn,
    grant_yr,
    app_yr,
    inv_seq,
    pik,
    ein,
    firmid,
    emp_yr,
    Index('pik_idx_prdn_ein_firmid', "prdn", "ein", "firmid"),
)

metadata.create_all(engine)
conn = engine.connect()
conn.execute('commit')
conn.execute('COPY pik_data FROM \'/pgdata/FAKE_prdn_piks.csv\' WITH (FORMAT csv);')
