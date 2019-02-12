from sqlalchemy import Column, Integer, Text


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
