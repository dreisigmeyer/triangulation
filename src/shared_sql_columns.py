class DatabaseColumn():
    '''Column to be used in a database.

    name -- the name of the column
    cmd -- definition of the column where the name is formatted in.  For example
        \'{} INTEGER NOT NULL\'
    '''

    def __init__(self, name, cmd):
        self.name = name
        self.cmd = cmd.format(name)


app_yr = DatabaseColumn('app_yr', '{} INTEGER NOT NULL')
assg_ctry = DatabaseColumn('assg_ctry', '{} TEXT')
assg_seq = DatabaseColumn('assg_seq', '{} INTEGER NOT NULL')
assg_st = DatabaseColumn('assg_st', '{} TEXT')
assg_type = DatabaseColumn('assg_type', '{} TEXT')
cw_yr = DatabaseColumn('crosswalk_yr', '{} INTEGER NOT NULL')
ein = DatabaseColumn('ein', '{} TEXT NOT NULL')
emp_yr = DatabaseColumn('emp_yr', '{} INTEGER NOT NULL')
firmid = DatabaseColumn('firmid', '{} TEXT NOT NULL')
grant_yr = DatabaseColumn('grant_yr', '{} INTEGER NOT NULL')
inv_seq = DatabaseColumn('inv_seq', '{} INTEGER NOT NULL')
num_assg = DatabaseColumn('num_assg', '{} INTEGER')
pass_no = DatabaseColumn('pass_no', '{} INTEGER NOT NULL')
pik = DatabaseColumn('pik', '{} TEXT NOT NULL')
prdn = DatabaseColumn('prdn', '{} TEXT NOT NULL')
us_inventor_flag = DatabaseColumn('us_inventor_flag', '{} INTEGER')
