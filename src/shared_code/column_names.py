class DatabaseColumn():
    '''Column to be used in a database.

    name -- the name of the column
    cmd -- definition of the column where the name is formatted in.  For example
        \'{} INTEGER NOT NULL\'
    '''

    def __init__(self, name, cmd):
        self.name = name
        self.cmd = cmd.format(name)


abs_cw_yr = DatabaseColumn('abs_cw_yr', '{} INTEGER')
abs_emp_yr = DatabaseColumn('abs_emp_yr', '{} INTEGER')
abs_yr_diff = DatabaseColumn('abs_yr_diff', '')
app_yr = DatabaseColumn('app_yr', '{} INTEGER NOT NULL')
assg_ctry = DatabaseColumn('assg_ctry', '{} TEXT')
assg_ein = DatabaseColumn('assg_ein', '')
assg_firmid = DatabaseColumn('assg_firmid', '')
assg_name = DatabaseColumn('assg_name', '{} TEXT NOT NULL')
assg_num = DatabaseColumn('assg_num', '')
assg_prdn = DatabaseColumn('assg_prdn', '')
assg_seq = DatabaseColumn('assg_seq', '{} INTEGER NOT NULL')
assg_st = DatabaseColumn('assg_st', '{} TEXT')
assg_type = DatabaseColumn('assg_type', '{} TEXT')
br_yr = DatabaseColumn('br_yr', '{} INTEGER NOT NULL')
cw_yr = DatabaseColumn('cw_yr', '{} INTEGER NOT NULL')
ein = DatabaseColumn('ein', '{} TEXT NOT NULL')
emp_yr = DatabaseColumn('emp_yr', '{} INTEGER NOT NULL')
firmid = DatabaseColumn('firmid', '{} TEXT NOT NULL')
foreign_assg_flag = DatabaseColumn('foreign_assg_flag', '{} INTEGER')
grant_yr = DatabaseColumn('grant_yr', '{} INTEGER NOT NULL')
inv_seq = DatabaseColumn('inv_seq', '{} INTEGER NOT NULL')
assg_ein = DatabaseColumn('assg_ein', '')
model = DatabaseColumn('model', '')
mult_assg_flag = DatabaseColumn('multiple_assignee_flag', '{} INTEGER')
name = DatabaseColumn('name', '')
num_assg = DatabaseColumn('num_assg', '{} INTEGER')
num_inv = DatabaseColumn('num_inv', '{} INTEGER')
pass_num = DatabaseColumn('pass_num', '{} INTEGER NOT NULL')
pik = DatabaseColumn('pik', '{} TEXT NOT NULL')
pik_ein = DatabaseColumn('pik_ein', '')
pik_firmid = DatabaseColumn('pik_firmid', '')
pik_prdn = DatabaseColumn('pik_prdn', '')
prdn = DatabaseColumn('prdn', '{} TEXT NOT NULL')
ui_firm_id = DatabaseColumn('ui_firm_id', '')
uniq_firmid = DatabaseColumn('uniq_firmid', '{} INTEGER')
us_assg_flag = DatabaseColumn('us_assg_flag', '{} INTEGER')
us_inv_flag = DatabaseColumn('us_inv_flag', '{} INTEGER')
uspto_pat_num = DatabaseColumn('uspto_pat_num', '{} TEXT NOT NULL')
xml_pat_num = DatabaseColumn('xml_pat_num', '{} TEXT NOT NULL')
year = DatabaseColumn('year', '{} INTEGER')
yr_diff = DatabaseColumn('yr_diff', '')
zip3_flag = DatabaseColumn('zip3_flag', '{} INTEGER')
