import sys, os, util_test,subprocess

## Print Script
print(f"Starting - {os.path.basename(__file__)}")

## Get Test Settings
util_test.set_env()
#

port=int(os.getenv("EDGE_START_PORT",6432))
usr=os.getenv("EDGE_USERNAME","lcusr")
pw=os.getenv("EDGE_PASSWORD","password")
db=os.getenv("EDGE_DB","demo")
host=os.getenv("EDGE_HOST","localhost")
dbname=os.getenv("EDGE_DB","lcdb")
cluster_dir = os.getenv("EDGE_CLUSTER_DIR")


#Test setup: CREATE table 
row = util_test.write_psql("CREATE TABLE IF NOT EXISTS foo3 (employeeID bigserial PRIMARY KEY,employeeName VARCHAR(40),employeeMail VARCHAR(40))",host,dbname,port,pw,usr)
print(row)
print("*"*100)

#INSERT data
row = util_test.write_psql("INSERT INTO foo3 (employeeID,employeeName,employeeMail) VALUES(1,'Carol','carol@pgedge.com'),(2,'Bob','bob@pgedge.com')",host,dbname,port,pw,usr)
print("*"*100)

#Check data
row = util_test.read_psql("SELECT * FROM foo3",host,dbname,port,pw,usr)
print(row)
print("*"*100)

#convert to SnowFlake Sequence
cmd_node = f"spock sequence-convert public.foo3_employeeid_seq {dbname}"
sequence=util_test.run_cmd("SnowFlake Conversion", cmd_node, f"{cluster_dir}/n1")
print(sequence.stdout)
print("*" * 100)

#INSERT data
row = util_test.write_psql("INSERT INTO foo3 (employeeName,employeeMail) VALUES('Mary','mary@pgedge.com'),('George','george@pgedge.com')",host,dbname,port,pw,usr)
print("*"*100)

#Check data
row = util_test.read_psql("SELECT * FROM foo3",host,dbname,port,pw,usr)
print(row)
print("*"*100)

####################################################
## Check the results returned by snowflake functions
####################################################

# Check the snowflake.nextval:

nextval = util_test.read_psql("SELECT * FROM snowflake.nextval('foo3_employeeid_seq'::regclass)",host,dbname,port,pw,usr)
print(nextval)
for x in ('[',']'):
    temp_nextval = nextval.replace(x, '')
    nextval = temp_nextval
nextval_str = str(nextval)
print(f"Updated nextval contains: {nextval_str}")

if not nextval:
    util_test.EXIT_FAIL

# Check the get_epoch command:

cmd = f"SELECT snowflake.get_epoch({nextval_str})::text"
print(cmd)
row = util_test.read_psql(cmd,host,dbname,port,pw,usr)
print(f".get_epoch returns: {row}")
print("*"*100)

if not row:
    util_test.EXIT_FAIL

# Check the to_timestamp functionality:

cmd = f"SELECT to_timestamp(snowflake.get_epoch({nextval_str}))::text"
print(cmd)
timestamp = util_test.read_psql(cmd,host,dbname,port,pw,usr)
print(f"to_timestamp from .get_epoch returns: {timestamp}")
print("*"*100)

if not timestamp:
    util_test.EXIT_FAIL

# Check the .get_count functionality:

cmd = f"SELECT snowflake.get_count({nextval_str})::text"
print(cmd)
count = util_test.read_psql(cmd,host,dbname,port,pw,usr)
print(f".get_count returns: {count}")
print("*"*100)

if not count:
    util_test.EXIT_FAIL

# Check the .get_node functionality:

cmd = f"SELECT snowflake.get_node({nextval_str})::text"
print(cmd)
node = util_test.read_psql(cmd,host,dbname,port,pw,usr)
print(f".get_node returns: {node}")
print("*"*100)

if not node:
    util_test.EXIT_FAIL

# Check the snowflake.nextval and then (in the same call, since the trip to util_test.py is considered a separate session)
# the value of snowflake.currval:

nextval_currval = util_test.read_psql("SELECT snowflake.nextval('foo3_employeeid_seq'::regclass), snowflake.currval('foo3_employeeid_seq'::regclass)",host,dbname,port,pw,usr)
print(nextval_currval)
for x in ('[',']'):
    temp_nextval = nextval.replace(x, '')
    nextval = temp_nextval
nextval_str = str(nextval)
print(f"nextval_currval contains: {nextval_str}")
print("*"*100)

if not nextval_currval:
    util_test.EXIT_FAIL

# Check the pg_sequences view for the presence of snowflake/spock sequences:

sequence = util_test.read_psql("SELECT * FROM pg_sequences",host,dbname,port,pw,usr)
print(f"pg_sequences contains: {sequence}")
print("*"*100)

if not sequence:
    util_test.EXIT_FAIL

if "snowflake" in sequence and "foo3_employeeid_seq" in sequence:

    util_test.EXIT_PASS
else:
    util_test.EXIT_FAIL
   
