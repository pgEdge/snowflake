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


#CREATE table 
row = util_test.write_psql("CREATE TABLE IF NOT EXISTS foo2 (employeeID bigserial PRIMARY KEY,employeeName VARCHAR(40),employeeMail VARCHAR(40))",host,dbname,port,pw,usr)
print(row)
print("*"*100)

#INSERT data
row = util_test.write_psql("INSERT INTO foo2 (employeeID,employeeName,employeeMail) VALUES(1,'Carol','carol@pgedge.com'),(2,'Bob','bob@pgedge.com')",host,dbname,port,pw,usr)
print("*"*100)

#Check data
row = util_test.read_psql("SELECT * FROM foo2",host,dbname,port,pw,usr)
print(row)
print("*"*100)

#convert to SnowFlake Sequence
cmd_node = f"spock sequence-convert public.foo2_employeeid_seq {dbname}"
sequence=util_test.run_cmd("SnowFlake Conversion", cmd_node, f"{cluster_dir}/n1")
print(sequence.stdout)
print("*" * 100)

#INSERT data
row = util_test.write_psql("INSERT INTO foo2 (employeeName,employeeMail) VALUES('Mary','mary@pgedge.com'),('George','george@pgedge.com')",host,dbname,port,pw,usr)
print("*"*100)

#Check data
row = util_test.read_psql("SELECT * FROM foo2",host,dbname,port,pw,usr)
print(row)
print("*"*100)

# Check the sequence type of the column:
row = util_test.read_psql("SELECT table_name, column_name, column_default FROM information_schema.columns WHERE table_name = 'foo2';",host,dbname,port,pw,usr)
print(row)
print("*"*100)

if "Converting sequence public.foo2_employeeid_seq to snowflake sequence" in sequence.stdout:

    util_test.EXIT_PASS
else:
    util_test.EXIT_FAIL
   
