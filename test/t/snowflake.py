# Note - this test only passes if the config.env file creates a two-node cluster.  If you get an error, check that first!

import sys, os, util_test,subprocess

## Print Script
print(f"Starting - {os.path.basename(__file__)}")

## Get Test Settings
util_test.set_env()

num_nodes=int(os.getenv("EDGE_NODES",2))
port=int(os.getenv("EDGE_START_PORT",6432))
usr=os.getenv("EDGE_USERNAME","lcusr")
pw=os.getenv("EDGE_PASSWORD","password")
db=os.getenv("EDGE_DB","demo")
host=os.getenv("EDGE_HOST","localhost")
dbname=os.getenv("EDGE_DB","lcdb")
cluster_dir = os.getenv("EDGE_CLUSTER_DIR")


for n in range(1,num_nodes):    
    #CREATE table 
    row = util_test.write_psql("CREATE TABLE IF NOT EXISTS acctg (employeeid  bigserial PRIMARY KEY,employeename VARCHAR(40),employeemail VARCHAR(40))",host,dbname,port,pw,usr)
    #INSERT data
    row = util_test.write_psql("INSERT INTO acctg (employeeid,employeename,employeemail) VALUES(1,'Carol','carol@pgedge.com'),(2,'Bob','bob@pgedge.com')",host,dbname,port,pw,usr)
    print("*"*100)
    
    # Check the sequence types:
    sequences = util_test.read_psql("SELECT * FROM pg_sequences;",host,dbname,port,pw,usr)
    print(sequences)
    print("*"*100)

    #Convert to SnowFlake Sequence
    #which inturn does:
    # - converts existing sequences to snowflake
    # - all the nodes will be propagated with new snowflake sequence change
    # - the column will have the DEFAULT value set to snowflake.sequence_name 
    cmd_node = f"spock sequence-convert public.acctg_employeeid_seq {dbname}"
    res=util_test.run_cmd("SnowFlake Conversion", cmd_node, f"{cluster_dir}/n{n}")
    print(res)
    print("*" * 100)
    
    #INSERT data
    row = util_test.write_psql("INSERT INTO acctg (employeename,employeemail) VALUES('Mary','mary@pgedge.com'),('George','george@pgedge.com')",host,dbname,port,pw,usr)
    print("*"*100)
    print(port)

    #Check the format of the employeeid column:
    row3 = util_test.read_psql("select employeeid,snowflake.format(employeeid) from acctg",host,dbname,port,pw,usr)
    print(row3)
    print("*"*100)

    port = port + 1

# Confirm that the sequence type of the column has been converted to a snowflake.nextval() sequence:

if "Converting sequence public.acctg_employeeid_seq to snowflake sequence" in row3:

    util_test.EXIT_PASS
else:
    util_test.EXIT_FAIL


