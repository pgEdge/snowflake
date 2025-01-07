# This test case (and the other spock_# tests) expect to be run against a two node cluster.
# If it fails with an error: pg_reload_conf \n----------------\n t\n(1 row)\n\nSet GUC snowflake.node to 1\n[\n  {\n  ...
# you are probably running against a 3 node cluster.
# Per conversation with Cady, we may want to use a new setup script written in .py that uses the same
# logic as 8000a/8000b, but that uses the environment variable values. 

import os, util_test, subprocess

## Get Test Settings
util_test.set_env()

def run():
    # Get environment variables
    num_nodes = int(os.getenv("EDGE_NODES", 2))
    cluster_dir = os.getenv("EDGE_CLUSTER_DIR")
    port=int(os.getenv("EDGE_START_PORT",6432))
    repuser=os.getenv("EDGE_REPUSER","pgedge")
    pw=os.getenv("EDGE_PASSWORD","lcpasswd")
    db=os.getenv("EDGE_DB","lcdb")
    host=os.getenv("EDGE_HOST","localhost")

    for n in range(1,num_nodes+1):
        ## Create Nodes
        cmd_node = f"spock node-create n{n} 'host=127.0.0.1 port={port} user={repuser} dbname={db}' {db}"
        res=util_test.run_cmd("Node Create", cmd_node, f"{cluster_dir}/n{n}")
        print(res)
        if res.returncode == 1 or "node_create" not in res.stdout:
            util_test.exit_message(f"Fail - {os.path.basename(__file__)} - Node Create", 1) 
        port = port + 1

if __name__ == "__main__":
    ## Print Script
    print(f"Starting - {os.path.basename(__file__)}")
    run()
    util_test.exit_message(f"Pass - {os.path.basename(__file__)}", 0) 
