import sys, os, util_test, subprocess
import json

## Print Script
print(f"Starting - {os.path.basename(__file__)}")

## Get Test Settings
util_test.set_env()

ncdir = os.getenv("NC_DIR")
homedir = os.getenv("EDGE_HOME_DIR")
clusterdir = os.getenv('EDGE_CLUSTER_DIR')
numnodes = int(os.getenv('EDGE_NODES'))
pgname = os.getenv('EDGE_COMPONENT')

copydir = "/tmp/nccopy"

## First Cleanup Script- Removes Nodes

for n in range(1,numnodes+1):
    nodedir = os.path.join(clusterdir, f"n{n}", "pgedge")

    cmd_node = f"remove {pgname} --rm-data"
    res=util_test.run_nc_cmd("Remove", cmd_node, nodedir)
    util_test.printres(res)
    if res.returncode != 0:
        util_test.exit_message(f"Couldn't remove node {n}")
    cmd_node = f"remove backrest"
    res=util_test.run_nc_cmd("Remove", cmd_node, nodedir)

    modules = {
        pgname: False,
        f"snowflake-{pgname}": False,
        f"spock": False
    }

    cmd_node = f"um list"
    res=util_test.run_nc_cmd("List", cmd_node, nodedir)
    util_test.printres(res)

    for line in res.stdout.strip().split("\\n"):
        for key in modules.keys():
            if key in line and "Installed" in line:
                modules[key] = True

    for key in modules.keys():
        if modules[key]:
            util_test.exit_message(f"Faild, module {key} still installed")
        else:
            print(f"Module {key} was removed")

util_test.exit_message(f"Pass - {os.path.basename(__file__)}", 0)
