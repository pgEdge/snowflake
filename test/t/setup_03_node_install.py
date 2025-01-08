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

copydir = "/tmp/nccopy"

## Second Setup Script- Creates Nodes for Testing

# Checks for nc dir    
if not os.path.exists(os.path.join(ncdir, "pgedge")):
    util_test.exit_message(f"Error: nc dir does not exist, run set_01 before this")

# Deletes copydir
cmd_node = f"rm -rf {copydir}"
res=subprocess.run(cmd_node, shell=True, capture_output=True, text=True)
util_test.printres(res)
if res.returncode == 1:
    util_test.exit_message(f"Faild {cmd_node}")

# Copies pgedge into copydir
cmd_node = f"cp -r -T {ncdir}/. {copydir}"
res=subprocess.run(cmd_node, shell=True, capture_output=True, text=True)
util_test.printres(res)
if res.returncode == 1:
    util_test.exit_message(f"Faild {cmd_node}")

for n in range(1,numnodes+1):
    nodedir = os.path.join(clusterdir, f"n{n}")

    # Check if the directory already exists
    if os.path.exists(nodedir):
        if os.path.exists(os.path.join(nodedir, "install.py")) and os.path.exists(os.path.join(nodedir, "pgedge")):
            print(f"Node {nodedir} already installed...")
            print("Skipping the download and install and continuing to next node")
            continue
        else:
            util_test.exit_message(f"Error: Previous Install Exists and Is Broken")

    # Creates nodedir
    cmd_node = f"mkdir -p {nodedir}"
    res=subprocess.run(cmd_node, shell=True, capture_output=True, text=True)
    util_test.printres(res)
    if res.returncode == 1:
        util_test.exit_message(f"Faild {cmd_node}")

    # Copies pgedge into nodedir
    cmd_node = f"cp -r -T {copydir}/. {nodedir}"
    res=subprocess.run(cmd_node, shell=True, capture_output=True, text=True)
    util_test.printres(res)
    if res.returncode == 1:
        util_test.exit_message(f"Faild {cmd_node}")

# Deletes copydir
cmd_node = f"rm -rf {copydir}"
res=subprocess.run(cmd_node, shell=True, capture_output=True, text=True)
util_test.printres(res)
if res.returncode == 1:
    util_test.exit_message(f"Faild {cmd_node}")

util_test.exit_message(f"Pass - {os.path.basename(__file__)}", 0)
