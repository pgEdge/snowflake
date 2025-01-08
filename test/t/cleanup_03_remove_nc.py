import sys, os, util_test, subprocess
import json

## Print Script
print(f"Starting - {os.path.basename(__file__)}")

## Get Test Settings
util_test.set_env()

ncdir = os.getenv("NC_DIR")
home = os.getenv('HOME')

## Second Cleanup Script- Removes pgEdge

# Removes {nc}
cmd_node = f"sudo rm -rf {ncdir}"
res=subprocess.run(cmd_node, shell=True, capture_output=True, text=True)
util_test.printres(res)
if res.returncode == 1:
    util_test.exit_message(f"Faild {cmd_node}")

if os.path.exists(ncdir):
    util_test.exit_message(f"Couldn't delete {ncdir}")

# Removes pgpass file
cmd_node = f"sudo rm -rf {os.path.join(home, '.pgpass')}"
res=subprocess.run(cmd_node, shell=True, capture_output=True, text=True)
util_test.printres(res)
if res.returncode == 1:
    util_test.exit_message(f"Faild {cmd_node}")

if os.path.exists(ncdir):
    util_test.exit_message(f"Couldn't delete .pgpass from home")

util_test.exit_message(f"Pass - {os.path.basename(__file__)}", 0)
