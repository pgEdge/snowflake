import sys, os, util_test, subprocess
import json

## Print Script
print(f"Starting - {os.path.basename(__file__)}")

## Get Test Settings
util_test.set_env()

ncdir = os.getenv("NC_DIR")
homedir = os.getenv("EDGE_HOME_DIR")
edgerepo = os.getenv('EDGE_REPO')

## First Setup Script to Create Environment for All Future Tests
print(f"whoami = {os.getenv('EDGE_REPUSER')}")

# Check if the directory already exists
if os.path.exists(homedir):
    print(f"{homedir} already installed...")
    print("skipping the download and install and exiting with success")
    sys.exit(0)

# Creates `platform_test/{nc}`
cmd_node = f"mkdir -p {ncdir}"
res=subprocess.run(cmd_node, shell=True, capture_output=True, text=True)
util_test.printres(res)
if res.returncode == 1:
    util_test.exit_message("Couldn't make nc dir")

# Downloads pgedge-upstream into {nc}
cmd_node = f"curl -fsSL {edgerepo} > {ncdir}/install.py"
res=subprocess.run(cmd_node, shell=True, capture_output=True, text=True)
util_test.printres(res)
if res.returncode == 1:
    util_test.exit_message("Couldn't download pgedge upstream")

os.chdir(ncdir)

# Downloads pgedge-upstream into {nc}
cmd_node = f"python install.py"
res=subprocess.run(cmd_node, shell=True, capture_output=True, text=True)
util_test.printres(res)
if res.returncode == 1:
    util_test.exit_message("Couldn't run install.py")

os.chdir("..")

# Pull down pgedge info.
cmd_node = f"{ncdir}/pgedge/pgedge info"
res=subprocess.run(cmd_node, shell=True, capture_output=True, text=True)
util_test.printres(res)
if res.returncode == 1:
    util_test.exit_message("Install of pgedge failed")

util_test.exit_message(f"Pass - {os.path.basename(__file__)}", 0)

