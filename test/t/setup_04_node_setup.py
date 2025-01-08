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
clicommand = os.getenv('EDGE_CLI')
pgusn = os.getenv('EDGE_USERNAME')
pgpsw = os.getenv('EDGE_PASSWORD')
dbname = os.getenv('EDGE_DB')
startport = int(os.getenv('EDGE_START_PORT'))
pgversion = os.getenv('EDGE_INST_VERSION')
pgname = os.getenv('EDGE_COMPONENT')
spockver = os.getenv('EDGE_SPOCK_VER')

## Third Setup Script- Turns on Nodes for Testing

os.chdir(clusterdir)
for n in range(1,numnodes+1):
    os.chdir(os.path.join(f"n{n}", "pgedge"))

    # Deletes copydir
    cmd_node = f"./{clicommand} setup -U {pgusn} -P {pgpsw} -d {dbname} -p {startport + n - 1} --pg_ver {pgversion}"
    if spockver:
        cmd_node = f"{cmd_node} --spock_ver \"{spockver}\""

    res=subprocess.run(cmd_node, shell=True, capture_output=True, text=True)
    util_test.printres(res)
    if res.returncode == 1:
        util_test.exit_message(f"Faild {cmd_node}")
    if "already installed" in res.stdout:
        print("PG Already Running on Node")
        continue

    modules = {
        pgname: False,
        f"snowflake-{pgname}": False,
        f"spock": False
    }

    cmd_node = f"./{clicommand} um list"
    res=subprocess.run(cmd_node, shell=True, capture_output=True, text=True)
    util_test.printres(res)

    for line in res.stdout.strip().split("\\n"):
        for key in modules.keys():
            if key in line and "Installed" in line:
                modules[key] = True
                if key == "spock" and spockver:
                    if spockver in line:
                        print(f"Correct spock ver {spockver} is installed")
                    else:
                        util_test.exit_message(f"Faild, wrong spock ver {spockver} installed",1)
    
    
    for key in modules.keys():
        if modules[key]:
            print(f"Module {key} is installed")
        else:
            util_test.exit_message(f"Faild, module {key} not installed")


    os.chdir("../..")
    
os.chdir("../../../..")
util_test.exit_message(f"Pass - {os.path.basename(__file__)}", 0)
