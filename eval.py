import subprocess
from EVAL import *
from datetime import datetime
import time
import os
import shutil

shutil.rmtree("log", ignore_errors=True)

#CONFIG = {"memory": NONDET(60, 80, 100), "scale": 1, "use_zombie": NONDET(0, 1)}
CONFIG = {"memory": NONDET(30, 40, 50, 60, 70, 80, 90, 100, 110, 120), "scale": 1, "use_zombie": NONDET(0, 1)} # trying a bigger run

cleanup()
subprocess.run("cargo update", shell=True, check=True)
subprocess.run("cargo build --release --bin noria-server", shell=True, check=True)
subprocess.run("cargo build --release --bin lobsters-noria", shell=True, check=True)

for (i, x) in enumerate(flatten_nondet(CONFIG).l):
    log_dir_name = "log/" + str(datetime.now().strftime('%Y-%m-%d-%H-%M-%S')) + "-" + str(i)
    os.makedirs(log_dir_name)
    result = subprocess.run(f"python3 single_eval.py --config={repr(str(x))} --log_dir={repr(log_dir_name)}", shell=True, capture_output=True, text=True)
    with open(f"{log_dir_name}/result.stdout", 'w') as f:
        f.write(result.stdout)
        print(result.stdout)
    with open(f"{log_dir_name}/result.stderr", 'w') as f:
        f.write(result.stderr)
        print(result.stderr)
