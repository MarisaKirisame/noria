import subprocess
from EVAL import *
from datetime import datetime
import time
import os
import shutil

shutil.rmtree("log")

CONFIG = {"memory": NONDET(40, 60, 80, 100, 120), "scale": 1, "use_zombie": NONDET(0, 1)}

for (i, x) in enumerate(flatten_nondet(CONFIG).l):
    log_dir_name = "log/" + str(datetime.now().strftime('%Y-%m-%d-%H-%M-%S')) + "-" + str(i)
    os.makedirs(log_dir_name)
    result = subprocess.run(f"python3 single_eval.py --config={repr(str(x))} --log_dir={repr(log_dir_name)}", shell=True, capture_output=True, text=True)
    with open(f"{log_dir_name}/result.stdout", 'w') as f:
        f.write(result.stdout)
    with open(f"{log_dir_name}/result.stderr", 'w') as f:
        f.write(result.stderr)
