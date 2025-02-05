import subprocess
import argparse

parser = argparse.ArgumentParser("single eval")
parser.add_argument("--config", required=True)
parser.add_argument("--log_dir", required=True)
args = parser.parse_args()

config = eval(args.config)

memory = config["memory"]
scale = config["scale"]
evict_mode = config["evict_mode"]

use_zombie = 0 if evict_mode == "baseline" else 1
use_kh = 1 if evict_mode == "kh" else 0

log_dir = args.log_dir

with open(f"{log_dir}/config", 'w') as f:
    f.write(str(config))

PROFILE = False

profile_header = "heaptrack " if PROFILE else ""
def cleanup():
    subprocess.run("../bin/zkCli.sh deleteall /x", shell=True)
    subprocess.run("pkill -f noria-server", shell=True)
    subprocess.run("pkill -f heaptrack", shell=True)

# does not call cleanup - have to call it yourself.
def run(mb):
    subprocess.Popen(f"RUST_BACKTRACE=1 USE_ZOMBIE={use_zombie} USE_KH={use_kh} ZOMBIE_LOG_DIR={log_dir} {profile_header}./target/release/noria-server --deployment x --memory {mb * 1024 * 1024} --durability memory", shell=True)
    #subprocess.Popen(f"./target/release/noria-server --deployment x --durability memory", shell=True)
    result = subprocess.run(f"timeout 5m ./target/release/lobsters-noria --deployment x --scale {scale} --prime --warmup 60 --runtime 60", shell=True, capture_output=True, text=True)
    print("printing result...")
    print(result.stdout)
    print("printing result err...")
    print(result.stderr)
    print("for loop...")
    for x in result.stdout.splitlines():
        result_pattern = "# generated ops/s:"
        if x.startswith(result_pattern):
            with open(f"{log_dir}/throughput", 'w') as f:
                f.write(str(float(x[len(result_pattern):])))

cleanup()
run(memory)
cleanup()
