import subprocess
import argparse

parser = argparse.ArgumentParser("single eval")
parser.add_argument("--config", required=True)
parser.add_argument("--log_dir", required=True)
args = parser.parse_args()

config = eval(args.config)

memory = config["memory"]
scale = config["scale"]
use_zombie = config["use_zombie"]

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
    subprocess.Popen(f"RUST_BACKTRACE=1 USE_ZOMBIE={use_zombie} {profile_header}./target/release/noria-server --deployment x --memory {mb * 1024 * 1024} --durability memory", shell=True)
    #subprocess.Popen(f"./target/release/noria-server --deployment x --durability memory", shell=True)
    result = subprocess.run(f"timeout 5m ./target/release/lobsters-noria --deployment x --scale {scale} --prime --warmup 60 --runtime 60", shell=True, capture_output=True, text=True)
    print("printing result...")
    print(result.stdout)
    print("printing result err...")
    print(result.stderr)
    print("for loop...")
    for x in result.stdout.splitlines():
        print(x)
        result_pattern = "# generated ops/s:"
        if x.startswith(result_pattern):
            with open(f"{log_dir}/throughput", 'w') as f:
                f.write(str(float(x[len(result_pattern):])))

cleanup()
subprocess.run("cargo update", shell=True, check=True)
subprocess.run("cargo build --release --bin noria-server", shell=True, check=True)
subprocess.run("cargo build --release --bin lobsters-noria", shell=True, check=True)

run(memory)
cleanup()
