import subprocess

# todo: we are running without jemalloc. maybe turn it back on?

PROFILE = False

profile_header = "heaptrack " if PROFILE else ""
def cleanup():
    subprocess.run("../bin/zkCli.sh deleteall /x", shell=True)
    subprocess.run("pkill -f noria-server", shell=True)
    subprocess.run("pkill -f heaptrack", shell=True)

# does not call cleanup - have to call it yourself.
def run(mb):
    subprocess.Popen(f"RUST_BACKTRACE=1 USE_ZOMBIE=1 {profile_header}./target/release/noria-server --deployment x --memory {mb * 1024 * 1024} --durability memory", shell=True)
    #subprocess.Popen(f"./target/release/noria-server --deployment x --durability memory", shell=True)
    result = subprocess.run("./target/release/lobsters-noria --deployment x --scale 10 --prime --warmup 300 --runtime 300", shell=True, capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)

subprocess.run("cargo update", shell=True, check=True)
subprocess.run("cargo build --release --bin noria-server", shell=True, check=True)
subprocess.run("cargo build --release --bin lobsters-noria", shell=True, check=True)
    
cleanup()
run(1000)
cleanup()
#run(10000)
#cleanup()
#run(256)
#cleanup()
