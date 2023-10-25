import os
from matplotlib import pyplot as plt
import subprocess
import dominate
from dominate.tags import *
import re

class Counter:
    def __init__(self):
        self.num = 0

    def count(self):
        ret = self.num
        self.num += 1
        return ret

counter = Counter()

class doc(dominate.document):
    def _add_to_ctx(self): pass # don't add to contexts

def normal_throughput(t):
    return t >= 100 and t <= 1e6

experiment = []

class Experiment:
    def __init__(self, log_dir):
        self.log_dir = log_dir
        with open(f"{self.log_dir}/config") as f:
            self.config = eval(f.read())
        with open(f"{self.log_dir}/throughput") as f:
            self.throughput = float(f.read())
        self.is_normal = normal_throughput(self.throughput)
        self.stdout_dir = f"{self.log_dir}/result.stdout"
        self.stderr_dir = f"{self.log_dir}/result.stderr"
        with open(self.stdout_dir) as f:
            self.peak_memory = 0
            for x in f.readlines():
                if x.startswith("handle_eviction:"):
                    memory = int(re.findall(r"handle_eviction: memory = (\d*), limit = .*", x)[0])
                    self.peak_memory = max(memory, self.peak_memory)
                if x.startswith("warmup finished at"):
                    self.warmup_finished_time = int(re.findall(r"warmup finished at (\d*)", x)[0])
        if not hasattr(self, "warmup_finished_time"):
            self.is_normal = False

    def __str__(self):
        return str((self.config, self.peak_memory, self.throughput, self.is_normal))

    def __repr__(self):
        return repr((self.config, self.peak_memory, self.throughput, self.is_normal))

    def use_zombie(self):
        if x.config["use_zombie"] == 0:
            return False
        elif x.config["use_zombie"] == 1:
            return True
        else:
            raise

for x in os.listdir("log"):
    experiment.append(Experiment(f"log/{x}"))

def run(cmd):
    subprocess.run(cmd, shell=True, check=True)

run("rm -rf output")
run("mkdir output")

baseline_points = []
zombie_points = []

for x in experiment:
    if x.is_normal:
        if x.use_zombie():
            zombie_points.append((x.peak_memory, x.throughput))
        else:
            baseline_points.append((x.peak_memory, x.throughput))

baseline_points.sort()
zombie_points.sort()

plt.xlabel("Cache Memory (MB)")
plt.ylabel("Ops per sec")

plt.plot(*zip(*baseline_points), label="baseline")
plt.plot(*zip(*zombie_points), label="zombie")
plt.legend()
plt.savefig("output/pic.png")
plt.clf()

def file(log_dir, filename, suffix):
    count = counter.count()
    run(f"cp {log_dir}/{filename} output/{count}.{suffix}")
    with a(href=f"{count}.{suffix}"):
        p(filename)

def single_eval(x):
    with doc(title=str(x)) as inner_doc:
        p(str(x))
        file(x.log_dir, "result.stdout", "txt")
        file(x.log_dir, "result.stderr", "txt")
        recomputation_total_time = 0
        eviction_total_time = 0
        wait_total_time = 0
        if x.is_normal:
            files = []
            for y in os.listdir(x.log_dir):
                total_time = 0
                if y.endswith(".log"):
                    with open(f"{x.log_dir}/{y}") as f:
                        for l in f.readlines():
                            j = eval(l)
                            if j["command"] == "recomputation":
                                if j["current_time"] >= x.warmup_finished_time:
                                    recomputation_total_time += j["spent_time"]
                                    total_time += j["spent_time"]
                            elif j["command"] == "eviction":
                                if j["current_time"] >= x.warmup_finished_time:
                                    eviction_total_time += j["spent_time"]
                                    total_time += j["spent_time"]
                            elif j["command"] == "wait":
                                if j["current_time"] >= x.warmup_finished_time:
                                    wait_total_time += j["spent_time"]
                                    total_time += j["spent_time"]
                            else:
                                print(j)
                                raise
                    files.append((-total_time, y))
            p(f"recomputation_total_time = {recomputation_total_time}")
            p(f"eviction_total_time = {eviction_total_time}")
            files.sort()
            for (_, y) in files:
                file(x.log_dir, y, "txt")
        else:
            for y in os.listdir(x.log_dir):
                if y.endswith(".log"):
                    file(x.log_dir, y, "txt")
    count = counter.count()
    with open(f"output/{count}.html", "w") as f:
        f.write(str(inner_doc))
    return f"{count}.html", wait_total_time, eviction_total_time#, wait_total_time

baseline_eviction_time = []
zombie_eviction_time = []

baseline_recomputation_time = []
zombie_recomputation_time = []

with doc(title='noria') as output:
    img(src="pic.png")
    for x in experiment:
        page_loc, recomputation_time, eviction_time = single_eval(x)
        if x.is_normal:
            if x.use_zombie(): 
                zombie_recomputation_time.append((x.peak_memory, recomputation_time))
                zombie_eviction_time.append((x.peak_memory, eviction_time))
            else:
                baseline_recomputation_time.append((x.peak_memory, recomputation_time))
                baseline_eviction_time.append((x.peak_memory, eviction_time))
        with a(href=page_loc):
           p(str(x))
    baseline_eviction_time.sort()
    zombie_eviction_time.sort()
    baseline_recomputation_time.sort()
    zombie_recomputation_time.sort()
    #plt.plot(*zip(*baseline_eviction_time), label="baseline_eviction")
    #plt.plot(*zip(*zombie_eviction_time), label="zombie_eviction")
    plt.plot(*zip(*baseline_recomputation_time), label="baseline_recomputation")
    plt.plot(*zip(*zombie_recomputation_time), label="zombie_recomputation")
    plt.xlabel("Cache Memory (MB)")
    plt.ylabel("Time spent(ms) across all cpu")
    plt.legend()
    plt.savefig("output/overhead.png")
    plt.clf()
    img(src="overhead.png")

with open("output/index.html", "w") as f:
    f.write(str(output))

run("rm -rf ../../output")
run("mv output ../../")

