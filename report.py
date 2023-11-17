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
        if os.path.exists(f"{self.log_dir}/throughput"):
            with open(f"{self.log_dir}/throughput") as f:
                self.throughput = float(f.read())
        else:
            self.throughput = 0
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

    def evict_mode(self):
        return x.config["evict_mode"]

for x in os.listdir("log"):
    experiment.append(Experiment(f"log/{x}"))

def run(cmd):
    subprocess.run(cmd, shell=True, check=True)

run("rm -rf output")
run("mkdir output")

emm_points = {}

for x in experiment:
    if x.is_normal:
        em = x.evict_mode()
        if em not in emm_points:
            emm_points[em] = []
        emm_points[em].append((x.peak_memory, x.throughput))

plt.xlabel("Cache Memory (MB)")
plt.ylabel("Ops per sec")

for em in emm_points:
    x = emm_points[em]
    x.sort()
    plt.plot(*zip(*x), label=em)
plt.legend()
plt.savefig("output/pic.png")
plt.clf()

def file(log_dir, filename, suffix):
    count = counter.count()
    run(f"cp {log_dir}/{filename} output/{count}.{suffix}")
    with a(href=f"{count}.{suffix}"):
        p(filename)

def safe_div(l, r):
    return 0 if r == 0 else l / r

def plot_lines(lines, x_label, y_label):
    for (name, values) in lines:
        plt.plot(*zip(*values), label=name)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.legend()
    count = counter.count()
    plt.savefig(f"output/{count}.png")
    plt.clf()
    img(src=f"{count}.png")

def plot_bar_chart(breakdown):
    total_size = 0
    for (k, v) in breakdown.items():
        total_size += v
    sizes = []
    labels = []
    for (k, v) in breakdown.items():
        if v * 100 >= total_size:
            labels.append(k)
            sizes.append(v)
    plt.pie(sizes, labels=labels)
    count = counter.count()
    plt.savefig(f"output/{count}.png")
    plt.clf()
    img(src=f"{count}.png")

def single_eval(x):
    with doc(title=str(x)) as inner_doc:
        p(str(x))
        file(x.log_dir, "result.stdout", "txt")
        file(x.log_dir, "result.stderr", "txt")
        recomputation_total_time = 0
        eviction_total_time = 0
        wait_total_time = 0
        hit = 0
        miss = 0
        c_values = []
        memorys = []
        breakdown = {}
        if x.is_normal:
            files = []
            for y in os.listdir(x.log_dir):
                if y.endswith(".log"):
                    total_time = 0
                    c_value = []
                    memory = []
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
                                    c_value.append((j["current_time"], j["c_value"]))
                            elif j["command"] == "wait":
                                if j["current_time"] >= x.warmup_finished_time:
                                    wait_total_time += j["spent_time"]
                                    total_time += j["spent_time"]
                            elif j["command"] == "process":
                                if j["current_time"] >= x.warmup_finished_time:
                                    hit += j["hit"]
                                    miss += j["miss"]
                            elif j["command"] == "update_size":
                                memory.append((j["current_time"], j["size"]))
                            elif j["command"] == "log_individual_eviction":
                                if j["current_time"] >= x.warmup_finished_time:
                                    for (k, v) in j["breakdown"].items():
                                        if k not in breakdown:
                                            breakdown[k] = 0
                                        breakdown[k] += v
                            else:
                                print(x)
                                print(j)
                                raise
                    files.append((-total_time, y))
                    if len(c_value) > 0:
                        c_values.append((y, c_value))
                    if len(memory) > 0:
                        memorys.append((y, memory))
            plot_bar_chart(breakdown)
            plot_lines(c_values, "time", "c_value")
            plot_lines(memorys, "time", "memory")
            p(f"recomputation_total_time = {recomputation_total_time}")
            p(f"eviction_total_time = {eviction_total_time}")
            p(f"hit = {hit}")
            p(f"miss = {miss}")
            p(f"miss rate = {safe_div(miss, hit + miss)}")
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
    return f"{count}.html", recomputation_total_time, eviction_total_time#, wait_total_time

class Record:
    def __init__(self):
        self.recomputation_time = []
        self.eviction_time = []
        self.eviction_overhead = []

    def prepare(self):
        self.recomputation_time.sort()
        self.eviction_time.sort()
        self.eviction_overhead.sort()

    def record(self, peak_memory, recomputation_time, eviction_time):
        self.recomputation_time.append((peak_memory, recomputation_time))
        self.eviction_time.append((peak_memory, eviction_time))
        self.eviction_overhead.append((peak_memory, eviction_time/(recomputation_time + eviction_time)))

emm_records = {}

with doc(title='noria') as output:
    img(src="pic.png")
    for x in experiment:
        page_loc, recomputation_time, eviction_time = single_eval(x)
        if x.is_normal:
            em = x.evict_mode()
            if em not in emm_records:
                emm_records[em] = Record()
            emm_records[em].record(x.peak_memory, recomputation_time, eviction_time)
        with a(href=page_loc):
           p(str(x)) 
    for em in emm_records:
        emm_records[em].prepare()

    for em in emm_records:
        r = emm_records[em]
        plt.plot(*zip(*r.recomputation_time), label=f"{em}_recomputation")
    plt.xlabel("Cache Memory (MB)")
    plt.ylabel("Time spent(ms) across all cpu")
    plt.legend()
    plt.savefig("output/recomputation_time.png")
    plt.clf()
    img(src="recomputation_time.png")

    for em in emm_records:
        r = emm_records[em]
        plt.plot(*zip(*r.eviction_time), label=f"{em}_eviction")
    plt.xlabel("Cache Memory (MB)")
    plt.ylabel("Time spent(ms) across all cpu")
    plt.legend()
    plt.savefig("output/eviction_time.png")
    plt.clf()
    img(src="eviction_time.png")

    for em in emm_records:
        r = emm_records[em]
        plt.plot(*zip(*r.eviction_overhead), label=f"{em}_eviction_overhead")
    plt.xlabel("Cache Memory (MB)")
    plt.ylabel("fraction")
    plt.legend()
    plt.savefig("output/overhead.png")
    plt.clf()
    img(src="overhead.png")

with open("output/index.html", "w") as f:
    f.write(str(output))

run("rm -rf ../../output")
run("mv output ../../")

