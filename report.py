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

    def __str__(self):
        return str((self.config, self.throughput, self.is_normal))

    def __repr__(self):
        return repr((self.config, self.throughput, self.is_normal))

for x in os.listdir("log"):
    experiment.append(Experiment(f"log/{x}"))

baseline_points = []
zombie_points = []

def run(cmd):
    subprocess.run(cmd, shell=True, check=True)

run("rm -rf output")
run("mkdir output")

for x in experiment:
    if x.config["use_zombie"] == 0:
        if x.is_normal:
            baseline_points.append((x.peak_memory, x.throughput))
    elif x.config["use_zombie"] == 1:
        if x.is_normal:
            zombie_points.append((x.peak_memory, x.throughput))
    else:
        raise

baseline_points.sort()
zombie_points.sort()

plt.xlabel("Cache Memory (MB)")
plt.ylabel("Ops per sec")

plt.plot(*zip(*baseline_points), label="baseline")
plt.plot(*zip(*zombie_points), label="zombie")
plt.legend()
plt.savefig("output/pic.png")

with doc(title='noria') as output:
    img(src="pic.png")
    for x in experiment:
        count = counter.count()
        with doc(title=str(x)) as inner_doc:
            p(str(x))
            stdout_count = counter.count()
            stderr_count = counter.count()
            run(f"cp {x.log_dir}/result.stdout output/{stdout_count}.txt")
            run(f"cp {x.log_dir}/result.stderr output/{stderr_count}.txt")
            with a(href=f"{stdout_count}.txt"):
                p("stdout")
            with a(href=f"{stderr_count}.txt"):
                p("stderr")
        with open(f"output/{count}.html", "w") as f:
            f.write(str(inner_doc))
        with a(href=f"{count}.html"):
               p(str(x))

with open("output/index.html", "w") as f:
    f.write(str(output))

run("rm -rf ../../output")
run("mv output ../../")

