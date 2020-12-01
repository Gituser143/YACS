#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import re
import datetime
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
plt.style.use('fivethirtyeight')


# In[2]:


log_path = "logs/SCHED_"
algos = ["RR", "RANDOM", "LL"]

print("Enter 0: RR.")
print("Enter 1: RANDOM.")
print("Enter 2: LL.")

algo_idx = int(input())

# In[3]:


log_file = log_path + algos[algo_idx]
log_files = [log_path + "RR",log_path + "RANDOM",log_path + "LL"]
master_logs = [log_files[i]+"/master.log" for i in range(0,3)]


# In[4]:


master_log = log_file + "/master.log"
conf_path = "config.json"
config_file = open(conf_path, "r")
json_data = json.load(config_file)
config_file.close()

worker_ids = []
workers = json_data["workers"]
for worker in workers:
    worker_id = worker["worker_id"]
    worker_ids.append(worker_id)

worker_logs = []
for worker_id in worker_ids:
    worker_logs.append(log_file + "/worker_" + str(worker_id) + ".log")


# In[5]:

# Parsing jobs
job_arrival_pattern = r"\[(.*)\] Job arrived: (.*)"
job_ending_pattern = r"\[(.*)\] Completed job: (.*)"

# Parsing tasks
task_arrival_pattern = r"\[(.*)\] Task arrived: (.*)"
task_ending_pattern = r"\[(.*)\] Completed task: (.*)"


# In[6]:


def duration(start, end):
    start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S.%f")
    end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S.%f")

    duration = end - start

    elapsed = float((duration.days * 86400) + (duration.seconds * 1) + float(duration.microseconds / 1000) / 1000)

    return elapsed


# In[7]:


f = open(master_log, "r")
lines = f.readlines()
f.close()


# In[8]:


def compute_stats(lines, arrival_pattern, ending_pattern):
    times = dict()
    for line in lines:
        start = re.match(arrival_pattern, line)
        if start:
            timestamp, Id = start.groups()
            times[Id] = timestamp
            continue

        end = re.search(ending_pattern, line)
        if end:
            end_timestamp, Id = end.groups()
            start_timestamp = times[Id]

            time_duration = duration(start_timestamp, end_timestamp)

            times[Id] = time_duration
    times = pd.DataFrame(times.values(), columns=["durations"])
    mean = times["durations"].mean()
    median = times["durations"].median()

    return mean, median


# In[9]:


job_mean, job_median = compute_stats(lines, job_arrival_pattern, job_ending_pattern)
print(job_mean, job_median)


# In[10]:


lines = []

for worker_log in worker_logs:
    with open(worker_log, "r") as f:
        lines += f.readlines()


# In[11]:


task_mean, task_median = compute_stats(lines, task_arrival_pattern, task_ending_pattern)
print(task_mean, task_median)


# In[12]:


f = open(master_log, "r")
base_line = f.readline()
f.close()

base_match = re.match(job_arrival_pattern, base_line)
if base_match:
    base, _ = base_match.groups()


# In[13]:


def get_plot_vals(lines, task_arrival_pattern, task_ending_pattern, base):
    stats = []
    for line in lines:
        start_match = re.match(task_arrival_pattern, line)
        if start_match:
            start_time, _ = start_match.groups()
            diff = duration(base, start_time)
            stats.append([diff, 1])
            continue

        end_match = re.match(task_ending_pattern, line)
        if end_match:
            end_time, _ = end_match.groups()
            diff = duration(base, end_time)
            stats.append([diff, -1])
            continue

    x = [i[0] for i in stats]
    y = [i[1] for i in stats]
    y = [sum(y[:i]) for i in range(1, len(y)+1)]
    return x, y


# In[14]:


def plot_scatter(lines, task_arrival_pattern, task_ending_pattern, base, worker_id):
    x, y = get_plot_vals(lines, task_arrival_pattern, task_ending_pattern, base)
    plt.figure(figsize=(15, 10))
    title = "Algorithm: " + algos[algo_idx] + ": Number of tasks running on worker " + worker_id + " against time."
    plt.figtext(.5, .9, title, fontsize=25, ha='center')
    plt.rc('xtick', labelsize=20)
    plt.rc('ytick', labelsize=20)
    plt.scatter(x, y, color=(0.5, 0.1, 0.5, 0.6))
    plt.xlabel("Time in seconds", fontsize=20)
    plt.ylabel("Number of tasks", fontsize=20)
    file_name = "scatter" + algos[algo_idx] + "worker" + worker_id + ".png"
    plt.savefig(file_name)


# In[15]:


def plot_line(lines, task_arrival_pattern, task_ending_pattern, base, worker_id):
    x, y = get_plot_vals(lines, task_arrival_pattern, task_ending_pattern, base)
    plt.figure(figsize=(15, 10))
    title = "Algorithm: " + algos[algo_idx] + ": Number of tasks running on worker " + worker_id + " against time."
    plt.figtext(.5, .9, title, fontsize=25, ha='center')
    plt.rc('xtick', labelsize=20)
    plt.rc('ytick', labelsize=20)
    plt.plot(x, y, 'b')
    plt.xlabel("Time in seconds", fontsize=20)
    plt.ylabel("Number of tasks", fontsize=20)
    file_name = "line" + algos[algo_idx] + "worker" + worker_id + ".png"
    plt.savefig(file_name)


# In[16]:


def plot_bar(lines, task_arrival_pattern, task_ending_pattern, base, worker_id):
    x, y = get_plot_vals(lines, task_arrival_pattern, task_ending_pattern, base)
    x_bar = [i for i in range(len(x))]
    y_bar = y

    plt.figure(figsize=(15, 10))
    title = "Algorithm: " + algos[algo_idx] + ": Number of tasks running on worker " + worker_id + " against event logs."
    plt.figtext(.5, .9, title, fontsize=25, ha='center')
    plt.rc('xtick', labelsize=20)
    plt.rc('ytick', labelsize=20)
    plt.bar(x_bar, y_bar)
    plt.xlabel("Series of events", fontsize=20)
    plt.ylabel("Number of tasks", fontsize=20)
    file_name = "bar" + algos[algo_idx] + "worker" + worker_id + ".png"
    plt.savefig(file_name)

# In[17]:

def plot_grouped_bar(labels,means,medians,flag) : 
    #flag = 0 for job, 1 for task
    names = ["Job","Task"]
    name = names[flag] 
    x = np.arange(len(labels))
    width = 0.35
    fig,ax = plt.subplots(figsize = (15,10))
    
    rects1 = ax.bar(x - width/2, means, width, label='mean')
    rects2 = ax.bar(x + width/2, medians, width, label='median')

    ax.set_ylabel('Mean/Median')
    ax.set_title(name + ' Mean/Medians by scheduling algorithms')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

    autolabel(rects1)
    autolabel(rects2)
    fig.tight_layout()
    figname = name + "_groupby_mean_median_bar.png"
    plt.savefig(figname)

def groupby_mean_median_plot() : 

    #job
    mul_lines = []
    for master_log in master_logs : 
        f = open(master_log, "r")
        lines = f.readlines()
        mul_lines.append(lines)
        f.close()

    job_vals = [compute_stats(s_lines,job_arrival_pattern,job_ending_pattern) for s_lines in mul_lines]

    mul_lines = []
    for log_file in log_files : 
        lines = []
        worker_logs = []
        for worker_id in worker_ids:
            worker_logs.append(log_file + "/worker_" + str(worker_id) + ".log")

        for worker_log in worker_logs:
            with open(worker_log, "r") as f:
                lines += f.readlines()
        mul_lines.append(lines)
    
    task_vals = [compute_stats(s_lines, task_arrival_pattern, task_ending_pattern) for s_lines in mul_lines]

    #for jobs
    labels = ['RR','RANDOM','LL']
    means = [round(job_vals[i][0],2) for i in range(0,3)]
    medians = [round(job_vals[i][1],2) for i in range(0,3)]
    plot_grouped_bar(labels,means,medians,0)

    #for tasks
    means = [round(task_vals[i][0],2) for i in range(0,3)]
    medians = [round(task_vals[i][1],2) for i in range(0,3)]
    plot_grouped_bar(labels,means,medians,1)


# In[18]:


for worker_log in worker_logs:
    with open(worker_log, "r") as f:
        lines = f.readlines()
    worker_id = worker_log.split("/")[-1]
    worker_id = worker_id.split("_")[-1]
    worker_id = worker_id.split(".")[0]
    plot_scatter(lines, task_arrival_pattern, task_ending_pattern, base, worker_id)


# In[19]:


for worker_log in worker_logs:
    with open(worker_log, "r") as f:
        lines = f.readlines()
    worker_id = worker_log.split("/")[-1]
    worker_id = worker_id.split("_")[-1]
    worker_id = worker_id.split(".")[0]
    plot_line(lines, task_arrival_pattern, task_ending_pattern, base, worker_id)


# In[20]:


for worker_log in worker_logs:
    with open(worker_log, "r") as f:
        lines = f.readlines()
    worker_id = worker_log.split("/")[-1]
    worker_id = worker_id.split("_")[-1]
    worker_id = worker_id.split(".")[0]
    plot_bar(lines, task_arrival_pattern, task_ending_pattern, base, worker_id)

# In[21]:

groupby_mean_median_plot()
