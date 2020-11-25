import datetime
import re
import pandas as pd

task_start_pattern = r"\[(.*)\] Started task: (.*) on worker: (.*)"
task_end_pattern = r"\[(.*)\] Completed task: (.*) on worker: (.*)"

job_start_pattern = r"\[(.*)\] Started job: (.*)"
job_end_pattern = r"\[(.*)\] Completed job: (.*)"

filenames = ["log_file_LL.txt", "log_file_RR.txt", "log_file_RANDOM.txt"]

for filename in filenames:

    jobs = dict()
    tasks = dict()

    with open(filename, "r") as f:
        line = f.readline()
        while line:
            # Process job start times
            job_start = re.match(job_start_pattern, line)
            if job_start:
                timestamp, job_id = job_start.groups()
                jobs[job_id] = timestamp

            # Process job end times and compute duration
            job_end = re.match(job_end_pattern, line)
            if job_end:
                end, job_id = job_end.groups()
                start = jobs[job_id]

                startTime = datetime.datetime.strptime(start, "%Y-%m-%d     %H:%M:%S.%f")

                endTime = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S.%f")

                duration = endTime - startTime

                elapsed = (duration.days * 86400) + (duration.seconds * 1) + float(duration.microseconds / 1000)/1000

                jobs[job_id] = float(elapsed)

            # Process task start times
            task_start = re.match(task_start_pattern, line)
            if task_start:
                timestamp, task_id, _ = task_start.groups()
                tasks[task_id] = timestamp

            # Process task end times and compute duration
            task_end = re.match(task_end_pattern, line)
            if task_end:
                end, task_id, _ = task_end.groups()
                start = tasks[task_id]

                startTime = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S.%f")
                endTime = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S.%f")

                duration = endTime - startTime

                elapsed = (duration.days * 86400) + (duration.seconds * 1) + float(duration.microseconds / 1000)/1000

                tasks[task_id] = float(elapsed)

            line = f.readline()

    jobs_df = pd.DataFrame(jobs.values(), columns=["jobs"])
    tasks_df = pd.DataFrame(tasks.values(), columns=["tasks"])

    print(jobs_df["jobs"].mean())
    print(tasks_df["tasks"].mean())

    print(jobs_df["jobs"].median())
    print(tasks_df["tasks"].median())
