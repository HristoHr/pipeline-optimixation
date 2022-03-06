import copy
import random

import numpy as np


class Task:
    def __init__(self, name, minutes, group=None):
        self.name = name
        self.minutes = minutes
        self.group = group
        self.dep = []
        self.inv_dep = []

    def set_dep(self, dep):
        self.dep = dep

    def set_inv_dep(self, inv_dep):
        self.inv_dep = inv_dep

    def importance_score(self, group):
        score = 0
        for t in self.inv_dep:
            if t in group:
                score += t.minutes
        return score

    def get_dep_time(self):
        if self.dep is None:
            return 0
        dep_time = 0
        for t in self.dep:
            dep_time += t.minutes
        return dep_time


class Core:
    def __init__(self, current_task=None):
        self.current_task = current_task

    def is_idle(self):
        return self.current_task is None or self.current_task.minutes == 0


class CPU:
    def __init__(self, cores):
        self.cores = cores

    def get_current_tasks(self):
        current_tasks = []
        for c in self.cores:
            if not c.is_idle():
                current_tasks.append(c.current_task)
        return current_tasks

    def get_idle_cores(self):
        idle_cores = []
        for c in self.cores:
            if c.is_idle():
                idle_cores.append(c)
        return idle_cores


def find_task_by_name(t_list, t_name):
    for t in t_list:
        if t.name == t_name:
            return t


from itertools import islice
import sys

num_cores = 2
read_file = 'pipeline_big.txt'
for arg in sys.argv:
    if "--cpu_cores=" in arg:
        num_cores = int(arg.split('=')[1])
    elif "--pipeline=" in arg:
        read_file = arg.split('=')[1]

execution_history_map = {}

task_list = []
task_dep_dict = {}
with open(read_file, 'r') as f:
    while True:
        next_n_lines = list(islice(f, 4))

        if next_n_lines[0] == 'END' or not next_n_lines:
            break

        name = next_n_lines[0].strip()
        minutes = int(next_n_lines[1].strip())
        group = next_n_lines[2].strip()

        dep_str = next_n_lines[3].strip()
        dep_names = None if next_n_lines[3].strip() == '' else dep_str.split(',')

        task = Task(name, minutes, group)
        task_list.append(task)

        task_dep_dict[task] = dep_names

for task, dep_names in task_dep_dict.items():
    if dep_names is None:
        continue
    dep_tasks = []
    for dep_name in dep_names:
        dep_tasks.append(find_task_by_name(task_list, dep_name))
    task.set_dep(dep_tasks)

inverted_task_dep_dict = {}
for task, dep_names in task_dep_dict.items():
    if dep_names is None:
        continue
    for task_name in dep_names:
        inverted_task_dep_dict.setdefault(find_task_by_name(task_list, task_name), []).append(task)

for task, inv_dep_tasks in inverted_task_dep_dict.items():
    task.set_inv_dep(inv_dep_tasks)

groups_dict = {'raw': [], 'feature': [], 'model': [], 'meta_models': [], 'no_group': []}
no_group = []
for task in task_list:
    if task.group == '':
        groups_dict['no_group'].append(task)
    else:
        groups_dict[task.group].append(task)


def optimal_task(grouped_tasks, processing_tasks):
    tasks = list(set(grouped_tasks).difference(set(processing_tasks)))
    random.shuffle(tasks)
    for t in tasks:
        if t.get_dep_time() == 0:
            return t


def set_task_dict(task_list):
    task_dict = {'raw': [], 'feature': [], 'model': [], 'meta_models': [], 'no_group': []}
    no_group = []
    for item in task_list:
        if item.group == '':
            task_dict['no_group'].append(item)
        else:
            task_dict[item.group].append(item)

    return task_dict


for i in range(1000):

    counter = 0
    counter_str = ''
    core_list = []

    for core in range(num_cores):
        core_list.append(Core())
    execution_history = []
    cpu = CPU(core_list)

    groups_dict = set_task_dict(copy.deepcopy(task_list))
    for group_name, group_tasks in groups_dict.items():
        while len(group_tasks) > 0:
            counter += 1
            counter_str = str(counter)
            for core in cpu.get_idle_cores():
                opt_task = optimal_task(group_tasks + groups_dict['no_group'], cpu.get_current_tasks())
                if opt_task is None:
                    opt_task = optimal_task(groups_dict['no_group'], cpu.get_current_tasks())
                if opt_task is None:
                    continue
                core.current_task = opt_task
            current_task_names = []
            for current_task in cpu.get_current_tasks():
                current_task.minutes = current_task.minutes - 1
                current_task_names.append(current_task.name)
                if current_task.minutes == 0:
                    if current_task in group_tasks:
                        group_tasks.remove(current_task)
                    elif current_task in groups_dict['no_group']:
                        groups_dict['no_group'].remove(current_task)

            group_name_str = group_name + '\n' if group_name != '| no_group' else '\n'
            execution_history.append(current_task_names)
    execution_history_map[counter] = execution_history

min_execution = np.min(list(execution_history_map.keys())) if len(execution_history_map.keys()) > 0 else None

if min_execution is not None:
    tasks_order = []
    for task_names in execution_history_map[min_execution]:
        executed_tasks = []
        for task_name in task_names:
            executed_tasks.append(find_task_by_name(task_list, task_name))
        tasks_order.append(executed_tasks)
    output_file_name = "output.txt"
    f = open(output_file_name, "a")
    f.write("| Time    | Tasks being Executed | Group Name\n")
    f.write("| ------- | -------------------- | ----------\n")

    counter = 0
    for tasks in tasks_order:
        tasks_names_str = ''
        tasks_group_str = ''

        for task in tasks:
            tasks_names_str += task.name + ','
            if task.group != 'no_group':
                tasks_group_str = task.group

        counter += 1
        f.write('|' + str(counter) + (9 - len(str(counter))) * ' ' + '|' + tasks_names_str[:-1] + (
                23 - len(tasks_names_str)) * ' ' + '| ' + tasks_group_str + '\n')

        for task in tasks:
            task.minutes -= 1
    # print("--- %s seconds ---" % (time.time() - start_time))
    print('Minimum Execution : ', min_execution, 'minutes')
    # print(len(historical_state_list))
