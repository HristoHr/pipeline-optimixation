import copy
import threading
import time

import numpy as np
from itertools import combinations
import sys
import random
from itertools import islice


class Task:
    def __init__(self, name, minutes, group=None, dep=[]):
        self.name = name
        self.minutes = minutes
        self.group = group
        self.dep = []
        # self.inv_dep = []

    def set_dep(self, dep):
        self.dep = dep

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

    def get_idle_core(self):
        for c in self.cores:
            if c.is_idle():
                return c

    def load_idle_cores(self, tasks):
        for task in tasks:
            if (self.get_idle_core() is not None) and (task not in self.get_current_tasks()):
                self.get_idle_core().current_task = task

    def get_idle_cores(self):
        idle_cores = []
        for c in self.cores:
            if c.is_idle():
                idle_cores.append(c)
        return idle_cores

    def get_time_to_idle_core(self):
        current_tasks = self.get_current_tasks()
        current_tasks.sort(key=lambda x: x.minutes, reverse=False)
        return current_tasks[0].minutes


def find_task_by_name(t_list, t_name):
    for t in t_list:
        if t.name == t_name:
            return t


num_cores = 3
read_file = 'pipeline_big.txt'
for arg in sys.argv:
    if "--cpu_cores=" in arg:
        num_cores = int(arg.split('=')[1])
    elif "--pipeline=" in arg:
        read_file = arg.split('=')[1]

execution_history_map = {}
def set_tasks():
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
    return task_list


def set_task_dict(task_list):
    task_dict = {'raw': [], 'feature': [], 'model': [], 'meta_models': [], 'no_group': []}
    no_group = []
    for item in task_list:
        if item.group == '':
            task_dict['no_group'].append(item)
        else:
            task_dict[item.group].append(item)

    return task_dict


def flatten_dict(dict):
    flat_list = []
    for key, value_list in dict.items():
        for value in value_list:
            flat_list.append(value)
    return flat_list


def get_task_names(tasks):
    task_names = []
    for task in tasks:
        task_names.append(task.name)
    return task_names


clock_str = ''
# core_list = []
cpu_initial_list = []
# for core in range(num_cores):
#     core_list.append(Core())

# Set CPU initial state
task_list = set_tasks()

init_task_options = []
groups_dict = set_task_dict(task_list)
for init_task_option in (groups_dict['raw'] + groups_dict['no_group']):
    if init_task_option.get_dep_time() > 0:
        continue
    init_task_options.append(init_task_option.name)


class HistoricalState:
    def __init__(self, clock, cpu, groups_dict, core_task_combo, history):
        cpu_task_names = get_task_names(cpu)
        combo_task_names = get_task_names(core_task_combo)
        task_list_flat = flatten_dict(groups_dict)

        core_list = []
        for i in range(num_cores):
            core_list.append(Core())
        self.cpu = CPU(core_list)

        for cpu_task_name in cpu_task_names:
            task_found = find_task_by_name(task_list_flat, cpu_task_name)
            if task_found not in self.cpu.get_current_tasks():
                if self.cpu.get_idle_core() is not None:
                    self.cpu.get_idle_core().current_task = task_found

        for combo_task_name in combo_task_names:
            task_found = find_task_by_name(task_list_flat, combo_task_name)
            if task_found not in self.cpu.get_current_tasks():
                if self.cpu.get_idle_core() is not None:
                    self.cpu.get_idle_core().current_task = task_found

        self.history = history
        self.groups_dict = groups_dict
        self.clock = clock

    # def get_execution_time(self):
    #     return len(self.history) + 1


def get_options(tasks, cpu_current_tasks):
    unprocessed_tasks = list(set(tasks).difference(set(cpu_current_tasks)))
    options = []
    for ut in unprocessed_tasks:
        if ut.get_dep_time() == 0:
            options.append(ut)
    return options


def get_core_task_combos(options, cpu):
    if len(set(options)) != len(options):
        return list(combinations(get_combo_options(options), len(cpu.get_idle_cores())))
    else:
        return list(combinations(options, len(cpu.get_idle_cores())))


def get_combo_options(options):
    minutes_dict = {}
    for option in options:
        minutes_dict[option.minutes] = option
    return list(minutes_dict.values())


def pipeline_execution(clock, groups_dict, cpu, historical_state_list, execution_history, min_result=None):
    for group_name, group_tasks in groups_dict.items():
        while len(group_tasks) > 0:
            if len(cpu.get_idle_cores()) > 0:
                options = get_options(group_tasks + groups_dict['no_group'], cpu.get_current_tasks())
                if len(options) >= len(cpu.get_idle_cores()):

                    core_task_combos = get_core_task_combos(options, cpu)
                    if len(core_task_combos) > 0:
                        for i in range(1, len(core_task_combos)):
                            hist_state = HistoricalState(clock,
                                                         cpu.get_current_tasks(),
                                                         copy.deepcopy(groups_dict),
                                                         core_task_combos[i],
                                                         copy.deepcopy(execution_history))
                            historical_state_list.append(hist_state)
                        cpu.load_idle_cores(list(core_task_combos[0]))
                else:
                    cpu.load_idle_cores(options)

            minutes_to_idle_core = cpu.get_time_to_idle_core()
            clock += minutes_to_idle_core

            if min_result is not None and clock >= min_result:
                return

            current_task_names = []
            for current_task in cpu.get_current_tasks():
                current_task_names.append(current_task.name)
                current_task.minutes -= minutes_to_idle_core
                if current_task.minutes == 0:
                    if current_task in group_tasks:
                        group_tasks.remove(current_task)
                    elif current_task in groups_dict['no_group']:
                        groups_dict['no_group'].remove(current_task)
            execution_history.append(current_task_names)
    execution_history_map[clock] = execution_history


historical_state_list = []
combo_size = len(init_task_options) if num_cores >= len(init_task_options)-1 else num_cores
for task_comb in combinations(init_task_options, combo_size):
    task_list_copy = copy.deepcopy(task_list)
    core_list = []

    for init_task_name in task_comb:
        core_list.append(Core(find_task_by_name(task_list_copy, init_task_name)))

    groups_dict = set_task_dict(task_list_copy)
    cpu = CPU(core_list)
    min_execution = np.min(list(execution_history_map.keys())) if len(execution_history_map.keys()) > 0 else None
    pipeline_execution(0, groups_dict, cpu, historical_state_list, [], min_execution)

for historical_state in historical_state_list:
    min_execution = np.min(list(execution_history_map.keys())) if len(execution_history_map.keys()) > 0 else None
    pipeline_execution(historical_state.clock, historical_state.groups_dict, historical_state.cpu, historical_state_list,
                       historical_state.history, min_execution)

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
        shortest_task_minutes = sorted(tasks, key=lambda x: x.minutes, reverse=False)[0].minutes
        tasks_names_str = ''
        tasks_group_str = ''

        for task in tasks:
            tasks_names_str += task.name + ','
            if task.group != 'no_group':
                tasks_group_str = task.group

        for i in range(shortest_task_minutes):
            counter += 1
            f.write('|' + str(counter) + (9 - len(str(counter))) * ' ' + '|' + tasks_names_str[:-1] + (
                        23 - len(tasks_names_str)) * ' ' + '| ' + tasks_group_str + '\n')

        for task in tasks:
            task.minutes -= shortest_task_minutes
    print('Minimum Execution : ', np.min(min_execution))
