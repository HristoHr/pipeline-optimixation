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

    #
    # def set_inv_dep(self, inv_dep):
    #     self.inv_dep = inv_dep
    #
    # def importance_score(self, group):
    #     score = 0
    #     for t in self.inv_dep:
    #         if t in group:
    #             score += t.minutes
    #     return score

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


def find_task_by_name(t_list, t_name):
    for t in t_list:
        if t.name == t_name:
            return t


def optimal_task(grouped_tasks, processing_tasks):
    tasks = list(set(grouped_tasks).difference(set(processing_tasks)))
    # tasks.sort(key=lambda x: x.minutes, reverse=False)
    # tasks.sort(key=lambda x: x.minutes, reverse=True)
    # tasks.sort(key=lambda x: x.importance_score(grouped_tasks), reverse=True)
    options = []
    for t in tasks:
        if t.get_dep_time() == 0:
            options.append(t)
    # if len(options) > 1:
    #     for option in options:
    #         print(option.name,end=",")
    #     print()
    if len(options) >= 1:
        return random.choice(options)


num_cores = 3
read_file = 'pipeline_big.txt'
for arg in sys.argv:
    if "--cpu_cores=" in arg:
        num_cores = int(arg.split('=')[1])
    elif "--pipeline=" in arg:
        read_file = arg.split('=')[1]

result_list = []


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


def get_task_names(task_list):
    task_names = []
    for task in task_list:
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
    def __init__(self, cpu_task, groups_state, core_task_combo, history):
        cpu_task_names = get_task_names(cpu_task)
        combo_task_names = get_task_names(core_task_combo)
        task_list_flat = flatten_dict(groups_state)
        core_list = []
        for i in range(num_cores):
            core_list.append(Core())
        self.cpu_state = CPU(core_list)

        for cpu_task_name in cpu_task_names:
            task_found = find_task_by_name(task_list_flat, cpu_task_name)
            if task_found not in self.cpu_state.get_current_tasks():
                if self.cpu_state.get_idle_core() is not None:
                    self.cpu_state.get_idle_core().current_task = task_found

        for combo_task_name in combo_task_names:
            task_found = find_task_by_name(task_list_flat, combo_task_name)
            if task_found not in self.cpu_state.get_current_tasks():
                if self.cpu_state.get_idle_core() is not None:
                    self.cpu_state.get_idle_core().current_task = task_found

        self.history = history
        self.groups_state = groups_state

    def get_execution_time(self):
        return len(self.history) + 1


historical_state_list = []
execution_history_list = []
for task_comb in combinations(init_task_options, num_cores):

    task_list_copy = copy.deepcopy(task_list)
    groups_dict_copy = set_task_dict(task_list_copy)
    core_list = []
    execution_history = []
    for init_task_name in task_comb:
        core_list.append(Core(find_task_by_name(task_list_copy, init_task_name)))

    cpu = CPU(core_list)
    for state_group_name, state_group_tasks in groups_dict_copy.items():
        while len(state_group_tasks) > 0:
            if len(cpu.get_idle_cores()) > 0:
                unprocessed_tasks = list(
                    set(state_group_tasks + groups_dict_copy['no_group']).difference(set(cpu.get_current_tasks())))
                options = []
                for ut in unprocessed_tasks:
                    if ut.get_dep_time() == 0:
                        options.append(ut)
                if len(options) >= len(cpu.get_idle_cores()):
                    core_task_combos = list(combinations(options, len(cpu.get_idle_cores())))
                    if len(core_task_combos) > 0:
                        for i in range(1, len(core_task_combos)):
                            hist_state = HistoricalState(cpu.get_current_tasks(),
                                                         copy.deepcopy(groups_dict_copy),
                                                         core_task_combos[i],
                                                         copy.deepcopy(execution_history))
                            historical_state_list.append(hist_state)
                        cpu.load_idle_cores(list(core_task_combos[0]))
                else:
                    cpu.load_idle_cores(options)
            current_task_names = []
            for current_task in cpu.get_current_tasks():
                current_task_names.append(current_task.name)
                current_task.minutes = current_task.minutes - 1
                if current_task.minutes == 0:
                    if current_task in state_group_tasks:
                        state_group_tasks.remove(current_task)
                    elif current_task in groups_dict_copy['no_group']:
                        groups_dict_copy['no_group'].remove(current_task)
            current_task_names.sort()
            # print(execution_history)
            execution_history.append(current_task_names)

    result_list.append(len(execution_history))
    execution_history_list.append(execution_history)

for historical_state in historical_state_list:
    groups_state = historical_state.groups_state
    if len(historical_state.history) >= np.min(result_list):
        #print('too long 1')
        break
    for state_group_name, state_group_tasks in groups_state.items():
        while len(state_group_tasks) > 0:
            if len(historical_state.cpu_state.get_idle_cores()) > 0:
                unprocessed_tasks = list(
                    set(state_group_tasks + groups_state['no_group']).difference(
                        set(historical_state.cpu_state.get_current_tasks())))
                options = []
                for ut in unprocessed_tasks:
                    if ut.get_dep_time() == 0:
                        options.append(ut)

                if len(options) >= len(historical_state.cpu_state.get_idle_cores()):
                    core_task_combos = list(combinations(options, len(historical_state.cpu_state.get_idle_cores())))
                    if len(core_task_combos) > 0:
                        if historical_state.history not in historical_state_list:
                            for i in range(1, len(core_task_combos)):
                                hist_state = HistoricalState(historical_state.cpu_state.get_current_tasks(),
                                                             copy.deepcopy(groups_state),
                                                             core_task_combos[i],
                                                             copy.deepcopy(historical_state.history))
                                historical_state_list.append(hist_state)
                            historical_state.cpu_state.load_idle_cores(list(core_task_combos[0]))
                        else:
                            print("Duplicate")
                else:
                    historical_state.cpu_state.load_idle_cores(options)
            current_task_names = []
            for ct in historical_state.cpu_state.get_current_tasks():
                current_task_names.append(ct.name)
                ct.minutes = ct.minutes - 1
                if ct.minutes == 0:
                    if ct in state_group_tasks:
                        state_group_tasks.remove(ct)
                    elif ct in groups_state['no_group']:
                        groups_state['no_group'].remove(ct)
            current_task_names.sort()
            historical_state.history.append(current_task_names)

            if len(historical_state.history) >= np.min(result_list):
                #print('too long 2')
                break
        if len(historical_state.history) >= np.min(result_list):
            #print('too long 3')
            break
    if len(historical_state.history) < np.min(result_list):
        #print(len(historical_state.history))
        result_list.append(len(historical_state.history))
        execution_history_list.append(historical_state.history)
        # print(historical_state.history)
print('Minimum Execution : ', np.min(result_list))
