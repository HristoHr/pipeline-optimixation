import copy
import threading
import time

import numpy as np
from itertools import combinations
import sys
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


def find_task_by_name_in_task_list(t_list, t_name):
    for t in t_list:
        if t.name == t_name:
            return t


def find_task_by_name_in_groups(t_dict, t_name):
    for key, t_list in t_dict.items():
        for t in t_list:
            if t.name == t_name:
                return t


num_cores = 2
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
            dep_tasks.append(find_task_by_name_in_task_list(task_list, dep_name))
        task.set_dep(dep_tasks)
    return task_list


def set_task_dict(task_list):
    task_dict = {'raw': [], 'feature': [], 'model': [], 'meta_models': [], 'no_group': []}
    for item in task_list:
        if item.group == '':
            task_dict['no_group'].append(item)
        else:
            task_dict[item.group].append(item)

    return task_dict


def get_task_names(t_list):
    t_names = []
    for t in t_list:
        t_names.append(t.name)
    return t_names


# Set CPU initial state
task_list = set_tasks()

init_task_options = []
groups_dict = set_task_dict(task_list)
for init_task_option in (groups_dict['raw'] + groups_dict['no_group']):
    if init_task_option.get_dep_time() > 0:
        continue
    init_task_options.append(init_task_option.name)


# Historical State is a copy of pipeline execution at a certain point during the computation.
class HistoricalState:
    def __init__(self, clock, cpu, groups_dict, core_task_combo, history):
        # Get cpu and combo task names.
        # One can't just create a copy of the cpu state because it will have
        # different task objects than the one in the copy of the task groups.

        cpu_task_names = get_task_names(cpu)
        combo_task_names = get_task_names(core_task_combo)

        core_list = []
        for idx in range(num_cores):
            core_list.append(Core())
        self.cpu = CPU(core_list)

        # Load tasks that have been loaded in the CPU
        for cpu_task_name in cpu_task_names:
            task_found = find_task_by_name_in_groups(groups_dict, cpu_task_name)
            if task_found not in self.cpu.get_current_tasks():
                self.cpu.get_idle_core().current_task = task_found

        # Cores with the rest of the combination tasks
        for combo_task_name in combo_task_names:
            task_found = find_task_by_name_in_groups(groups_dict, combo_task_name)
            if task_found not in self.cpu.get_current_tasks():
                self.cpu.get_idle_core().current_task = task_found

        self.history = history
        self.groups_dict = groups_dict
        self.clock = clock


# Get tasks ready for execution.
def get_options(tasks, cpu_current_tasks):
    unprocessed_tasks = list(set(tasks).difference(set(cpu_current_tasks)))
    options = []
    for ut in unprocessed_tasks:
        if ut.get_dep_time() == 0:
            options.append(ut)
    return options


# Get combinations of tasks and empty cores.
def get_core_task_combos(options, cpu):
    return list(combinations(get_combo_options(options), len(cpu.get_idle_cores())))


# Get list of options discarding options of the same length
def get_combo_options(options):
    minutes_dict = {}
    for option in options:
        minutes_dict[option.minutes] = option
    return list(minutes_dict.values())


# Simulates pipeline execution
def pipeline_execution(clock, groups_dict, cpu, historical_state_list, execution_history, min_result=None):
    for group_name, group_tasks in groups_dict.items():
        while len(group_tasks) > 0:
            if len(cpu.get_idle_cores()) > 0:
                # Get list of tasks that are ready to execute
                options = get_options(group_tasks + groups_dict['no_group'], cpu.get_current_tasks())
                if len(options) >= len(cpu.get_idle_cores()):
                    # Get combinations of available tasks and idle cores
                    core_task_combos = get_core_task_combos(options, cpu)
                    if len(core_task_combos) > 0:
                        # Save pipeline state at this point and create add
                        # to a list of states with all possible cpu task combinations
                        # (except the first one).
                        for idx in range(1, len(core_task_combos)):
                            hist_state = HistoricalState(clock,
                                                         cpu.get_current_tasks(),
                                                         copy.deepcopy(groups_dict),
                                                         core_task_combos[idx],
                                                         copy.deepcopy(execution_history))
                            historical_state_list.append(hist_state)
                        # Continue pipeline computation with first combinations
                        cpu.load_idle_cores(list(core_task_combos[0]))
                else:
                    # If tasks ready to execute are same number as idle cpu cores or less add them to cpu.
                    cpu.load_idle_cores(options)

            # Instead of incrementing clock second by second
            # find when next core will be idle.
            # This way a number of iterations are saved.
            minutes_to_idle_core = cpu.get_time_to_idle_core()
            clock += minutes_to_idle_core

            # If current clock is bigger or equal to minimum clock so far stop computation.
            if min_result is not None and clock >= min_result:
                return

            current_task_names = []
            for current_task in cpu.get_current_tasks():
                current_task_names.append(current_task.name)
                # Update tasks
                current_task.minutes -= minutes_to_idle_core
                # Delete already computed tasks
                if current_task.minutes == 0:
                    if current_task in group_tasks:
                        group_tasks.remove(current_task)
                    elif current_task in groups_dict['no_group']:
                        groups_dict['no_group'].remove(current_task)
            execution_history.append(current_task_names)
    execution_history_map[clock] = execution_history


historical_state_list = []
start_time = time.time()
combo_size = len(init_task_options) if num_cores >= len(init_task_options) - 1 else num_cores
combinations_times_combo_map = {}

# Filter out combinations that are equivalent.
# Hence, combinations that have same task times.
for t_comb in combinations(init_task_options, combo_size):
    minutes_list = []
    for t_name in t_comb:
        t = find_task_by_name_in_task_list(task_list, t_name)
        minutes_list.append(t.minutes)
    minutes_list.sort()
    combinations_times_combo_map[str(minutes_list)] = t_comb

# Loop through all core task combinations
for task_combo in list(combinations_times_combo_map.values()):
    task_list_copy = copy.deepcopy(task_list)
    core_list = []

    for init_task_name in task_combo:
        core_list.append(Core(find_task_by_name_in_task_list(task_list_copy, init_task_name)))

    groups_dict = set_task_dict(task_list_copy)
    # Load CPU with combination of tasks.
    cpu = CPU(core_list)
    # Find min execution time so far.
    min_execution = np.min(list(execution_history_map.keys())) if len(execution_history_map.keys()) > 0 else None

    pipeline_execution(0, groups_dict, cpu, historical_state_list, [], min_execution)

# Loop through all historical states and compute theme
for historical_state in historical_state_list:
    min_execution = np.min(list(execution_history_map.keys())) if len(execution_history_map.keys()) > 0 else None
    pipeline_execution(historical_state.clock, historical_state.groups_dict, historical_state.cpu,
                       historical_state_list,
                       historical_state.history, min_execution)

# Print on a file
min_execution = np.min(list(execution_history_map.keys())) if len(execution_history_map.keys()) > 0 else None
if min_execution is not None:
    tasks_order = []
    for task_names in execution_history_map[min_execution]:
        executed_tasks = []
        for task_name in task_names:
            executed_tasks.append(find_task_by_name_in_task_list(task_list, task_name))
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
