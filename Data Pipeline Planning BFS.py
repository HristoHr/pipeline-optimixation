from itertools import islice, combinations
import sys


class Path:
    def __init__(self, tasks):
        self.tasks = tasks
        tasks_minutes_sum = 0
        for task in tasks:
            tasks_minutes_sum += task.minutes
        self.minutes = tasks_minutes_sum
        self.str = self.to_string()

    def to_string(self):
        path_string = ''
        for task in self.tasks:
            path_string += task.name + ','
        return path_string[:-1]


class Task:
    def __init__(self, name, minutes, group=None):
        self.name = name
        self.minutes = minutes
        self.group = group
        self.parents = []
        self.children = []

    def set_parents(self, parents):
        self.parents = parents

    def set_children(self, children):
        self.children = children

    def importance_score(self, group):
        score = 0
        for t in self.children:
            if t in group:
                score += t.minutes
        return score

    def get_dep_time(self):
        if self.parents is None:
            return 0
        dep_time = 0
        for t in self.parents:
            dep_time += t.minutes
        return dep_time


class Core:
    def __init__(self, current_path=None, is_idle=True):
        self.current_path = current_path
        self.is_idle = is_idle

    def set_curren_path(self, path):
        self.current_path = path
        self.is_idle = False

    def set_is_busy(self):
        self.current_path = None
        self.is_idle = True


class CPU:
    def __init__(self, cores):
        self.cores = cores

    # def get_current_tasks(self):
    #     current_tasks = []
    #     for c in self.cores:
    #         if not c.is_idle():
    #             current_tasks.append(c.current_task)
    #     return current_tasks
    def get_loaded_paths(self):
        path_list = []
        for core in self.cores:
            if not core.is_idle:
                path_list.append(core.current_path)
        return path_list

    def get_idle_core(self):
        for core in self.cores:
            if core.is_idle:
                return core

    def get_idle_cores(self):
        idle_cores = []
        for core in self.cores:
            if core.is_idle:
                idle_cores.append(core)
        return idle_cores

    def find_core_by_path(self, current_ptah):
        for core in self.cores:
            if core.current_path == current_ptah:
                return core
    # def load_idle_cores(self, tasks):
    #     for task in tasks:
    #         if (self.get_idle_core() is not None) and (task not in self.get_current_tasks()):
    #             self.get_idle_core().current_task = task
    #
    # def get_idle_cores(self):
    #     idle_cores = []
    #     for c in self.cores:
    #         if c.is_idle():
    #             idle_cores.append(c)
    #     return idle_cores


def find_task_by_name(t_list, t_name):
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

result_list = []
# for i in range(1):
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
        dep_task = find_task_by_name(task_list, dep_name)
        if dep_task.group == task.group:
            dep_tasks.append(find_task_by_name(task_list, dep_name))
    task.set_parents(dep_tasks)

inverted_task_dep_dict = {}
for task, dep_names in task_dep_dict.items():
    if dep_names is None:
        continue
    for task_name in dep_names:
        dep_task = find_task_by_name(task_list, task_name)
        if dep_task.group == task.group:
            inverted_task_dep_dict.setdefault(dep_task, []).append(task)

for task, inv_dep_tasks in inverted_task_dep_dict.items():
    task.set_children(inv_dep_tasks)

groups_dict = {'raw': [], 'feature': [], 'model': [], 'meta_models': [], 'no_group': []}
no_group = []
for task in task_list:
    if task.group == '':
        groups_dict['no_group'].append(task)
    else:
        groups_dict[task.group].append(task)

graph = {}
for task in task_list:
    child_idx = []
    for child in task.children:
        if child.group == task.group:
            child_idx.append(child)
    graph[task] = child_idx

visitedList = [[]]

print(graph)


def bfs(visited, graph, node):
    visited.append(node)
    queue.append(node)

    while queue:
        s = queue.pop(0)
        print(s.name , end =' ')
        for neighbour in graph[s]:
            if neighbour not in visited:
                visited.append(neighbour)
                queue.append(neighbour)
    print()
    return visited

    # Driver Code


clock = 0
core_list = []
for i in range(num_cores):
    core_list.append(Core())
cpu = CPU(core_list)
for group_name, group_tasks in groups_dict.items():
    print()
    print(group_name)
    print()
    path_list = []
    for task in group_tasks:
        if task.get_dep_time() == 0:
            visited = []  # List to keep track of visited nodes.
            queue = []  # Initialize a queue
            path = Path(bfs(visited, graph, task))
            path_list.append(path)
    # print(path_list)
    if num_cores >= len(path_list):
        # print(path_list)
        max_time = 0
        for path in path_list:
            max_time = max_time if max_time > path.minutes else path.minutes
        clock += max_time
    else:
        path_list.sort(key=lambda x: x.minutes, reverse=True)
        for path in path_list:
            if cpu.get_idle_core() is not None:
                cpu.get_idle_core().set_curren_path(path)
            loaded_paths = cpu.get_loaded_paths()
            loaded_paths.sort(key=lambda x: x.minutes, reverse=False)
            clock += loaded_paths[0].minutes
            for loaded_path in loaded_paths:
                loaded_path.minutes -= loaded_paths[0].minutes
            cpu.find_core_by_path(loaded_paths[0]).set_is_busy()

print(clock)
# print()
# visited = []  # List to keep track of visited nodes.
# queue = []  # Initialize a queue
# bfs(visited, graph, 'B')
# print()
# visited = []  # List to keep track of visited nodes.
# queue = []  # Initialize a queue
# bfs(visited, graph, 'C')
# print()
