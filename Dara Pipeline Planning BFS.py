from itertools import islice, combinations
import sys

class Path:
    def __init__(self, tasks):
        self.tasks = tasks
        tasks_minutes_sum = 0
        for task in tasks:
            tasks_minutes_sum += task.minutes
        self.path_length = tasks_minutes_sum
        self.path_str = self.to_string()

    def to_string(self):
        path_string = ''
        for task in self.tasks:
            path_string += task.name + ','
        return path_string[:-1]

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

def find_task_by_name(t_list, t_name):
    for t in t_list:
        if t.name == t_name:
            return t


num_cores = 2
read_file = 'pipeline_test.txt'
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

        for neighbour in graph[s]:
            if neighbour not in visited:
                visited.append(neighbour)
                queue.append(neighbour)
    return visited
    # Driver Code


clock = 0
for group_name, group_tasks in groups_dict.items():
    print(group_name)
    path_list = []
    for task in group_tasks:
        if task.get_dep_time() == 0:
            visited = []  # List to keep track of visited nodes.
            queue = []  # Initialize a queue
            path = Path(bfs(visited, graph, task))
            path_list.append(path)
    # print(path_list)
    if num_cores >= len(path_list):
        max_time = 0
        for path in path_list:
            max_time = max_time if max_time > path.path_length else path.path_length
        clock += max_time
    else:
        combo_time_map = {}
        for path_combo in (combinations(path_list, num_cores)):
            core_max_time = 0
            path_combo_str = ''
            for path in path_combo:
                core_max_time = core_max_time if core_max_time > path.path_length else path.path_length
                path_combo_str += path.path_str + ' '
            print(path_combo_str, core_max_time)
            combo_time_map[core_max_time] = list(path_combo)

            # combo_min_time = combo_min_time < path_option.path_length if combo_min_time else path_option.path_length
            # print(path_option.path_str)
            # print(path_option.path_length)
        print(combo_time_map)

# print()
# visited = []  # List to keep track of visited nodes.
# queue = []  # Initialize a queue
# bfs(visited, graph, 'B')
# print()
# visited = []  # List to keep track of visited nodes.
# queue = []  # Initialize a queue
# bfs(visited, graph, 'C')
# print()
