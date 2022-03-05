Solution 1: Data Pipeline Optimization.py

Prioritizes tasks that have the highest importance and the longest amount of time. (importance is the sum of minutes of dependent tasks) 
Yields a minimum time path for the short and big pipeline.
Writes the path into a file.

Solution 2: Data Pipeline Planning Monte Carlo.py

When selecting the next task to be processed choose a random task with a dependency time of 0. (Hence, doesn't depend on other tasks, or they have already been processed.)
Loops through the code 1000 times save the results into an array and prints the minimum amount of time.
Haven't finished it. Since it is not an optimal solution haven't coded the part where it writes the steps into a file. Only the Minimum time calculation.

Solution 3: Data Pipeline Planning Combinations.py

When a CPU core is idle find all possible combinations of idle CPU cores and tasks that are ready to execute. 
When more than one combination is available the first combination of tasks is loaded into the CPU the rest are into a list HistoryState objects.
The HistoryState preserves the current state of the execution with another option of tasks loaded into the CPU cores.
This way all possible combinations of all possible pipeline paths are simulated.
Takes too long, especially when too few CPU cores.
Unfinished because takes too long to execute,
longer than Monte Carlo.
Could be optimized.
Could be done with multiple threads.

Solution 4: Dara Pipeline Planning BFS.py

Uses Beath First Search to find paths of executions. Each execution path contains a list of tasks.
Works if there is an infinite amount of CPU cores.


For all solutions, I have assumed that groups of tasks have to be executed in order 
'raw','feature', 'model', 'meta_models'

