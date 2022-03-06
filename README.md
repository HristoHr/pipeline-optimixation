Solution 1: Data Pipeline Prioritization.py

Prioritizes tasks that have the highest importance and the longest amount of time. (importance is the sum of minutes of dependent tasks) 
Yields a minimum time path for the short and big pipeline.
Most efficient solution of all.

Solution 2: Data Pipeline Planning Monte Carlo.py

When selecting the next task to be processed choose a random task with a dependency time of 0. (Hence, doesn't depend on other tasks, or they have already been processed.)
Loops through the code 1000 times save the results and prints the minimum amount of time.
Not the most efficient sollution since the bigger the pipeline and the more the CPUs the longer it is goin to take.
Can't be 100% the one is goint to get the most optimal solution.

Solution 3: Data Pipeline Planning Combinations.py

When a CPU core is idle find all possible combinations of idle CPU cores and tasks that are ready to execute. 
When more than one combination is available the first combination of tasks is loaded into the CPU the rest of the combinations are saved into a list HistoryState objects.
The HistoryState preserves the current state of the execution with another option of tasks loaded into the CPU cores.
This way all possible combinations of all possible pipeline paths are simulated.
Takes too long, especially when too few CPU cores.
(Tried to optimize it with multithreading. However since its Python it actually took longer to run.)

Solution 4: Data Pipeline Planning BFS.py (Unfinished)

Attempt to use Breadth First Search.  
Coundn't figure it out.
Works only on pipeline_small.txt





For all solutions, I have assumed that groups of tasks have to be executed in order 
'raw','feature', 'model', 'meta_models'

