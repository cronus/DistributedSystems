package mapreduce

import (
    "fmt"
    "sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

    workers := make(map[string]bool)
    var args *DoTaskArgs
    var wg sync.WaitGroup
    var work string
    taskNum := 0

    // schedule() must give each worker a sequence of tasks, one at a time.

    // schedule() learns about the set of workers by reading its registerChan argument. 
    // That channel yields a string for each worker, containing the worker's RPC address. 
    // Some workers may exist before schedule() is called, and some may start while schedule() is running; 
    // schedule() should use all the workers, including ones that appear after it starts.

    // schedule() tells a worker to execute a task by sending a Worker.DoTask RPC to the worker. 
    // This RPC's arguments are defined by DoTaskArgs in mapreduce/common_rpc.go. 
    // The File element is only used by Map tasks, and is the name of the file to read; 
    // schedule() can find these file names in mapFiles.
    // Use the call() function in mapreduce/common_rpc.go to send an RPC to a worker. 
    // The first argument is the the worker's address, as read from registerChan. 
    // The second argument should be "Worker.DoTask". 
    // The third argument should be the DoTaskArgs structure, and the last argument should be nil.

    newWk :=<- registerChan
    workers[newWk] = true
    for taskNum < ntasks {
        select {
        case newWk :=<- registerChan:
            workers[newWk] = true
        default:
            fmt.Println(taskNum)
            loop:
                for {
                    for wk, status := range workers {
                        if status { 
                            work = wk
                            break loop
                        }
                    }
                }

            workers[work] = false
            args = new(DoTaskArgs)
            args.JobName       = jobName
            args.File          = mapFiles[taskNum]
            args.Phase         = phase
            args.TaskNumber    = taskNum
            taskNum++
            args.NumOtherPhase = n_other

            go func() {
                fmt.Printf("start goroutine %v\n", taskNum)
                wg.Add(1)
                call (work, "Worker.DoTask", args, nil)
                workers[work] = true
                wg.Done()
            }()
        }
    }

    // schedule() should wait until all tasks have completed, and then return.
    wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
