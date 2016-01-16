package rel

// Task is the Interface for a runnable Task to operate on data
// - higher level interfaces compose this expression task
// - Runs/Stops is all we know
type Task interface {
	Run() error
	Close() error
}

/*
- convert above task to ?

type Task interface {
	Runnable() RunnableTask
	// info about the task
	MarshalJSON() ([]byte, error)
}
type RunnableTask interface {
	Run() error
	Close() error
}
*/
