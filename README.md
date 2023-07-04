## cpp map reduce implementation

implementation of multiprogramming using map reduce algrotihm.

supported functions are


```
JobHandle startMapReduceJob(const MapReduceClient& client,
	const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel);
```

```
void waitForJob(JobHandle job);
```

```
void getJobState(JobHandle job, JobState* state);
```

```
void closeJobHandle(JobHandle job);
```
