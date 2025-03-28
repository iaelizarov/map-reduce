# map-reduce
Test assignment on map reduce by BenshiAI

To run the code you need to execute in terminal
```python driver.py --N 2 --M 4``` 

Then (or in any order) run 
```python worker.py```

Worker can send two types of "GET" requests: "GET map" and "GET reduce". If order is incorrect, or not all map tasks are finished before launching reduce task, or all map tasks finished but server send another "GET map", server will send a message, and Worker will keep be working.

You can launch multiple workers from multiple local terminals. Driver will handle tasks. Processes work in parallel, and if you add ```time.sleep(10)```, you can notice the time difference.

# The achitecture is following:
## Class MyServer
After initializing
- MyServer distributes map and reduce tasks between future workers
- MyServer creates multiprocessing.Manager() to manage resources between processes
- It accepts "GET map" and "GET reduce" requests from Worker

## Class MyRequestHandler
It handles all the requests from Worker
- GET parameters
- GET map
- GET reduce
- POST map
- POST reduce

## Class Worker
After initializing 
- Worker connects to server (if possible) or awaits for the connection
- Worker sends "GET parameters" request to the server to receive M and N for handling format of the output
- It can send "GET map" and "GET reduce" requests to receive list of files from the server, and it will process them and produce an output. After producing the output, it sends "POST" request to the driver, so the driver can flag the task as completed.
