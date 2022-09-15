# Client-Server Model
This Project deals with making a multithreaded client-server model.
- To get the executables, use the makefile as `make`
- To run the server, use `./server <number of threads in threadpool>`
- To run the client program, use `./client`
## Server
- The server is made with the assumption that the maximum threadpool size that is given input is 200
- The threads wait for a semaphore
- Each incoming request from the client is pushed into a queue, and post the semaphore
- When the thread gets the semaphore, it pops the queue, gets the client request string, and manipulates the dictionary to get the required change by the client.
- The dictionary has a maximum of 100 entries (keys till 100), and has a mutex lock for each key, so that no two threads read/write to the same key-value pair at a given instance.

## Client
- Client scans the `m` input given and stores it into the struct accordingly.
- Then, `m` client threads are spawned, which are then used to connect to the socket and do the operations mentioned by the user.
- After connection has been made succesful, the thread sends a request to the server and gets its response, and prints it to the console
- A mutex lock has been used to ensure that no two threads print to the console concurrently