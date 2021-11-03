# Chord: P2P System and Simulation
Project for COP5615 - Distributed Operating System Principles, Fall 2021

##### Group members:
1. Anuj Koli
2. Pratiksha Jain

##### Running the project:
From the directory where Master.fsx exists, run on terminal/command line - 

`dotnet fsi --langversion:preview Master.fsx numNodes numRequests`

where `numNodes` is the number of peers/actors to be created in the peer-to-peer system 
and `numRequests` is the number of requests each peer/actor has to make.

#### What is working
1. Creation of virtual ring in a peer-to-peer network.
2. Performing the join operation and updating successor and finger tables whenever a new node joins the network.
3. Stabilization of the network when a new peer joins the network which will check if a node does not have a predecessor when it will assign one and fix the finger table of that node.
4. Once a node joins the p2p network, it will start sending messages into network and will stop once it sends maximum of numRequests messages. This message is then forwarded to the node closest to the destination and it continues until finally we find the destination.
5. The hop count is counter which is incremented for every hop taken for a message to reach the destination.


#### Results
------------------------------------------
| NumNodes | NumRequests | Hop Count(avg) |
------------------------------------------
| 1024     |      16     |      5         |
------------------------------------------

#### What is the largest network you managed to deal with
Largest network we were able to run this network is 1024 with average hop count 5. 

