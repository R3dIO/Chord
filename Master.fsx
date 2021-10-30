#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic

//-------------------------------------- Initialization --------------------------------------//
type RingMasterMessage = 
    | NotifyMaster of int
    | InitializeRing of int
    | FindSuccessor of int
    | StabilizeRing
    | ConvergeRing

type RingWorkerMessage =
    | JoinRing of int
    | SetNodeId of int
    | InitializeKeys of int
    | MarkPredecessor of int
    | InitializeFingerTable
    | StabilizeNode
    | GetPredecessor

let numNodes = fsi.CommandLineArgs.[1] |> int
let numRequests = fsi.CommandLineArgs.[2] |> int
let stopWatch = Diagnostics.Stopwatch()
let system = ActorSystem.Create("System")
let mutable globalNodesDict = new Dictionary<int,IActorRef>()
let nodeList = ResizeArray([0])
let debug = true

if numNodes <= 0 || numRequests <= 0 then
    printfn "Invalid input"
    Environment.Exit(0)
//-------------------------------------- Initialization --------------------------------------//

//-------------------------------------- Utils --------------------------------------//
let rec divideLoop nodeSize =
    let mutable tableSize = nodeSize
    let mutable count = 0
    while (tableSize > 0) do
        tableSize <- tableSize/2
        count <- count + 1
    count    

let fingerTableSize = divideLoop numNodes
printf "fingertablesize %i" fingerTableSize
//-------------------------------------- Utils --------------------------------------//

//-------------------------------------- Worker Actor --------------------------------------//

let findSuccessor (nodeId:int, nodeList:Dictionary<int,bool>) =
    let mutable flag = true
    let mutable succesor = 0
    if nodeId < numNodes then
        for id in nodeId+1 .. numNodes do 
            if nodeList.ContainsKey id && flag then
                // if debug then printfn "found id %i" id
                succesor <- id
                flag <- false
    succesor 
//-------------------------------------- Utils --------------------------------------//

//-------------------------------------- Master Actor --------------------------------------//
let RingMaster(mailbox: Actor<_>) =
    
    let mutable requestCount = 0
    let mutable totalNumNodes = 0
    let mutable localNodeDict = new Dictionary<int,bool>()

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        let response = mailbox.Sender();
        try
            match msg with 
                | InitializeRing n ->
                    printfn "Starting execution" 
                    totalNumNodes <- n
                | NotifyMaster nodeId ->
                    if debug then printfn "INFO: Node %i Requested to Join" nodeId
                    localNodeDict.Add(nodeId, true)
                | FindSuccessor nodeId ->
                    let successorId = findSuccessor(nodeId, localNodeDict)
                    if debug then printfn "INFO: Found succesor %i for %i" successorId nodeId
                    response <! successorId
                | StabilizeRing ->
                    if debug then printfn "INFO: Stabilizing the Ring"
                    for KeyValue(key, worker) in globalNodesDict do
                        worker <! StabilizeNode
                    // Async.Sleep 2000 |> Async.RunSynchronously
                    // mailbox.Self <! StabilizeRing
                | ConvergeRing ->
                    requestCount <- requestCount + 1
                    if requestCount = numRequests then
                        stopWatch.Stop()
                        printfn "Time for convergence: %f ms" stopWatch.Elapsed.TotalMilliseconds
                        printfn "------------- End Transfer -------------"
                        Environment.Exit(0)
                | _ -> ()
        with
            | :? System.IndexOutOfRangeException -> printfn "ERROR: Tried to access outside array!" |> ignore
        return! loop()
    }            
    loop()

let master = spawn system "Master" RingMaster
//-------------------------------------- Master Actor --------------------------------------//

//-------------------------------------- Worker Actor --------------------------------------//
let RingWorker (mailbox: Actor<_>) =
    let mutable nodeId = -1;
    let mutable succesor = numNodes;
    let mutable predecessor = -1;
    let mutable fingerTable = new Dictionary<int,IActorRef>()

    let rec loop()= actor{
        let! message = mailbox.Receive();
        let response = mailbox.Sender();
        match message with
            | SetNodeId Id ->
                nodeId <- Id
            | JoinRing succesorId ->
                try 
                    succesor <- succesorId
                    globalNodesDict.[succesorId] <! MarkPredecessor nodeId
                    master <! NotifyMaster nodeId
                with 
                    | :?  System.Collections.Generic.KeyNotFoundException ->  printfn "ERROR: Key doesn't exist" |> ignore
            | GetPredecessor ->
                response <! predecessor
            | MarkPredecessor predecessorId ->
                if debug then printfn "INFO: Marking %i as predecessor for %i" predecessorId nodeId
                predecessor <- predecessorId
            | InitializeFingerTable ->
                if debug then printfn "INFO: Initializing Finger Table for %i" nodeId
                let requestList = []
                for i in [0..fingerTableSize] do
                    let nextEntry = nodeId + (pown 2 i)
                    if (nextEntry <= numNodes) then
                        let response =  (master <? FindSuccessor )
                        List.append requestList [response] |> ignore
                try
                    requestList 
                        |> Async.Parallel
                        |> Async.RunSynchronously
                        |> ignore
                with 
                    | :?  System.TimeoutException ->  printfn "ERROR: Unable to resolve all fingertable request" |> ignore
                for successorId in requestList do
                    fingerTable.Add(successorId, globalNodesDict.[successorId])
                for entry in fingerTable do printfn "INFO: FingerTable for node %i with Key %i and Value %A" nodeId entry.Key entry.Value
            | StabilizeNode ->
                try
                    let predecessorIdResp = (globalNodesDict.[succesor] <? GetPredecessor)
                    let nextNodePredecessor = Async.RunSynchronously (predecessorIdResp, 5000)
                    if nextNodePredecessor <> nodeId then
                        succesor <- nextNodePredecessor
                with 
                    | :?  System.Collections.Generic.KeyNotFoundException ->  printfn "ERROR: Key doesn't exist" |> ignore
                    | :?  System.TimeoutException ->  printfn "ERROR: Unable to StabilizeNode RTO" |> ignore
            | _ -> ()
        return! loop()
    }   
    loop()
//-------------------------------------- Worker Actor --------------------------------------//

//-------------------------------------- Main Program --------------------------------------//
stopWatch.Start()
master <! InitializeRing numNodes

if debug then printfn "Intializing the ring"
let key = "RingWorker0" 
let worker = spawn system (key) RingWorker
worker <! SetNodeId 0
globalNodesDict.Add(0, worker)
worker <! JoinRing numNodes

// Create nodes that will become part of ring
for x in [1..numNodes] do
    let key: string = "RingWorker" + string(x)
    let worker = spawn system (key) RingWorker
    worker <! SetNodeId x
    nodeList.Add(x)
    globalNodesDict.Add(x, worker)

// Select a random node and join it to ring
for x in [1..numNodes] do
    let rndNodeId = Random().Next(1,nodeList.Count)
    let worker = globalNodesDict.[nodeList.[rndNodeId]] 
    let response =  (master <? FindSuccessor nodeList.[rndNodeId])
    let successorId = Async.RunSynchronously (response, 10000)
    worker <! JoinRing successorId
    nodeList.RemoveAt(rndNodeId) |> ignore

master <! StabilizeRing

for KeyValue(key, worker) in globalNodesDict do
    worker <! InitializeFingerTable

Console.ReadLine() |> ignore
//-------------------------------------- Main Program --------------------------------------//

system.WhenTerminated.Wait()