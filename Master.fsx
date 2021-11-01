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
    | GetRingList

type RingWorkerMessage =
    | JoinRing of int
    | SetNodeId of int
    | InitializeKeys of int
    | MarkPredecessor of int
    | InitializeFingerTable
    | StabilizeNode
    | GetPredecessor of IActorRef
    | Notify of int
    | DistributeKeys of list<int>

let numNodes = fsi.CommandLineArgs.[1] |> int
let numRequestsPerNode = fsi.CommandLineArgs.[2] |> int
let stopWatch = Diagnostics.Stopwatch()
let system = ActorSystem.Create("System")
let mutable globalNodesDict = new Dictionary<int,IActorRef>()
let nodeList = ResizeArray([0])
let debug = true

if numNodes <= 0 || numRequestsPerNode <= 0 then
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

let findSuccessor (nodeId:int, nodeList:Dictionary<int,_>) =
    let mutable flag = true
    let mutable succesor = 0
    if nodeId < numNodes then
        for id in nodeId .. numNodes do 
            if nodeList.ContainsKey id && flag then
                // if debug then printfn "found id %i" id
                succesor <- id
                flag <- false
    succesor 

let RandomJoin(maxNodes:int, master:IActorRef) = 
    // Select a random node and join it to ring
    for x in [1..maxNodes] do
        let rndNodeId = Random().Next(1,nodeList.Count)
        let worker = globalNodesDict.[nodeList.[rndNodeId]] 
        let response =  (master <? FindSuccessor nodeList.[rndNodeId])
        let successorId = Async.RunSynchronously (response, 10000)
        worker <! JoinRing successorId
        nodeList.RemoveAt(rndNodeId) |> ignore

let fingerTableSize = divideLoop numNodes

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

                | GetRingList ->
                    response <! localNodeDict

                | StabilizeRing ->
                    if debug then printfn "INFO: Stabilizing the Ring"
                    for KeyValue(key, worker) in globalNodesDict do
                        worker <! StabilizeNode
                    let delay = async { do! Async.Sleep(2000) }
                    Async.RunSynchronously(delay)
                    mailbox.Self <! StabilizeRing

                | ConvergeRing ->
                    requestCount <- requestCount + 1
                    if requestCount = numRequestsPerNode then
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
    let mutable succesor = -1;
    let mutable predecessor = -1;
    let mutable keysList = new ResizeArray<_>()
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

            | GetPredecessor requestor ->
                requestor <! Notify predecessor

            | MarkPredecessor predecessorId ->
                if debug then printfn "INFO: Marking %i as predecessor for %i" predecessorId nodeId
                predecessor <- predecessorId

            | InitializeFingerTable ->
                if debug then printfn "INFO: Initializing Finger Table for %i" nodeId
                let response = (master <? GetRingList)
                let nodeList:Dictionary<int, bool> = Async.RunSynchronously response
                
                for exponent in [0..fingerTableSize] do
                    let mutable nextEntry = nodeId + (pown 2 exponent) - 1
                    // if nextEntry > numNodes then nextEntry <- nextEntry % numNodes
                    if (nextEntry <= numNodes) then
                        let successorId = findSuccessor(nextEntry, nodeList)
                        fingerTable.Add(successorId, globalNodesDict.[successorId])
                if not (fingerTable.ContainsKey(0)) && nodeId <> 0 then 
                    fingerTable.Add(0, globalNodesDict.[0])
                if debug then for entry in fingerTable do printfn "INFO: FingerTable for node %i with Key %i" nodeId entry.Key

            | DistributeKeys globalKeysList ->
                printfn "keys list length %i for node %i" globalKeysList.Length nodeId
                let mutable newKeyList =  new ResizeArray<_>()
                for key in globalKeysList do
                    if (key % numNodes) <= nodeId then
                        keysList.Add(key)
                    else    
                        newKeyList.Add(key)
                printfn "keys list length after processsing %i for node %i" newKeyList.Count succesor
                if newKeyList.Count > 0 then
                    globalNodesDict.[succesor] <! DistributeKeys (Seq.toList newKeyList)
                if debug then printfn "INFO: Distributing keys at node %i and current key count %i" nodeId keysList.Count 

            | StabilizeNodeReq ->
                globalNodesDict.[succesor] <? GetPredecessor mailbox.Self
                
            | Notify nextNodePredecessor ->      
                if (nextNodePredecessor <> -1) && (nextNodePredecessor <> nodeId) then
                    if debug then printfn "INFO: Updating succesor for %i with %i" nodeId nextNodePredecessor
                    succesor <- nextNodePredecessor
                    globalNodesDict.[succesor] <! MarkPredecessor nodeId

            | _ -> ()
        return! loop()
    }   
    loop()
//-------------------------------------- Worker Actor --------------------------------------//

//-------------------------------------- Main Program --------------------------------------//
stopWatch.Start()
master <! InitializeRing numNodes

if debug then printfn "Intializing the ring for %i nodes and Fingertable size %i" numNodes fingerTableSize
let key = "RingWorker0" 
let worker = spawn system (key) RingWorker
worker <! SetNodeId 0
globalNodesDict.Add(0, worker)
worker <! JoinRing numNodes

// Create nodes that will become part of ring
for nodeId in [1 .. numNodes] do
    let key: string = "RingWorker" + string(nodeId)
    let worker = spawn system (key) RingWorker
    worker <! SetNodeId nodeId
    globalNodesDict.Add(nodeId, worker)

// Generating a ring linearly
for node in [numNodes .. -1 .. 1] do
    let worker = globalNodesDict.[node] 
    let successorId = Async.RunSynchronously (master <? FindSuccessor node)
    worker <! JoinRing successorId

master <! StabilizeRing

for KeyValue(key, worker) in globalNodesDict do
    worker <! InitializeFingerTable

let keysList = [0 .. numNodes * numRequestsPerNode]
globalNodesDict.[0] <! DistributeKeys keysList

Console.ReadLine() |> ignore
//-------------------------------------- Main Program --------------------------------------//

system.WhenTerminated.Wait()