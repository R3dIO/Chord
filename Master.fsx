#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic

//-------------------------------------- Initialization --------------------------------------//
type NodeMetaInfo = {
        mutable NodeId: int
        mutable NodeInstance: IActorRef
    }

type RingMasterMessage = 
    | NotifyMaster of int
    | FindSuccessor of int
    | JoinRing of int * Dictionary<int,IActorRef>
    | StabilizeRing of Dictionary<int,IActorRef>
    | ConvergeRing
    | GetRingList

type RingWorkerMessage =
    | SetNodeId of int
    | InitializeKeys of int
    | SetSuccessor of int * IActorRef
    | SetPredecessor of int * IActorRef
    | InitializeFingerTable of IActorRef * Dictionary<int,IActorRef>
    | StabilizeNodeReq
    | GetPredecessor of IActorRef
    | Notify of NodeMetaInfo
    | DistributeKeys of list<int>
    | PrintRing

let numNodes = fsi.CommandLineArgs.[1] |> int
let numRequestsPerNode = fsi.CommandLineArgs.[2] |> int
let stopWatch = Diagnostics.Stopwatch()
let system = ActorSystem.Create("System")
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

let findSuccessorDict (nodeId:int, nodeList:Dictionary<int,_>) =
    let mutable flag = true
    let mutable successor = 0
    if nodeId < numNodes then
        for id in (nodeId + 1) .. numNodes do 
            if nodeList.ContainsKey id && flag then
                // if debug then printfn "found id %i" id
                successor <- id
                flag <- false
    successor 

let findSuccessor (nodeId:int, nodeList:list<int>) =
    // let successor = nodeList |> List.indexed |> List.find ( fun(index, value) -> index > nodeId && value ) 
    // fst(successor) 
    try
        let successor = nodeList |> List.sort |> List.find ( fun(elem) -> elem > nodeId )
        successor
    with 
        | :?  System.Collections.Generic.KeyNotFoundException -> 0

let RandomJoin(maxNodes:int, globalNodesDict: Dictionary<int, IActorRef>, nodeList:ResizeArray<int>, master:IActorRef) = 
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
    let mutable localNodeList = []

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        let response = mailbox.Sender();
        try
            match msg with 
                | JoinRing (nodeId, globalNodeDict) ->
                    let successorId = findSuccessor(nodeId, localNodeList)
                    if debug then printfn "INFO: Found successor %i for %i" successorId nodeId
                    globalNodeDict.[nodeId] <! SetSuccessor (successorId, globalNodeDict.[successorId])
                    localNodeList <- localNodeList @ [nodeId]

                | GetRingList ->
                    response <! localNodeList

                | StabilizeRing globalNodesDict ->
                    if debug then printfn "INFO: Stabilizing the Ring"
                    for KeyValue(key, worker) in globalNodesDict do
                        if key = 0 then
                            let successorId = findSuccessor(0, localNodeList)
                            globalNodesDict.[0] <! SetSuccessor (successorId, globalNodesDict.[successorId])
                        worker <! StabilizeNodeReq
  
                    // if debug then printfn "INFO: Ring After Stabilization"; globalNodesDict.[0] <! PrintRing     
                    let delay = async { do! Async.Sleep(5000) }
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

//-------------------------------------- Master Actor --------------------------------------//

//-------------------------------------- Worker Actor --------------------------------------//
let RingWorker (mailbox: Actor<_>) =
    let mutable nodeId = -1;
    let mutable successor = {NodeId = -1; NodeInstance = null};
    let mutable predecessor = {NodeId = -1; NodeInstance = null};
    let mutable keysList = new ResizeArray<_>()
    let mutable fingerTable = new Dictionary<int,IActorRef>()

    let rec loop()= actor{
        let! message = mailbox.Receive();
        let response = mailbox.Sender();
        match message with
            | SetNodeId Id ->
                nodeId <- Id

            | SetSuccessor(successorId, nodeRef) ->
                if nodeId <> -1 then
                    try 
                        successor.NodeId <- successorId
                        successor.NodeInstance <- nodeRef
                        successor.NodeInstance <! SetPredecessor (nodeId, mailbox.Self)
                    with 
                        | :?  System.Collections.Generic.KeyNotFoundException ->  printfn "ERROR: Key doesn't exist" |> ignore

            | GetPredecessor requestor ->
                requestor <! Notify predecessor

            | SetPredecessor (predecessorId, predecessorRef) ->
                if debug then printfn "INFO: Marking %i as predecessor for %i" predecessorId nodeId
                predecessor.NodeId <- predecessorId
                predecessor.NodeInstance <- predecessorRef
            
            | StabilizeNodeReq ->
                successor.NodeInstance <? GetPredecessor mailbox.Self

            | Notify nextNodePredecessor  ->
                if nodeId <> -1 then
                    if (nextNodePredecessor.NodeId <> -1) && (nextNodePredecessor.NodeId > nodeId) then
                        if debug then printfn "INFO: Updating successor for %i with %i" nodeId nextNodePredecessor.NodeId
                        successor.NodeId <- nextNodePredecessor.NodeId
                        successor.NodeInstance <- nextNodePredecessor.NodeInstance 
                        successor.NodeInstance <! SetPredecessor (nodeId, mailbox.Self)

            | InitializeFingerTable (master, globalNodeDict) ->
                if debug then printfn "INFO: Initializing Finger Table for %i" nodeId
                let nodeList = Async.RunSynchronously (master <? GetRingList)
                
                for exponent in [0..fingerTableSize] do
                    let mutable nextEntry = nodeId + (pown 2 exponent) - 1
                    // if nextEntry > numNodes then nextEntry <- nextEntry % numNodes
                    if (nextEntry <= numNodes) then
                        let successorId = findSuccessor(nextEntry, nodeList)
                        fingerTable.Add(successorId, globalNodeDict.[successorId])

                if not (fingerTable.ContainsKey(0)) && nodeId <> 0 then 
                    fingerTable.Add(0, globalNodeDict.[0])
                if debug then for entry in fingerTable do printfn "INFO: FingerTable for node %i with Key %i" nodeId entry.Key

            | DistributeKeys globalKeysList ->
                if debug then printfn "INFO: Distributing keys at node %i and current key count %i and recived keys %i" nodeId keysList.Count globalKeysList.Length 
 
                let mutable newKeyList =  []
                for key in globalKeysList do
                    if (key % numNodes) <= nodeId then
                        keysList.Add(key)
                    else    
                        newKeyList <- newKeyList @ [key]

                if newKeyList.Length > 0 then
                    successor.NodeInstance <! DistributeKeys newKeyList

            | PrintRing ->
                // if (successor <> 0) then
                    printf "%i --->>> " nodeId
                    successor.NodeInstance <! PrintRing

            | _ -> ()
        return! loop()
    }   
    loop()
//-------------------------------------- Worker Actor --------------------------------------//

//-------------------------------------- Main Program --------------------------------------//
stopWatch.Start()
let mutable globalNodesDict = new Dictionary<int,IActorRef>()
let master = spawn system "Master" RingMaster

if debug then printfn "Intializing the ring for %i nodes and Fingertable size %i" numNodes fingerTableSize

// Create nodes that will become part of ring
for nodeId in [0 .. numNodes] do
    let key: string = "RingWorker" + string(nodeId)
    let worker = spawn system (key) RingWorker
    worker <! SetNodeId nodeId
    globalNodesDict.Add(nodeId, worker)

master <! JoinRing(0, globalNodesDict)

// Generating a ring linearly by joining nodes
for nodeId in [numNodes .. -1 .. 1] do
    master <! JoinRing(nodeId, globalNodesDict)

master <! StabilizeRing globalNodesDict

for KeyValue(key, worker) in globalNodesDict do
    worker <! InitializeFingerTable (master, globalNodesDict)

System.Threading.Thread.Sleep(500)

let keysList = [0 .. numNodes * numRequestsPerNode]
globalNodesDict.[0] <! DistributeKeys keysList

Console.ReadLine() |> ignore
//-------------------------------------- Main Program --------------------------------------//

system.WhenTerminated.Wait()