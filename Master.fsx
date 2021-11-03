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
    | FindSuccessor of int
    | JoinRing of int * bool
    | StabilizeRing
    | StartSearch
    | InitializeFingerTable
    | InitializeRing of Dictionary<int,IActorRef>
    | CountSearches
    | CountHops
    | GetRingList

type RingWorkerMessage =
    | SetNodeId of int
    | InitializeKeys of int
    | SetSuccessor of int * IActorRef
    | SetPredecessor of int * IActorRef
    | CreateFingerTable of list<int> * Dictionary<int,IActorRef>
    | StabilizeNodeReq
    | GetPredecessor of IActorRef
    | Notify of NodeMetaInfo
    | DistributeKeys of list<int> * IActorRef
    | FindKey of int * IActorRef * IActorRef
    | FoundKey of int * int * IActorRef
    | PrintRing

let numNodes = fsi.CommandLineArgs.[1] |> int
let numRequestsPerNode = fsi.CommandLineArgs.[2] |> int
let stopWatch = Diagnostics.Stopwatch()
let system = ActorSystem.Create("System")
let debug = true
let rand = Random()

if numNodes <= 0 || numRequestsPerNode <= 0 then
    printfn "Invalid input"
    Environment.Exit(0)
//-------------------------------------- Initialization --------------------------------------//

//-------------------------------------- Utils --------------------------------------//
let rec divideLoop nodeSize =
    let mutable tableSize = 0
    let mutable maxNodes = nodeSize
    while maxNodes > 0 do
        tableSize <- tableSize + 1
        maxNodes <- (maxNodes >>> 1)
    tableSize        

let findSuccessorDict (nodeId:int, nodeList:Dictionary<int,_>) =
    let mutable flag = true
    let mutable successor = 0
    if nodeId < numNodes then
        for id in (nodeId + 1) .. numNodes do 
            if nodeList.ContainsKey id && flag then
                successor <- id
                flag <- false
    successor 

let findSuccessor (nodeId:int, nodeList:list<int>) =
    // fst( nodeList |> List.indexed |> List.find ( fun(index, value) -> index > nodeId && value ) )  
    try
        let successor = nodeList |> List.sort |> List.find ( fun(elem) -> elem > nodeId )
        successor
    with 
        | :?  System.Collections.Generic.KeyNotFoundException -> 0

let searchFingertable (keyToFind:int, fingerTable:list<int>) =
    let mutable successor = 0
    for nodeId in  fingerTable do if  keyToFind >= nodeId && nodeId >= successor then successor <- nodeId
    successor

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
    
    let mutable searchCount = 0
    let mutable hopCount = 0
    let mutable localNodeList = []
    let mutable nodeSaturationCount = 0
    let mutable globalNodesDict = new Dictionary<int,IActorRef>()

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        let response = mailbox.Sender();
        try
            match msg with 
                | InitializeRing nodeDict ->
                    printfn "Intializing the ring for %i nodes and Fingertable size %i" numNodes fingerTableSize
                    globalNodesDict <- nodeDict
 
                | JoinRing (nodeId, selfjoin) ->
                    if nodeId > 0 then
                        if not selfjoin then
                            let successorId = findSuccessor(nodeId, localNodeList)
                            if debug then printfn "INFO: Found successor %i for %i" successorId nodeId
                            globalNodesDict.[nodeId] <! SetSuccessor (successorId, globalNodesDict.[successorId])
                        localNodeList <- localNodeList @ [nodeId]

                | GetRingList ->
                    response <! localNodeList

                | CountSearches ->
                    searchCount <- searchCount + 1
                    if searchCount = (numNodes * numRequestsPerNode) then
                        stopWatch.Stop()
                        printfn "Time for convergence: %f ms" stopWatch.Elapsed.TotalMilliseconds
                        let avgHopCount = (hopCount |> double)/((numNodes*numRequestsPerNode) |> double)
                        printfn "Search complete with Total %i hops and Average hop count per request %f" hopCount avgHopCount
                        Environment.Exit(0)

                | CountHops ->
                    hopCount <- hopCount + 1
                
                | StabilizeRing ->
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

                | InitializeFingerTable ->
                    for KeyValue(key, worker) in globalNodesDict do
                        worker <! CreateFingerTable (localNodeList, globalNodesDict)

                | StartSearch -> 
                    nodeSaturationCount <- nodeSaturationCount + 1
                    localNodeList <- List.sort localNodeList
                    if nodeSaturationCount = numNodes then
                        printfn "Distributed all keys searching"
                        for KeyValue(key, worker) in globalNodesDict do
                            for numKeys in [1 .. numRequestsPerNode] do
                                let randomKey = rand.Next(1, (numNodes * numRequestsPerNode))
                                // System.Threading.Thread.Sleep(5000)
                                worker <! FindKey(randomKey, worker, mailbox.Self) 

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
    let mutable keysList = []
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
                // if predecessor.NodeId <> -1 then predecessor.NodeInstance <! SetSuccessor(predecessorId, predecessorRef)
                predecessor.NodeId <- predecessorId
                predecessor.NodeInstance <- predecessorRef
            
            | StabilizeNodeReq ->
                successor.NodeInstance <! GetPredecessor mailbox.Self

            | Notify nextNodePredecessor  ->
                if nodeId <> -1 then
                    if (nextNodePredecessor.NodeId <> -1) && (nextNodePredecessor.NodeId > nodeId) then
                        if debug then printfn "INFO: Updating successor for %i with %i" nodeId nextNodePredecessor.NodeId
                        successor.NodeId <- nextNodePredecessor.NodeId
                        successor.NodeInstance <- nextNodePredecessor.NodeInstance 
                        successor.NodeInstance <! SetPredecessor (nodeId, mailbox.Self)

            | CreateFingerTable (nodeList, globalNodeDict) ->
                if debug then printfn "INFO: Initializing Finger Table for %i" nodeId
                
                for exponent in [0..fingerTableSize] do
                    let mutable nextEntry = nodeId + (pown 2 exponent) - 1
                    // if nextEntry > numNodes then nextEntry <- nextEntry % numNodes
                    if (nextEntry <= numNodes) then
                        let successorId = findSuccessor(nextEntry, nodeList)
                        fingerTable.Add(successorId, globalNodeDict.[successorId])

                if not (fingerTable.ContainsKey(0)) && nodeId <> 0 then 
                    fingerTable.Add(0, globalNodeDict.[0])
                // if debug then for entry in fingerTable do printfn "INFO: FingerTable for node %i with Key %i" nodeId entry.Key

            | DistributeKeys (globalKeysList, master) ->
                master <! StartSearch
                let mutable newKeyList =  []
                for key in globalKeysList do
                    if (key % numNodes) >= nodeId then
                        keysList <- keysList @ [key]
                    else    
                        newKeyList <- newKeyList @ [key]

                if newKeyList.Length > 0 then
                    if predecessor.NodeId <> -1 then
                        predecessor.NodeInstance <! DistributeKeys (newKeyList, master)
                    else 
                        printfn "INFO: Ring is disconnected at %i stabilising" nodeId
                        mailbox.Self <! StabilizeNodeReq
                        mailbox.Self <! DistributeKeys (newKeyList, master)
                if debug then printfn "INFO: Distributing keys at node %i and current key count %i and recived keys %i" nodeId keysList.Length globalKeysList.Length 
            
            | FindKey (keyToFind, requestorRef, master) ->
                    // printfn "Request to find key %i at node %i with key count %i" keyToFind nodeId keysList.Length
                    master <! CountHops 
                    let mutable keyFound = false
                    for keys in keysList do
                        if keyToFind = keys then
                            keyFound <- true
                            requestorRef <! FoundKey (keyToFind, nodeId, master)
                    
                    if not keyFound then 
                        let ftKeyList = [ for KeyValue(key, value) in fingerTable do yield key ]
                        let nextNode =  searchFingertable(keyToFind % numNodes, ftKeyList)
                        if (nodeId = 0) && (nextNode = 0) then
                            if debug then printfn "INFO: key %i not found at %i and table is %A" keyToFind  nodeId keysList  
                        else
                            fingerTable.[nextNode] <! FindKey (keyToFind, requestorRef, master)

            | FoundKey (keyToFind, founderId, master) ->
                if debug then printfn "Found key %i at node %i" keyToFind founderId
                master <! CountSearches

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

// Create nodes that will become part of ring
for nodeId in [0 .. numNodes] do
    let key: string = "RingWorker" + string(nodeId)
    let worker = spawn system (key) RingWorker
    worker <! SetNodeId nodeId
    globalNodesDict.Add(nodeId, worker)

master <! InitializeRing globalNodesDict
master <! JoinRing (0, false)

// Generating a ring linearly by joining nodes
// for nodeId in [numNodes .. -1 .. 1] do
//     master <! JoinRing (nodeId, false)

for nodeId in [numNodes .. -1 .. 1] do
    let nodesList = Async.RunSynchronously(master <? GetRingList)
    let successorId = findSuccessor(nodeId, nodesList)
    if debug then printfn "INFO: Found successor %i for %i" successorId nodeId
    globalNodesDict.[nodeId] <! SetSuccessor (successorId, globalNodesDict.[successorId])
    master <! JoinRing (nodeId, true)

printfn "Joined all nodes"
master <! StabilizeRing

master <! InitializeFingerTable

let keysList = [0 .. (numNodes * numRequestsPerNode)]
let lastNode = ([ for KeyValue(key, value) in globalNodesDict do yield key ] |> List.max)
globalNodesDict.[lastNode] <! DistributeKeys (keysList,master)

Console.ReadLine() |> ignore
//-------------------------------------- Main Program --------------------------------------//

system.WhenTerminated.Wait()