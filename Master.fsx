
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic

//-------------------------------------- Initialization --------------------------------------//
type ChordMessageTypes =
    | TotalNodes of int
    | JoinRing
    | FindSuccessor of int
    | InitializeRing
    | SetId of int
    | ConvergeRing
    | InitializeKeys of int

let numNodes = int (string (fsi.CommandLineArgs.GetValue 1))
let numRequests = int(string (fsi.CommandLineArgs.GetValue 2))
let stopWatch = Diagnostics.Stopwatch()
let system = ActorSystem.Create("System")
let mutable globalNodesDict = new Dictionary<int,IActorRef>()
let nodeArray = new List<IActorRef>()

if numNodes <= 0 || numRequests <= 0 then
    printfn "Invalid input"
    Environment.Exit(0)
//-------------------------------------- Initialization --------------------------------------//

//-------------------------------------- Utils --------------------------------------//
let nthroot n A =
    let rec f x =
        let m = n - 1.
        let x' = (m * x + A/x**m) / n
        match abs(x' - x) with
        | t when t < abs(x * 1e-9) -> x'
        | _ -> f x'
    f (A / double n)
//-------------------------------------- Utils --------------------------------------//

//-------------------------------------- Worker Actor --------------------------------------//
    // Activate Gossip worker first initialize neighbour and then select a random node for Gossip

let RingWorker (mailbox: Actor<_>) =
    let mutable nodeId = -1;
    let mutable succesor = numNodes;
    let mutable predecessor = -1;

    let rec loop()= actor{
        let! message = mailbox.Receive();
        match message with
        | SetId Id ->
            nodeId <- Id
        | JoinRing ->
            succesor <- succesorID
        | _ -> ()
        return! loop()
    }            
    loop()
//-------------------------------------- Worker Actor --------------------------------------//

//-------------------------------------- Master Actor --------------------------------------//
let RingMaster(mailbox: Actor<_>) =
    
    let mutable requestCount = 0
    let mutable totalNumNodes = 0

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        let response = mailbox.Sender();
        try
        match msg with 
            | TotalNodes n -> totalNumNodes <- n
            | InitializeRing ->
                printfn "Intializing the ring"
                let key = "RingWorker0" 
                let worker = spawn system (key) RingWorker
                worker <! SetId 0
                nodeArray.Add(worker)
                globalNodesDict.Add(0, worker)
                worker <! JoinRing numNodes
            | FindSuccessor nodeId ->
                printfn "Node %i Requested to Join" nodeId
                let successorId = [for id in nodeId .. numNodes do if globalNodesDict.ContainsKey id then yield id]
                response <! successorId
            | ConvergeRing ->
                requestCount <- requestCount + 1
                if requestCount = numRequests then
                    stopWatch.Stop()
                    printfn "Time for convergence: %f ms" stopWatch.Elapsed.TotalMilliseconds
                    printfn "------------- End Transfer -------------"
                    Environment.Exit(0)
            | _ -> ()
        with
            | :? System.IndexOutOfRangeException -> printfn "Tried to access outside array!" |> ignore
        return! loop()
    }            
    loop()

let master = spawn system "Master" RingMaster
//-------------------------------------- Master Actor --------------------------------------//

//-------------------------------------- Main Program --------------------------------------//
stopWatch.Start()
master <! TotalNodes(numNodes)
master <! InitializeRing

// Create nodes that will become part of ring
for x in [1..numNodes] do
    let key: string = "RingWorker" + string(x)
    let worker = spawn system (key) RingWorker
    worker <! SetId x
    nodeArray.Add(worker)
    globalNodesDict.Add(x, worker)

// Select a random node and join it to ring
for x in [1..numNodes] do
    let rndNodeId = Random().Next(0, nodeArray.Count)
    let worker = globalNodesDict.[rndNodeId] 
    let response =  (master <? FindSuccessor rndNodeId)
    let successorId = Async.RunSynchronously (response, 2500)
    worker <! JoinRing successorId 
    nodeArray.RemoveAt(rndNodeId) |> ignore

Console.ReadLine() |> ignore
//-------------------------------------- Main Program --------------------------------------//

