
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic

//-------------------------------------- Initialization --------------------------------------//
type RingMasterMessage = 
    | JoinRing of int
    | InitializeRing
    | TotalNodes of int

type RingWorkerMessage =
    | FindSuccessor of int
    | SetId of int
    | ConvergeRing
    | InitializeKeys of int

let numNodes = fsi.CommandLineArgs.[1] |> int
let numRequests = fsi.CommandLineArgs.[2] |> int
let stopWatch = Diagnostics.Stopwatch()
let system = ActorSystem.Create("System")
let mutable globalNodesDict = new Dictionary<int,IActorRef>()
let nodeList = ResizeArray()

if numNodes <= 0 || numRequests <= 0 then
    printfn "Invalid input"
    Environment.Exit(0)
//-------------------------------------- Initialization --------------------------------------//

//-------------------------------------- Utils --------------------------------------//
// let nthroot n A =
//     let rec f x =
//         let m = n - 1.
//         let x' = (m * x + A/x**m) / n
//         match abs(x' - x) with
//         | t when t < abs(x * 1e-9) -> x'
//         | _ -> f x'
//     f (A / double n)
//-------------------------------------- Utils --------------------------------------//

//-------------------------------------- Worker Actor --------------------------------------//
let RingWorker (mailbox: Actor<_>) =
    let mutable nodeId = -1;
    let mutable succesor = numNodes;
    let mutable predecessor = -1;

    let rec loop()= actor{
        let! message = mailbox.Receive();
        match message with
        | SetId Id ->
            nodeId <- Id
        | JoinRing succesorId ->
            succesor <- succesorId
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
                nodeList.Add(worker)
                globalNodesDict.Add(0, worker)
                worker <! JoinRing numNodes
            | FindSuccessor nodeId ->
                printfn "Node %i Requested to Join" nodeId
                let successorId = [for id in nodeId .. numNodes do if globalNodesDict.ContainsKey id then yield id]
                printfn "Found succesor %i for %i" successorId.[0] nodeId
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
printfn "Starting execution"
master <! TotalNodes(numNodes)
master <! InitializeRing

// Create nodes that will become part of ring
for x in [1..numNodes] do
    let key: string = "RingWorker" + string(x)
    let worker = spawn system (key) RingWorker
    worker <! SetId x
    nodeList.Add(worker)
    globalNodesDict.Add(x, worker)

// Select a random node and join it to ring
for x in [1..numNodes] do
    let rndNodeId = Random().Next(0, nodeList.Count)
    let worker = globalNodesDict.[rndNodeId] 
    let response =  (master <? FindSuccessor rndNodeId)
    let successorId = Async.RunSynchronously (response, 2500)
    worker <! JoinRing successorId 
    nodeList.RemoveAt(rndNodeId) |> ignore

Console.ReadLine() |> ignore
//-------------------------------------- Main Program --------------------------------------//

system.WhenTerminated.Wait()
