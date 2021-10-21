
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic

//-------------------------------------- Initialization --------------------------------------//
type ChordMessageTypes =
    | TotalNodes of int
    | InitailizeRing of IActorRef []
    | JoinRing of int
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

//-------------------------------------- Master Actor --------------------------------------//
let RingMaster(mailbox: Actor<_>) =
    
    let mutable nodeCount = 0
    let mutable totalNumNodes = 0

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        match msg with 
        | TotalNodes n -> totalNumNodes <- n
        | InitailizeRing (numNodes) -> 
            nodeCount <- nodeCount + 1
            if nodeCount = totalNumNodes then
                stopWatch.Stop()
                printfn "Time for convergence: %f ms" stopWatch.Elapsed.TotalMilliseconds
                printfn "------------- End Gossip -------------"
                Environment.Exit(0)
        | JoinRing nodeId ->
            printfn "Node %i Requested to Join" nodeId
        | ConvergeRing ->
            nodeCount <- nodeCount + 1
            if nodeCount = totalNumNodes then
                stopWatch.Stop()
                printfn "Time for convergence: %f ms" stopWatch.Elapsed.TotalMilliseconds
                printfn "------------- End Transfer -------------"
                Environment.Exit(0)
        | _ -> ()

        return! loop()
    }            
    loop()

let master = spawn system "Master" RingMaster
//-------------------------------------- Supervisor Actor --------------------------------------//

//-------------------------------------- Worker Actor --------------------------------------//
    // Activate Gossip worker first initialize neighbour and then select a random node for Gossip

let RingWorker (mailbox: Actor<_>) =
    let mutable nodeId = -1;
    let mutable succesor = 0;
    let mutable predecessor = -1;

    let rec loop()= actor{
        let! message = mailbox.Receive();
        try
            match message with
            | SetId Id ->
                nodeId <- Id
            | JoinRing succesorID ->
                succesor <- succesorID
            | _ -> ()
        with
            | :? System.IndexOutOfRangeException -> printfn "Tried to access outside array!" |> ignore
        return! loop()
    }            
    loop()


//-------------------------------------- Worker Actor --------------------------------------//

//-------------------------------------- Main Program --------------------------------------//
for x in [0..numNodes] do
    let key: string = "RingWorker" + string(x)
    let worker = spawn system (key) RingWorker
    worker <! SetId x
    nodeArray.Add(worker)
    globalNodesDict.Add(x, worker)

for x in [0..numNodes] do
    let rndNodeId = Random().Next(0, nodeArray.Count)
    master <! JoinRing rndNodeId
    nodeArray.RemoveAt(rndNodeId) |> ignore

stopWatch.Start()
// Select a random worker to act as master
let leader = Random().Next(0, numNodes)
master <! TotalNodes(numNodes)


Console.ReadLine() |> ignore
//-------------------------------------- Main Program --------------------------------------//

