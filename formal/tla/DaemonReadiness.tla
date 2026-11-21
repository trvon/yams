--------------------------- MODULE DaemonReadiness ---------------------------
(***************************************************************************)
(* Daemon startup readiness and request dispatch.                          *)
(*                                                                         *)
(* One client sends one request of each kind (Get, Status, Ping, Add) to a *)
(* daemon that starts, brings up its content store and metadata repository *)
(* in either order, commits readiness, and may shut down. The document     *)
(* named by Get is stored before the daemon starts.                        *)
(*                                                                         *)
(* Line references are to origin/main 7080fe645 (2026-11-20).              *)
(*   RD  src/daemon/components/RequestDispatcher.cpp                       *)
(*   RDD src/daemon/components/dispatcher/request_dispatcher_documents.cpp *)
(*   DC  src/daemon/client/daemon_client.cpp                               *)
(*   DM  src/daemon/components/DatabaseManager.cpp                         *)
(*   SM  src/daemon/components/ServiceManager.cpp                          *)
(*   DS  src/app/services/document_service.cpp                             *)
(*   LF  src/daemon/components/DaemonLifecycleFsm.cpp                      *)
(*   YD  src/daemon/daemon.cpp                                             *)
(*   CAT src/cli/commands/cat_command.cpp                                  *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    DaemonFix,     \* TRUE: the gate answers ErrorResponse InvalidState (RD:479-497).
                   \* FALSE: the gate answers a StatusResponse (RD before 0b48eed50).
    ClientFix,     \* TRUE: sendRequestStreaming turns a StatusResponse to a non-status
                   \*       request into a not-ready error (DC:1416-1437).
                   \* FALSE: GetHandler drops the StatusResponse (DC:746-752).
    IpcStopsFirst  \* TRUE: stop() closes the IPC layer (YD:1217) before
                   \*       ServiceManager::shutdown (YD:1232), as the code does.
                   \* FALSE: the reverse order, to show what that order protects.

ASSUME DaemonFix \in BOOLEAN /\ ClientFix \in BOOLEAN /\ IpcStopsFirst \in BOOLEAN

Reqs == {"Get", "Status", "Ping", "Add"}

\* Reachable LifecycleState values (include/yams/daemon/components/DaemonLifecycleFsm.h:14).
LcStates == {"Unknown", "Initializing", "Ready", "Degraded", "Failed", "Stopping"}

\* What the daemon writes back.
Replies == {"None", "Status", "Pong", "ErrNotReady", "ErrInvalidState",
            "Content", "ErrNotFound", "Accepted"}

\* What the caller of DaemonClient sees.
Results == {"Pending", "Content", "NotReady", "NotFound", "Transport",
            "Status", "Pong", "Accepted", "Dropped"}

VARIABLES
    lc,        \* lifecycle state
    cs,        \* state_->readiness.contentStoreReady
    db,        \* state_->readiness.metadataRepoReady
    repo,      \* DatabaseManager::metadataRepo_ is non-null
    ipc,       \* "up": socket server accepting, connections open
    stop,      \* TRUE once stop() has begun
    dbShut,    \* DatabaseManager::shutdown has cleared the flag
    req,       \* per request: "Idle" | "Sent" | "Gated" | "Replied"
    reply,     \* per request: Replies
    res,       \* per request: Results
    addEarly,  \* history: an Add reached its handler with cs /\ ~db
    addRejCs   \* history: an Add was rejected for readiness while cs held

vars == <<lc, cs, db, repo, ipc, stop, dbShut, req, reply, res, addEarly, addRejCs>>
daemonVars == <<lc, cs, db, repo, ipc, stop, dbShut>>
reqVars == <<req, reply, res, addEarly, addRejCs>>

TypeOK ==
    /\ lc \in LcStates
    /\ {cs, db, repo, stop, dbShut, addEarly, addRejCs} \subseteq BOOLEAN
    /\ ipc \in {"up", "down"}
    /\ req \in [Reqs -> {"Idle", "Sent", "Gated", "Replied"}]
    /\ reply \in [Reqs -> Replies]
    /\ res \in [Reqs -> Results]

Init ==
    /\ lc = "Unknown"
    /\ cs = FALSE /\ db = FALSE /\ repo = FALSE
    /\ ipc = "up"            \* the socket answers status during startup
    /\ stop = FALSE /\ dbShut = FALSE
    /\ req = [r \in Reqs |-> "Idle"]
    /\ reply = [r \in Reqs |-> "None"]
    /\ res = [r \in Reqs |-> "Pending"]
    /\ addEarly = FALSE /\ addRejCs = FALSE

-----------------------------------------------------------------------------
(* Daemon: startup.                                                         *)

\* YD:484 dispatch(BootstrappedEvent{}); LF:31-36.
Bootstrap ==
    /\ lc = "Unknown" /\ ~stop
    /\ lc' = "Initializing"
    /\ UNCHANGED <<cs, db, repo, ipc, stop, dbShut>> /\ UNCHANGED reqVars

\* SM:1808 contentStoreReady = true.
ContentStoreUp ==
    /\ ~cs /\ ~stop /\ lc \in {"Unknown", "Initializing"}
    /\ cs' = TRUE
    /\ UNCHANGED <<lc, db, repo, ipc, stop, dbShut>> /\ UNCHANGED reqVars

\* DM:463-466: publish metadataRepo_, then set metadataRepoReady. One step:
\* the flag is never set without the repository.
MetadataRepoUp ==
    /\ ~db /\ ~dbShut /\ ~stop /\ lc \in {"Unknown", "Initializing"}
    /\ repo' = TRUE /\ db' = TRUE
    /\ UNCHANGED <<lc, cs, ipc, stop, dbShut>> /\ UNCHANGED reqVars

\* YD:726 tryCommitInitializationReadiness after ServiceManager reports Ready;
\* LF:164-197 enters Ready, or Degraded when the model provider is missing.
CommitReadiness ==
    /\ lc = "Initializing" /\ cs /\ db /\ ~stop
    /\ lc' \in {"Ready", "Degraded"}
    /\ UNCHANGED <<cs, db, repo, ipc, stop, dbShut>> /\ UNCHANGED reqVars

\* FailureEvent (LF:51-61): startup fails; services may stay down.
InitFails ==
    /\ lc \in {"Unknown", "Initializing"} /\ ~stop
    /\ lc' = "Failed"
    /\ UNCHANGED <<cs, db, repo, ipc, stop, dbShut>> /\ UNCHANGED reqVars

-----------------------------------------------------------------------------
(* Daemon: shutdown (YamsDaemon::stop, YD:1145).                            *)

\* YD:1164 requestLifecycleClose -> ShutdownRequestedEvent (LF:63-67).
RequestStop ==
    /\ ~stop
    /\ stop' = TRUE
    /\ lc' = IF lc = "Failed" THEN lc ELSE "Stopping"
    /\ UNCHANGED <<cs, db, repo, ipc, dbShut>> /\ UNCHANGED reqVars

\* YD:1217 shutdownIpcLayer(): the socket server and its connections close.
IpcDown ==
    /\ stop /\ ipc = "up"
    /\ IpcStopsFirst \/ dbShut
    /\ ipc' = "down"
    /\ UNCHANGED <<lc, cs, db, repo, stop, dbShut>> /\ UNCHANGED reqVars

\* YD:1232 ServiceManager::shutdown -> SM:1503 DatabaseManager::shutdown.
\* DM:108 clears metadataRepoReady; DM:114-115 later drops metadataRepo_.
DbFlagDown ==
    /\ stop /\ ~dbShut
    /\ ~IpcStopsFirst \/ ipc = "down"
    /\ db' = FALSE /\ dbShut' = TRUE
    /\ UNCHANGED <<lc, cs, repo, ipc, stop>> /\ UNCHANGED reqVars

DbRepoDown ==
    /\ dbShut /\ repo
    /\ repo' = FALSE
    /\ UNCHANGED <<lc, cs, db, ipc, stop, dbShut>> /\ UNCHANGED reqVars

-----------------------------------------------------------------------------
(* Request path.                                                            *)

\* DC:1395 sendRequestStreaming. A closed socket fails the connect.
Send(r) ==
    /\ req[r] = "Idle"
    /\ IF ipc = "up"
          THEN /\ req' = [req EXCEPT ![r] = "Sent"]
               /\ UNCHANGED res
          ELSE /\ req' = [req EXCEPT ![r] = "Replied"]
               /\ res' = [res EXCEPT ![r] = "Transport"]
    /\ UNCHANGED <<reply, addEarly, addRejCs>> /\ UNCHANGED daemonVars

\* Reply of the readiness gate to a request it rejects (RD:479-497).
GateReject == IF DaemonFix THEN "ErrNotReady" ELSE "Status"

Answer(r, rep) ==
    /\ reply' = [reply EXCEPT ![r] = rep]
    /\ req' = [req EXCEPT ![r] = "Replied"]

\* RequestDispatcher::dispatch (RD:451) up to the handler call (RD:553).
\* Readiness flags are read once here, as the code does.
Dispatch(r) ==
    /\ req[r] = "Sent" /\ ipc = "up"
    /\ LET isAdd == r = "Add"
           gated == lc \notin {"Ready", "Degraded"}             \* RD:471-472
           coreReady == cs /\ db                                \* RD:476-478
           rejectGate == gated /\ ~coreReady /\ ~(isAdd /\ cs)  \* RD:479
           rejectDb == ~db /\ ~(isAdd /\ cs)                    \* RD:512-519
           rejectCs == ~cs                                      \* RD:521-529
       IN
       CASE r = "Status" ->                                     \* RD:453-455
              Answer(r, "Status") /\ UNCHANGED <<addEarly, addRejCs>>
         [] r = "Ping" ->                                       \* RD:456-458
              Answer(r, "Pong") /\ UNCHANGED <<addEarly, addRejCs>>
         [] rejectGate ->
              /\ Answer(r, GateReject)
              /\ addRejCs' = (addRejCs \/ (isAdd /\ cs))
              /\ UNCHANGED addEarly
         [] rejectDb \/ rejectCs ->
              /\ Answer(r, "ErrInvalidState")
              /\ addRejCs' = (addRejCs \/ (isAdd /\ cs))
              /\ UNCHANGED addEarly
         [] OTHER ->
              /\ req' = [req EXCEPT ![r] = "Gated"]
              /\ addEarly' = (addEarly \/ (isAdd /\ cs /\ ~db))
              /\ UNCHANGED <<reply, addRejCs>>
    /\ UNCHANGED res /\ UNCHANGED daemonVars

\* Handler body on a worker (RDD:991). Get by name resolves through the
\* metadata repository; a null repository is NotInitialized, and retrieve()
\* reports any resolve failure as NotFound (DS:2106-2110).
Handle(r) ==
    /\ req[r] = "Gated"
    /\ Answer(r, IF r = "Get" THEN (IF repo THEN "Content" ELSE "ErrNotFound")
                 ELSE "Accepted")
    /\ UNCHANGED <<res, addEarly, addRejCs>> /\ UNCHANGED daemonVars

\* Client mapping: DC:1416-1437 (notReady lambda in onHeader/onChunk),
\* DC:746-752 (GetHandler::onHeaderReceived), DC:165 makeErrorFromResponse.
ClientResult(r, rep) ==
    CASE rep = "Status" ->
            IF r = "Status" THEN "Status"
            ELSE IF ClientFix THEN "NotReady"     \* makeNotReadyErrorFromStatus, DC:147
            \* Old client: GetHandler ignores the StatusResponse and returns an
            \* empty GetResponse; cat prints "Document content not found" (CAT:131).
            ELSE IF r = "Get" THEN "NotFound" ELSE "Dropped"
      [] rep = "Pong" -> "Pong"
      [] rep \in {"ErrNotReady", "ErrInvalidState"} -> "NotReady"
      [] rep = "ErrNotFound" -> "NotFound"
      [] rep = "Content" -> "Content"
      [] rep = "Accepted" -> "Accepted"

Deliver(r) ==
    /\ req[r] = "Replied" /\ reply[r] # "None" /\ res[r] = "Pending"
    /\ ipc = "up"
    /\ res' = [res EXCEPT ![r] = ClientResult(r, reply[r])]
    /\ UNCHANGED <<req, reply, addEarly, addRejCs>> /\ UNCHANGED daemonVars

\* The IPC layer closed under a pending request: the client read loop sees EOF.
Abort(r) ==
    /\ ipc = "down" /\ req[r] # "Idle" /\ res[r] = "Pending"
    /\ res' = [res EXCEPT ![r] = "Transport"]
    /\ UNCHANGED <<req, reply, addEarly, addRejCs>> /\ UNCHANGED daemonVars

\* All requests answered: stutter, so TLC's deadlock check still flags any
\* other state with no successor.
Finished ==
    /\ \A r \in Reqs : res[r] # "Pending"
    /\ UNCHANGED vars

DaemonNext ==
    \/ Bootstrap \/ ContentStoreUp \/ MetadataRepoUp \/ CommitReadiness \/ InitFails
    \/ RequestStop \/ IpcDown \/ DbFlagDown \/ DbRepoDown

ReqNext(r) == Send(r) \/ Dispatch(r) \/ Handle(r) \/ Deliver(r) \/ Abort(r)

Next == DaemonNext \/ (\E r \in Reqs : ReqNext(r)) \/ Finished

\* Fairness on the request path only: startup may stall forever, but a
\* request the daemon received must still be answered.
Spec == Init /\ [][Next]_vars /\ \A r \in Reqs : WF_vars(ReqNext(r))

-----------------------------------------------------------------------------
(* Properties.                                                              *)

\* A get of a stored document yields content, a not-ready error or a
\* transport error, never NotFound.
GetNeverNotFound == res["Get"] \in {"Pending", "Content", "NotReady", "Transport"}

\* Status and Ping bypass readiness: answered in every lifecycle state.
StatusPingNeverGated ==
    /\ res["Status"] \in {"Pending", "Status", "Transport"}
    /\ res["Ping"] \in {"Pending", "Pong", "Transport"}

\* AddDocument is never rejected for readiness once the content store is up,
\* even while the metadata repository initializes (RD:479, RD:513).
AddNeverRejectedWithStore == ~addRejCs

\* Reachability witness: fails once an Add reaches its handler with cs /\ ~db.
\* The *_witness configuration expects this invariant to be violated.
NoAddEarly == ~addEarly

\* Every request is answered.
AllAnswered == \A r \in Reqs : <>(res[r] # "Pending")
=============================================================================
