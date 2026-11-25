---------------------------- MODULE ClientIoReset ----------------------------
(***************************************************************************)
(* Client connection pool and the GlobalIOContext reset lock.              *)
(*                                                                         *)
(* Requests run as coroutines on a fixed set of IO threads. A step of a    *)
(* request runs on one free IO thread and returns it at the next co_await. *)
(* The one exception is a blocking lock_shared(): the thread stays taken   *)
(* until the lock is granted. One reset() runs on a non-IO thread.         *)
(*                                                                         *)
(* Line references are to origin/main 7080fe645 (2026-11-20), except the  *)
(* guard GIO:159-176, which is the CheckFirst version.                     *)
(*   GIO src/daemon/client/global_io_context.cpp                           *)
(*   POOL src/daemon/client/asio_connection_pool.cpp                       *)
(*   CONN src/daemon/client/asio_connection.cpp                            *)
(*   TR  src/daemon/client/asio_transport.cpp                              *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Reqs,        \* request ids (model values)
    NIO,         \* number of IO threads
    FailFast,    \* TRUE: 6d5c9510a. reset() announces itself (GIO:142) and
                 \*       create_connection() uses try_acquire_operation_guard()
                 \*       (POOL:747-750, GIO:159-176).
                 \* FALSE: before 6d5c9510a. create_connection() blocks in
                 \*       acquire_operation_guard() (GIO:155-157).
    WriterPref,  \* TRUE: a waiting writer stops new readers (libc++, Windows SRW).
                 \* FALSE: readers enter while a writer waits (glibc default).
    MaxRetry,    \* EOF retries per request (TR:650-662 retry loop)
    Recycle,     \* TRUE: finished requests start again (unbounded load)
    CheckFirst   \* TRUE: current code. try_acquire_operation_guard() reads
                 \*       resets_in_progress_ before each try_lock_shared
                 \*       (GIO:165-172), so it fails whenever a reset is announced.
                 \* FALSE: 6d5c9510a. It calls try_lock_shared first and reads
                 \*       the counter only after a failure.

ASSUME NIO \in Nat \ {0} /\ MaxRetry \in Nat
ASSUME FailFast \in BOOLEAN /\ WriterPref \in BOOLEAN /\ Recycle \in BOOLEAN
ASSUME CheckFirst \in BOOLEAN /\ (CheckFirst => FailFast)

\* Connection slots. Without Recycle each attempt creates at most one
\* connection; with Recycle closed, unreferenced slots are reused.
Conns == 1..(Cardinality(Reqs) * (MaxRetry + 1))
Pools == 1..2      \* the pool before reset and the one get_or_create makes after

Terminal == {"Ok", "Err"}

VARIABLES
    rpc,        \* request state
    rconn,      \* connection a request holds (0 = none)
    rpool,      \* pool a request acquired from (0 = none)
    rtries,     \* EOF retries used
    cst,        \* per connection: "none" | "open" | "closed"
    cpool,      \* per connection: owning pool
    cinuse,     \* per connection: AsioConnection::in_use
    cpre,       \* per connection: created before reset() took the lock
    readers,    \* shared holders of GlobalIOContext::operation_mutex_
    writer,     \* "none" | "waiting" | "held"
    resets,     \* GlobalIOContext::resets_in_progress_
    rs,         \* reset() progress
    reg,        \* pool in the AsioConnectionPool registry (0 = empty)
    pshut,      \* per pool: AsioConnectionPool::shutdown_
    staleOk     \* history: a request succeeded on a pre-reset connection after reset

vars == <<rpc, rconn, rpool, rtries, cst, cpool, cinuse, cpre,
          readers, writer, resets, rs, reg, pshut, staleOk>>
connVars == <<cst, cpool, cinuse, cpre>>
lockVars == <<readers, writer, resets>>

ReqStates == {"Idle", "LockWait", "Connecting", "Write", "Waiting"} \cup Terminal
ResetStates == {"Idle", "WaitLock", "Closing", "ShutdownPools", "Done"}

TypeOK ==
    /\ rpc \in [Reqs -> ReqStates]
    /\ rconn \in [Reqs -> Conns \cup {0}]
    /\ rpool \in [Reqs -> Pools \cup {0}]
    /\ rtries \in [Reqs -> 0..MaxRetry]
    /\ cst \in [Conns -> {"none", "open", "closed"}]
    /\ cpool \in [Conns -> Pools \cup {0}]
    /\ cinuse \in [Conns -> BOOLEAN]
    /\ cpre \in [Conns -> BOOLEAN]
    /\ readers \in Nat
    /\ writer \in {"none", "waiting", "held"}
    /\ resets \in {0, 1}
    /\ rs \in ResetStates
    /\ reg \in Pools \cup {0}
    /\ pshut \in [Pools -> BOOLEAN]
    /\ staleOk \in BOOLEAN

Init ==
    /\ rpc = [r \in Reqs |-> "Idle"]
    /\ rconn = [r \in Reqs |-> 0]
    /\ rpool = [r \in Reqs |-> 0]
    /\ rtries = [r \in Reqs |-> 0]
    /\ cst = [c \in Conns |-> "none"]
    /\ cpool = [c \in Conns |-> 0]
    /\ cinuse = [c \in Conns |-> FALSE]
    /\ cpre = [c \in Conns |-> FALSE]
    /\ readers = 0 /\ writer = "none" /\ resets = 0
    /\ rs = "Idle"
    /\ reg = 0
    /\ pshut = [p \in Pools |-> FALSE]
    /\ staleOk = FALSE

-----------------------------------------------------------------------------
(* IO threads.                                                              *)

\* Threads stuck in a blocking lock_shared().
Blocked == {r \in Reqs : rpc[r] = "LockWait"}
FreeIO == NIO - Cardinality(Blocked)
HasIO == FreeIO > 0

\* std::shared_mutex: can a reader enter now?
SharedGrantable == writer = "none" \/ (writer = "waiting" /\ ~WriterPref)

-----------------------------------------------------------------------------
(* Request path: AsioTransportAdapter::send_request_streaming (TR:458).    *)

\* Free connection slot for create_connection: never used, or closed and
\* held by no request (the weak_ptr expired).
FreeSlots == {c \in Conns : cst[c] = "none" \/
                 (cst[c] = "closed" /\ \A q \in Reqs : rconn[q] # c)}

\* The attempt loop (TR:469-487, TR:650-662) retries a failed acquire or an
\* EOF once (kMaxRetries = 1); otherwise the error goes to the caller.
Fail(r) ==
    IF rtries[r] < MaxRetry
       THEN /\ rpc' = [rpc EXCEPT ![r] = "Idle"]
            /\ rtries' = [rtries EXCEPT ![r] = @ + 1]
       ELSE /\ rpc' = [rpc EXCEPT ![r] = "Err"]
            /\ UNCHANGED rtries

\* After the shared guard is held: PendingCreateGuard (POOL:756-781).
AfterGuard(r, p) ==
    IF pshut[p]
       THEN Fail(r) /\ UNCHANGED readers                \* POOL:779-781
       ELSE /\ rpc' = [rpc EXCEPT ![r] = "Connecting"] \* POOL:818 co_await connect
            /\ readers' = readers + 1
            /\ UNCHANGED rtries

\* AsioConnectionPool::acquire (POOL:611-677) on an IO thread, through the
\* first co_await of create_connection (POOL:743-818).
Acquire(r) ==
    /\ rpc[r] = "Idle" /\ HasIO
    /\ LET p == IF reg = 0 THEN (IF pshut[1] THEN 2 ELSE 1) ELSE reg \* get_or_create, POOL:462-494
           idle == {c \in Conns : cst[c] = "open" /\ cpool[c] = p /\ ~cinuse[c]}
       IN
       /\ reg' = p
       /\ rpool' = [rpool EXCEPT ![r] = p]
       /\ IF pshut[p] THEN                                      \* POOL:615-617
             Fail(r) /\ UNCHANGED <<rconn, cinuse, readers>>
          ELSE IF idle # {} THEN                                \* reuse, POOL:633-655
             \E c \in idle :
                /\ rconn' = [rconn EXCEPT ![r] = c]
                /\ cinuse' = [cinuse EXCEPT ![c] = TRUE]
                /\ rpc' = [rpc EXCEPT ![r] = "Write"]
                /\ UNCHANGED <<rtries, readers>>
          ELSE IF FailFast THEN                                 \* POOL:747-750
             IF SharedGrantable /\ resets = 0 THEN
                AfterGuard(r, p) /\ UNCHANGED <<rconn, cinuse>>
             ELSE IF SharedGrantable /\ ~CheckFirst THEN
                \* 6d5c9510a: try_lock_shared succeeds, or fails spuriously
                \* and then sees the announcement.
                \/ AfterGuard(r, p) /\ UNCHANGED <<rconn, cinuse>>
                \/ Fail(r) /\ UNCHANGED <<rconn, cinuse, readers>>
             ELSE
                \* SystemShutdown "Client IO is being reset" (POOL:749).
                Fail(r) /\ UNCHANGED <<rconn, cinuse, readers>>
          ELSE IF SharedGrantable THEN                          \* old: GIO:155-157
             AfterGuard(r, p) /\ UNCHANGED <<rconn, cinuse>>
          ELSE
             \* Old: lock_shared() blocks this IO thread.
             /\ rpc' = [rpc EXCEPT ![r] = "LockWait"]
             /\ UNCHANGED <<rtries, rconn, cinuse, readers>>
    /\ UNCHANGED <<cst, cpool, cpre, writer, resets, rs, pshut, staleOk>>

\* Old: the blocked lock_shared() returns; the coroutine runs on the same
\* thread to its first co_await.
LockGranted(r) ==
    /\ rpc[r] = "LockWait" /\ SharedGrantable
    /\ AfterGuard(r, rpool[r])
    /\ UNCHANGED <<rconn, rpool, reg, pshut, staleOk, writer, resets, rs>>
    /\ UNCHANGED connVars

\* Connect completes (POOL:818, then POOL:860-935). The shared guard is
\* released when create_connection returns.
Connected(r) ==
    /\ rpc[r] = "Connecting" /\ HasIO
    /\ readers' = readers - 1
    /\ IF pshut[rpool[r]] THEN                                  \* POOL:860-871
          Fail(r) /\ UNCHANGED <<rconn, connVars>>
       ELSE \E c \in FreeSlots :                                \* POOL:896-922
          /\ cst' = [cst EXCEPT ![c] = "open"]
          /\ cpool' = [cpool EXCEPT ![c] = rpool[r]]
          /\ cinuse' = [cinuse EXCEPT ![c] = TRUE]
          /\ cpre' = [cpre EXCEPT ![c] = rs \in {"Idle", "WaitLock"}]
          /\ rconn' = [rconn EXCEPT ![r] = c]
          /\ rpc' = [rpc EXCEPT ![r] = "Write"]
          /\ UNCHANGED rtries
    /\ UNCHANGED <<rpool, writer, resets, rs, reg, pshut, staleOk>>

\* Connect fails: refused, socket missing after its backoff, or timeout.
ConnectFails(r) ==
    /\ rpc[r] = "Connecting" /\ HasIO
    /\ readers' = readers - 1
    /\ Fail(r)
    /\ UNCHANGED <<rconn, rpool, writer, resets, rs, reg, pshut, staleOk>>
    /\ UNCHANGED connVars

\* Register the handler on the strand, write the frame, clear in_use
\* (TR:561-602). A write on a closed socket retires the connection and
\* returns the error without retry (TR:588-598).
Write(r) ==
    /\ rpc[r] = "Write" /\ HasIO
    /\ LET c == rconn[r] IN
       /\ cinuse' = [cinuse EXCEPT ![c] = FALSE]
       /\ rpc' = [rpc EXCEPT ![r] = IF cst[c] = "open" THEN "Waiting" ELSE "Err"]
    /\ UNCHANGED <<rconn, rpool, rtries, cst, cpool, cpre>>
    /\ UNCHANGED <<readers, writer, resets, rs, reg, pshut, staleOk>>

\* Response arrives on an open connection.
Reply(r) ==
    /\ rpc[r] = "Waiting" /\ HasIO /\ cst[rconn[r]] = "open"
    /\ rpc' = [rpc EXCEPT ![r] = "Ok"]
    /\ staleOk' = (staleOk \/ (cpre[rconn[r]] /\ rs = "Done"))
    /\ UNCHANGED <<rconn, rpool, rtries, readers, writer, resets, rs, reg, pshut>>
    /\ UNCHANGED connVars

\* The socket closed under the request: the read loop fails the handler with
\* EOF, and the attempt loop retries or returns it (TR:628-666). The activity
\* timeout (TR:670-690) bounds the wait in any case.
ReplyEof(r) ==
    /\ rpc[r] = "Waiting" /\ HasIO /\ cst[rconn[r]] = "closed"
    /\ Fail(r)
    /\ rconn' = [rconn EXCEPT ![r] = 0]
    /\ UNCHANGED <<rpool, readers, writer, resets, rs, reg, pshut, staleOk>>
    /\ UNCHANGED connVars

\* With Recycle, a finished request starts over.
Restart(r) ==
    /\ Recycle /\ rpc[r] \in Terminal
    /\ rpc' = [rpc EXCEPT ![r] = "Idle"]
    /\ rconn' = [rconn EXCEPT ![r] = 0]
    /\ rtries' = [rtries EXCEPT ![r] = 0]
    /\ UNCHANGED <<rpool, readers, writer, resets, rs, reg, pshut, staleOk>>
    /\ UNCHANGED connVars

-----------------------------------------------------------------------------
(* GlobalIOContext::reset (GIO:125-153), on a non-IO thread.                *)

\* GIO:142 announce (new only), then GIO:147 unique_lock: a waiting writer.
ResetBegin ==
    /\ rs = "Idle"
    /\ resets' = IF FailFast THEN 1 ELSE 0
    /\ writer' = "waiting"
    /\ rs' = "WaitLock"
    /\ UNCHANGED <<rpc, rconn, rpool, rtries, readers, reg, pshut, staleOk>>
    /\ UNCHANGED connVars

ResetLocked ==
    /\ rs = "WaitLock" /\ readers = 0
    /\ writer' = "held"
    /\ rs' = "Closing"
    /\ UNCHANGED <<rpc, rconn, rpool, rtries, readers, resets, reg, pshut, staleOk>>
    /\ UNCHANGED connVars

\* GIO:151 ConnectionRegistry::closeAll (POOL:65-77) -> AsioConnection::close
\* (CONN:55-58): alive=false, then the close is dispatched to the strand and
\* the caller waits for it while the io_context runs (CONN:113-138). The
\* handler needs a free IO thread.
ResetCloseOne ==
    /\ rs = "Closing"
    /\ \E c \in Conns :
          /\ cst[c] = "open" /\ HasIO
          /\ cst' = [cst EXCEPT ![c] = "closed"]
    /\ UNCHANGED <<rpc, rconn, rpool, rtries, cpool, cinuse, cpre>>
    /\ UNCHANGED <<readers, writer, resets, rs, reg, pshut, staleOk>>

ResetClosed ==
    /\ rs = "Closing" /\ \A c \in Conns : cst[c] # "open"
    /\ rs' = "ShutdownPools"
    /\ UNCHANGED <<rpc, rconn, rpool, rtries, readers, writer, resets, reg, pshut, staleOk>>
    /\ UNCHANGED connVars

\* GIO:152 shutdown_all (POOL:509-530): shut the registered pool (POOL:687-741;
\* its waits are bounded by the timeout and its sockets are already reset),
\* then clear the registry. Release the lock and the announcement.
ResetShutdownPools ==
    /\ rs = "ShutdownPools"
    /\ pshut' = IF reg = 0 THEN pshut ELSE [pshut EXCEPT ![reg] = TRUE]
    /\ reg' = 0
    /\ writer' = "none" /\ resets' = 0
    /\ rs' = "Done"
    /\ UNCHANGED <<rpc, rconn, rpool, rtries, readers, staleOk>>
    /\ UNCHANGED connVars

-----------------------------------------------------------------------------

Finished ==
    /\ rs = "Done" /\ \A r \in Reqs : rpc[r] \in Terminal
    /\ ~Recycle
    /\ UNCHANGED vars

ReqNext(r) ==
    \/ Acquire(r) \/ LockGranted(r) \/ Connected(r) \/ ConnectFails(r)
    \/ Write(r) \/ Reply(r) \/ ReplyEof(r) \/ Restart(r)

\* Requests that make progress (excludes the optional Restart).
ReqProgress(r) ==
    \/ Acquire(r) \/ LockGranted(r) \/ Connected(r) \/ ConnectFails(r)
    \/ Write(r) \/ Reply(r) \/ ReplyEof(r)

ResetNext == ResetBegin \/ ResetLocked \/ ResetCloseOne \/ ResetClosed \/ ResetShutdownPools

Next == (\E r \in Reqs : ReqNext(r)) \/ ResetNext \/ Finished

\* Weak fairness for every request and for reset(); strong fairness for the
\* writer taking the lock, so a writer that is enabled infinitely often wins.
Spec == /\ Init /\ [][Next]_vars
        /\ \A r \in Reqs : WF_vars(ReqProgress(r))
        /\ WF_vars(ResetBegin \/ ResetCloseOne \/ ResetClosed \/ ResetShutdownPools)
        /\ SF_vars(ResetLocked)

-----------------------------------------------------------------------------
(* Properties.                                                              *)

\* No IO thread ever blocks on the reset lock.
NoIOThreadBlocked == Blocked = {}

\* Shared holders are exactly the in-flight creations.
LockAccounting == readers = Cardinality({r \in Reqs : rpc[r] = "Connecting"})

\* in_use is set only while its request is between acquire and write.
InUseOwned ==
    \A c \in Conns : cinuse[c] => \E r \in Reqs : rconn[r] = c /\ rpc[r] = "Write"

\* Once all requests finish, every connection is released or closed.
AllReleased ==
    (\A r \in Reqs : rpc[r] \in Terminal) => \A c \in Conns : ~cinuse[c]

\* After reset() returns, no connection made before it is open, and no
\* request succeeds on one.
NoStaleAfterReset ==
    /\ rs = "Done" => \A c \in Conns : cpre[c] => cst[c] # "open"
    /\ ~staleOk

\* No request takes a connection from a shut-down pool.
NoShutdownPoolUse ==
    \A r \in Reqs : rpc[r] \in {"Write", "Waiting"} /\ rconn[r] # 0 =>
        ~pshut[cpool[rconn[r]]] \/ cst[rconn[r]] = "closed"

\* Liveness: every request ends (no hang); reset() returns.
RequestsEnd == \A r \in Reqs : <>(rpc[r] \in Terminal)
RequestsEndRecycle == \A r \in Reqs : []<>(rpc[r] \in Terminal \/ rpc[r] = "Idle")
ResetReturns == <>(rs = "Done")
=============================================================================
