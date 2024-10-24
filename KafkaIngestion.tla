--------------------------- MODULE KafkaIngestion ---------------------------

EXTENDS Naturals, Sequences, FiniteSets, TLC

VARIABLES msg_id, topic, state, state_history, compaction_upper, read_todo, read_data,
          read_consistent, todo_next, todo_until, batch, topic_upper, eof_read, todo_time

CONSTANTS Keys, Readers, MaxMessages, null

ReaderState == <<read_todo, read_data, read_consistent, topic_upper, todo_next, todo_until, todo_time, batch, eof_read>>
KafkaState == <<msg_id, topic, state_history, state, compaction_upper>>

MessageType == [key: Keys, value: Nat]
MessageTypeOrNull == MessageType \union { null }

Upper(t) == Len(t) + 1
Last(s) == s[Len(s)]

\* Initial values of all variables
Init ==
    \* Writer state
    /\ msg_id = 1
    /\ topic = <<>>
    /\ state = [k \in Keys |-> 0]
    /\ state_history = <<>>
    /\ compaction_upper = 1
    \* Global reader state
    /\ read_todo = <<1>>
    /\ read_data = <<{}>>
    /\ read_consistent = <<>>
    \* Per-reader state
    /\ topic_upper = [self \in Readers |-> 1]
    /\ todo_time = [self \in Readers |-> 0]
    /\ todo_next = [self \in Readers |-> 0]
    /\ todo_until = [self \in Readers |-> 0]
    /\ eof_read = [self \in Readers |-> FALSE]
    /\ batch = [self \in Readers |-> {}]

\* Some writer writes message msg in the compacted_topic
PublishMessage ==
    /\ Len(topic) < MaxMessages
    /\ \E key \in Keys:
        /\ state' = [state EXCEPT ![key] = msg_id]
        /\ topic' = Append(topic, [key |-> key, value |-> msg_id])
        /\ state_history' = Append(state_history, state')
        /\ msg_id' = msg_id + 1
    /\ UNCHANGED <<ReaderState, compaction_upper>>

IsLatest(i) ==
    \/ topic[i] = null 
    \/ \A j \in (i+1)..Len(topic) : topic[j] # null => topic[i].key # topic[j].key

\* Compact the topic if possible. The offset to which compaction happens is chosen arbitrarily
CompactUntil(i) ==
    /\ topic' = [j \in 1..Len(topic) |-> IF j >= i \/ IsLatest(j) THEN topic[j] ELSE null]
    /\ compaction_upper' = i
    /\ UNCHANGED <<msg_id, state, state_history, ReaderState>>
    

\* A reader discovers that the topic has more data and attempts to mint a todo entry
DiscoverOffset(self) ==
    /\ topic_upper[self] < Len(topic)
    \* Chose an arbitrary point that we assume this reader observed
    /\ \E observed_upper \in topic_upper[self]..Upper(topic) :
        \* Append a todo if the observed upper is in advance of the todo 
        /\ read_todo' = IF observed_upper > Last(read_todo) 
            THEN Append(read_todo, observed_upper)
            ELSE read_todo
        /\ topic_upper' = [topic_upper EXCEPT ![self] = Last(read_todo')]
    /\ UNCHANGED <<KafkaState, todo_time, todo_next, todo_until, batch, read_data, read_consistent, eof_read>>

\* A reader discovers there is a pending todo entry and starts working on it
DiscoverTodo(self) ==
    \* There is a todo available
    /\ Upper(read_data) < Upper(read_todo)
    \* And we're not already working on one
    /\ todo_time[self] = 0
    /\ todo_time' = [todo_time EXCEPT ![self] = Upper(read_data)]
    /\ todo_next' = [todo_next EXCEPT ![self] = read_todo[Len(read_data)]]
    /\ todo_until' = [todo_until EXCEPT ![self] = read_todo[Upper(read_data)]]
    /\ UNCHANGED <<KafkaState, batch, read_data, read_todo, read_consistent, topic_upper, eof_read>>

\* A reader reads on more message from its current todo
ReadMessage(self) ==
    \* We are working on a todo at time t
    /\ todo_time[self] # 0
    \* There are messages to be read
    /\ (todo_next[self] # 0 /\ todo_next[self] < todo_until[self])
    /\ LET msg == {<<todo_next[self], topic[todo_next[self]]>>} \ {<<todo_next[self], null>>}
       IN batch' = [batch EXCEPT ![self] = batch[self] \union msg]
    /\ eof_read' = [eof_read EXCEPT ![self] = todo_next[self] = Len(topic)]
    /\ todo_next' = [todo_next EXCEPT ![self] = todo_next[self] + 1]
    /\ UNCHANGED <<KafkaState, read_consistent, topic_upper, todo_time, todo_until, read_todo, read_data>>

\* A reader commits the batch it read to persist
CommitBatch(self) ==
    \* We are working on a todo at time t
    /\ todo_time[self] # 0
    \* And we have done all required reads
    /\ todo_next[self] = todo_until[self]
    \* And the the upper of the shard is exactly at t, so we can commit
    /\ todo_time[self] = Upper(read_data)
    /\ read_data' = Append(read_data, batch[self])
    /\ batch' = [batch EXCEPT ![self] = {}]
    /\ todo_next' = [todo_next EXCEPT ![self] = 0]
    /\ UNCHANGED <<KafkaState, read_consistent, topic_upper, todo_time, read_todo, eof_read, todo_until>>

\* A reader aborts its batch because someone else was first
AbortBatch(self) ==
    \* We are working on a todo for time t
    /\ todo_time[self] # 0
    \* We have completed all the required reads
    /\ todo_next[self] # 0
    /\ todo_next[self] = todo_until[self]
    \* But time t has already been committed
    /\ todo_time[self] < Upper(read_data)
    /\ batch' = [batch EXCEPT ![self] = {}]
    /\ todo_time' = [todo_time EXCEPT ![self] = 0]
    /\ todo_next' = [todo_next EXCEPT ![self] = 0]
    /\ todo_until' = [todo_until EXCEPT ![self] = 0]
    /\ eof_read' = [eof_read EXCEPT ![self] = FALSE]
    /\ UNCHANGED <<KafkaState, read_consistent, topic_upper, read_data, read_todo>>

\* A reader knows it performed a consistent read and tries to report it
CommitConsistentRead(self) ==
    \* We are working on a todo for time t
    /\ todo_time[self] # 0
    \* We have already committed the data (TODO: maybe explicitly track this)
    /\ todo_next[self] = 0
    \* We reached EOF while reading this range
    /\ eof_read[self] = TRUE
    \* No greater time than t has been reported as consistent
    /\ (Len(read_consistent) > 0 => Last(read_consistent)[1] < todo_time[self])
    \* Record the pair (round, offset) that is consistent
    /\ read_consistent' = Append(read_consistent, <<todo_time[self], todo_until[self]>>)
    /\ todo_time' = [todo_time EXCEPT ![self] = 0]
    /\ todo_until' = [todo_until EXCEPT ![self] = 0]
    /\ eof_read' = [eof_read EXCEPT ![self] = FALSE]
    /\ UNCHANGED <<KafkaState, topic_upper, todo_next, batch, read_data, read_todo>>
    
\* A reader fails to report its consistent read (maybe due to crash)
AbortConsistentRead(self) ==
    /\ todo_time[self] # 0
    /\ todo_next[self] = 0
    /\ todo_time' = [todo_time EXCEPT ![self] = 0]
    /\ todo_until' = [todo_until EXCEPT ![self] = 0]
    /\ eof_read' = [eof_read EXCEPT ![self] = FALSE]
    /\ UNCHANGED <<KafkaState, read_consistent, batch, topic_upper, read_data, read_todo, todo_next>>
   
Next ==
    \/ PublishMessage
    \/ \E i \in (compaction_upper + 1)..Len(topic) : CompactUntil(i)
    \/ \E reader \in Readers : DiscoverOffset(reader)
    \/ \E reader \in Readers : DiscoverTodo(reader)
    \/ \E reader \in Readers : ReadMessage(reader)
    \/ \E reader \in Readers : CommitBatch(reader)
    \/ \E reader \in Readers : AbortBatch(reader)
    \/ \E reader \in Readers : CommitConsistentRead(reader)
    \/ \E reader \in Readers : AbortConsistentRead(reader)

Spec ==
    Init /\ [][Next]_<<KafkaState, ReaderState>>

\* This invariant states that the derived state of the topic always
\* matches the current state, meaning that compaction has not deleted
\* information.
CompactionInvariant == 
    \A key \in Keys :
        \/ state[key] = 0
        \/ \E i \in 1..Len(topic) :
            /\ topic[i] # null
            /\ IsLatest(i)
            /\ topic[i].value = state[key]

\* This invariant states that the offsets increase from one round to the next
DiscoveryInvariant ==
    \A i \in 2..Len(read_todo) :
        read_todo[i] > read_todo[i-1]

DerivedState(shard, t) ==
    LET messages == (UNION { read_data[i] : i \in 1..t }) \union { <<0, [key |-> key, value |-> 0]>> : key \in Keys }
    IN [ key \in Keys |-> (CHOOSE msg \in messages : msg[2].key = key /\ \A msg2 \in messages : msg[2].key = msg2[2].key => msg2[1] <= msg[1])[2].value] 

IngestionInvariant == \A i \in 1..Len(read_consistent) : DerivedState(read_data, read_consistent[i][1]) = state_history[read_consistent[i][2]-1]

Invariant ==
    /\ CompactionInvariant
    /\ DiscoveryInvariant
    /\ IngestionInvariant
                       

=============================================================================
\* Modification History
\* Last modified Thu Oct 24 15:34:29 EEST 2024 by petrosagg
\* Created Tue Oct 22 22:09:19 EEST 2024 by petrosagg
