### Running
go run dynamo.go

### Output
1. Prints active virtual nodes in the ring after adding 5 physical nodes
2. Prints key-vale puts
3. Prints getting keys before server failure
4. Fails server (map contains all virtual nodes of that physical node)
5. Prints getting keys after server failure

### Implementation
1. Gossip membership
2. Key-object (map) stores as in memory maps
    
    a. Storing maps under keys allows for specification of
    which fields to update (fields are sparse--can be empty
    for some objects)
3. Live query (key + set of fields to listen to updates on)
    
    a. Store stream requests as normal objects under hashed
    key + stream_char. Request contains fields to listen to
    and where to send updates (NSQ topic that multiple clients)
    can subscribe to
4. N = 3 Replication
5. Rehash regions on detected node failures