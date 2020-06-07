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
2. N = 3 Replication
3. Rehash regions on node adds and detected node failures
4. Key-object (map) stores as in memory maps
    
    a. Storing maps under keys allows for specification of
    which fields to update (fields are sparse--can be empty
    for some objects). `put()` interface updates specified fields
    
5. Live query (key + set of fields to listen to updates on)
    
    a. Store stream requests within object to be streamed from
    
        someHashKey {
            accountType: gold,
            balance: 100,
            name: Alice
        }
        
        stream(someHashKey, {balance})
        
        someHashKey {
            accountType: gold,
            balance: 100,
            name: Alice,
            stream: balance
        }
    On subsequent `put` operations for `someHashKey`, if `balance` is included in the `put`,
    its new value can be streamed to clients via a message queue by reading the stream value
    of the stored object.
    
    To support multiple streams for an object, multi-dimensional maps can be used (we leave
    this to a real implementation):
    
        someHashKey {
            accountType: gold,
            balance: 100,
            name: Alice
        }
        
        stream(someHashKey, {balance})
        stream(someHashKey, {accountType, balance, name})
        
        someHashKey {
            accountType: gold,
            balance: 100,
            name: Alice,
            stream: {
                hash(balance): balance
                hash(accountType,balance,name): accountType,balance,name
            }
        }
