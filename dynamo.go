package main

import "fmt"
import "crypto/md5"
import "strings"
import "strconv"
import "time"
import "math/rand"
import "encoding/binary"

type Request struct {
    requestType string
    returnChan chan map[string]string
    key int
    value map[string]string
    fields map[string]bool
    newRing *[100]*NodeMembership
    isReplica bool
    isRehash bool
    kill bool
}

type NodeMembership struct {
    membershipReceiver chan [100]*NodeMembership
    requestReceiver chan Request
    virtualAddresses map[int]bool
	hbCount int
	time time.Time
}

type NodeState struct {
    membership *NodeMembership
    ring [100]*NodeMembership
    store map[int]map[string]string
    failed bool
}

type FailureState struct {
    numReplicas int
    failure bool
    rehash bool
}

func main() {
    var ring [100]*NodeMembership
    addNode(&ring)
    addNode(&ring)
    addNode(&ring)
    addNode(&ring)
    addNode(&ring)
    printRing(&ring)
    fmt.Println()

    put("Alice", map[string]string{"accountType": "gold", "balance": "100", "name": "Alice"}, &ring)
    put("Bob", map[string]string{"accountType": "silver", "balance": "80", "name": "Bob"}, &ring)
    fmt.Println()

    get("Alice", &ring)
    get("Bob", &ring)
    fmt.Println()

    stream("Alice", map[string]bool{"balance": true}, &ring)
    stream("Bob", map[string]bool{"accountType": true, "balance": true, "name": true}, &ring)
    fmt.Println()

    put("Alice", map[string]string{"balance": "110"}, &ring)
    put("Alice", map[string]string{"balance": "121"}, &ring)
    put("Alice", map[string]string{"balance": "133.1"}, &ring)
    put("Alice", map[string]string{"accountType": "platinum"}, &ring)
    time.Sleep(time.Duration(2) * time.Second)
    fmt.Println()

    put("Bob", map[string]string{"balance": "72"}, &ring)
    put("Bob", map[string]string{"balance": "64.8"}, &ring)
    put("Bob", map[string]string{"balance": "58.32"}, &ring)
    put("Bob", map[string]string{"accountType": "bronze"}, &ring)
    time.Sleep(time.Duration(2) * time.Second)
    fmt.Println()

    addNode(&ring)
    printRing(&ring)
    fmt.Println()

    deleteNode(&ring, 40)
    printRing(&ring)
    time.Sleep(time.Duration(10) * time.Second)
    fmt.Println()

    get("Alice", &ring)
    get("Bob", &ring)
    fmt.Println()
}

// Stream updates to a key
func stream(key string, fields map[string]bool, ring *[100]*NodeMembership) {
    fmt.Println("Streaming these fields from " + key + ": ", fields)

    hashKey := hash(key)

    var request Request
    request.requestType = "stream"
    request.key = hashKey
    request.fields = fields

    pos := hashKey
    for {
        nodeMembership := ring[pos]
        if nodeMembership != nil {
            nodeMembership.requestReceiver <- request
            break
        } else {
            pos = (pos + 1) % len(ring)
        }
    }
}

// Return node address and value
func get(key string, ring *[100]*NodeMembership) (int, map[string]string) {
    hashKey := hash(key)
    numReplicas := 3

    var request Request
    request.requestType = "get"
    request.key = hashKey
    request.returnChan = make(chan map[string]string, numReplicas)

    blacklist := make(map[int]bool)
    pos := hashKey
    var value map[string]string
    for {
        nodeMembership := ring[pos]
        // Replicas on distinct nodes
        if _, contains := blacklist[pos]; !contains && nodeMembership != nil {
            nodeMembership.requestReceiver <- request
            numReplicas = numReplicas - 1
            for k, _ := range nodeMembership.virtualAddresses {
                blacklist[k] = true
            }
            value = <-request.returnChan
            fmt.Println("Got "+key+" from "+strconv.Itoa(pos)+": ", value)
        }
        pos = (pos + 1) % len(ring)

        if numReplicas == 0 {
            break
        }
    }
    return pos, value
}

func put(key string, value map[string]string, ring *[100]*NodeMembership) {
    hashKey := hash(key)
    fmt.Println("Putting: " + key + ": ", value)
    putHashed(hashKey, value, ring, false)
}

func putHashed(key int, value map[string]string, ring *[100]*NodeMembership, isRehash bool) {
    var request Request
    request.requestType = "put"
    request.key = key
    request.value = value
    request.isReplica = false
    request.isRehash = isRehash

    blacklist := make(map[int]bool)
    pos := key
    numReplicas := 3
    for {
        nodeMembership := ring[pos]
        // Only give replicas to distinct physical nodes
        if _, contains := blacklist[pos]; !contains && nodeMembership != nil {
            nodeMembership.requestReceiver <- request
            numReplicas = numReplicas - 1
            for k, _ := range nodeMembership.virtualAddresses {
                blacklist[k] = true
            }
            request.isReplica = true
        }
        pos = (pos + 1) % len(ring)

        if numReplicas == 0 {
            break
        }
    }
}

func addNode(ring *[100]*NodeMembership) {
    var newRing [100]*NodeMembership

    newNodeMembership := NodeMembership{make(chan [100]*NodeMembership, 100),
                                        make(chan Request, 100),
                                        make(map[int]bool),
                                        0,
                                        time.Now()}
    newNodeState := NodeState{&newNodeMembership,
                              newRing,
                              make(map[int]map[string]string),
                              false}
    numVirtualNodes := 4

    for {
        // Random choice until 4 free positions are found
        choice := rand.Intn(len(ring))
        if ring[choice] == nil {
            newNodeMembership.virtualAddresses[choice] = true
            newNodeState.ring[choice] = &newNodeMembership
            ring[choice] = &newNodeMembership
            numVirtualNodes = numVirtualNodes - 1
        }

        if numVirtualNodes == 0 {
            break
        }
    }

    for i, nodeMembership := range ring {
        newNodeState.ring[i] = nodeMembership
    }

    fmt.Println("Adding node: ", newNodeMembership.virtualAddresses)
    go startNode(&newNodeState)

    blacklist := make(map[int]bool)
    for k, _ := range newNodeMembership.virtualAddresses {
        blacklist[k] = true
    }
    // Rehash next numReplicas - 1 nodes
    numReplicas := 3
    for address, _ := range newNodeMembership.virtualAddresses {
        pos := (address + 1) % len(ring)
        left := numReplicas - 1
        for {
            if _, contains := blacklist[pos]; !contains && ring[pos] != nil {
                for k, _ := range ring[pos].virtualAddresses {
                    blacklist[k] = true
                }
                var request Request
                request.requestType = "rehash"
                request.newRing = ring
                ring[pos].requestReceiver <- request
                left -= 1
            }
            pos = (pos + 1) % len(ring)
            if left <= 0 || pos == address {
                break
            }
        }
    }
}

func deleteNode(ring *[100]*NodeMembership, choice int) {
//     choice := rand.Intn(len(ring))
    // Walk ring until first node found
    for {
        if ring[choice] != nil {
            break
        } else {
            choice = (choice + 1) % len(ring)
        }
    }

    // Simulate node failing
    var request Request
    request.requestType = "kill"
    ring[choice].requestReceiver <- request
    fmt.Println("Node failing: ", ring[choice].virtualAddresses)
    for virtualAddress, _ := range ring[choice].virtualAddresses {
        ring[virtualAddress] = nil
    }
}

func startNode(nodeState *NodeState) {
    manageMembership(nodeState)

    // Handle requests
    Loop:
        for {
            request := <-nodeState.membership.requestReceiver
            switch requestType := request.requestType; requestType {
                case "get":
                    request.returnChan <- nodeState.store[request.key]
                case "stream":
                    cur := nodeState.store[request.key]
                    newObj := make(map[string]string)
                    // If key exists, update fields present in request
                    if cur != nil {
                        for k, v := range cur {
                            newObj[k] = v
                        }
                        keys := make([]string, 0, len(request.fields))
                        for k := range request.fields {
                            keys = append(keys, k)
                        }
                        newObj["stream"] = strings.Join(keys, ",")
                        nodeState.store[request.key] = newObj
                    } else {
                    	fmt.Println("Key does not exist, not streaming.")
                    }
                case "put":
                    if len(request.value) == 0 {
                        delete(nodeState.store, request.key)
                    } else {
                        cur := nodeState.store[request.key]
                        newObj := make(map[string]string)
                        // If key exists, update fields present in request
                        if cur != nil {
                            for k, v := range cur {
                                newObj[k] = v
                            }
                            for k, v := range request.value {
                                newObj[k] = v
                            }
                            nodeState.store[request.key] = newObj
                        } else {
                            nodeState.store[request.key] = request.value
                        }

                        if v, contains := newObj["stream"]; contains && !request.isReplica && !request.isRehash {
                            streamFields := make(map[string]bool)
                            for _, field := range strings.Split(v, ",") {
                                streamFields[field] = true
                            }

                            streamUpdate := false
                            for k, _ := range request.value {
                                if _, streamUpdated := streamFields[k]; streamUpdated {
                                    streamUpdate = true
                                }
                            }

                            if streamUpdate {
                                stream := make(map[string]string)
                                for k, v := range newObj {
                                    if _, streamUpdated := streamFields[k]; streamUpdated {
                                        stream[k] = v
                                    }
                                }
                                // Real implementation would send stream update to message queue (i.e. SNS/SQS on AWS)
                                fmt.Println("Streamed "+ strconv.Itoa(request.key) + "(hashed key) update : ", stream)
                            }
                        }
                    }
                case "rehash":
                    rehash(nodeState, request.newRing)
                case "kill":
                    nodeState.failed = true
                    break Loop
            }
        }
}

func manageMembership(nodeState *NodeState) {
    increaseHbTime, _ := time.ParseDuration(".25s")
    gossipTime, _ := time.ParseDuration(".75s")

    // Asynchronously update heartbeat counter
    go func() {
        for {
            if nodeState.failed {
                break
            }
            time.Sleep(increaseHbTime)
            nodeState.membership.hbCount = nodeState.membership.hbCount + 1
            nodeState.membership.time = time.Now()
//             printNode(nodeState)
        }
    }()

    // Asynchronously gossip to neighbors
    go func() {
        for {
            if nodeState.failed {
                break
            }
            time.Sleep(gossipTime)

            pos := rand.Intn(len(nodeState.ring))
            gossipCount :=  0
            for i := 0; i < 100; i++ {
                neighborAddress := (i + pos) % 100
                neighborMembership := nodeState.ring[neighborAddress]
                if neighborMembership != nil {
                    if _, contains := nodeState.membership.virtualAddresses[neighborAddress]; !contains {
                        if len(neighborMembership.membershipReceiver) < cap(neighborMembership.membershipReceiver) {
                            neighborMembership.membershipReceiver <- nodeState.ring
                            gossipCount = gossipCount + 1
                        }
                    }
                    if gossipCount >= 2 {
                        break
                    }
                }
            }

            // Bootstrap node communication
            if gossipCount < 2 {
                for neighborAddress, neighborMembership := range nodeState.ring {
                    if neighborMembership != nil {
                        if _, contains := nodeState.membership.virtualAddresses[neighborAddress]; !contains {
                            if len(neighborMembership.membershipReceiver) < cap(neighborMembership.membershipReceiver) {
                                neighborMembership.membershipReceiver <- nodeState.ring
                                gossipCount = gossipCount + 1
                            }
                        }
                    }
                    if gossipCount >= 2 {
                        break
                    }
                }
            }
        }
    }()

    // Spend rest of time receiving messages
    go func() {
        for {
            if nodeState.failed {
                break
            }
            receivedRing := <-nodeState.membership.membershipReceiver
            nodeState.ring = mergeRings(nodeState, receivedRing)
        }
    }()
}

func mergeRings(nodeState *NodeState, receivedRing [100]*NodeMembership) [100]*NodeMembership{
    deleteAfter, _ := time.ParseDuration("7s")

    var newRing [100]*NodeMembership
    failureState := FailureState{3, false, false}
    for i, nodeMembership := range nodeState.ring {
        if nodeMembership != nil {
            checkFailureAndAdd(i, nodeMembership, deleteAfter, &newRing, nodeState, &failureState)
        }
    }

    failureState.numReplicas = 3
    failureState.failure = false
    for address, receivedNode := range receivedRing {
        if receivedNode != nil {
            if _, virtualAddressesContains := nodeState.membership.virtualAddresses[address]; !virtualAddressesContains {
                node := newRing[address]
                if node != nil {
                    newTime := node.time
                    if receivedNode.hbCount > node.hbCount {
                        newTime = receivedNode.time
                    }
                    newRing[address].hbCount = max(node.hbCount, receivedNode.hbCount)
                    newRing[address].time = newTime
                } else {
                    // Add new node
                    checkFailureAndAdd(address, receivedNode, deleteAfter, &newRing, nodeState, &failureState)
                }
            }
        }
    }

    if failureState.rehash {
        // Send request to rehash itself--added to end of request queue
        var request Request
        request.requestType = "rehash"
        request.newRing = &newRing
        nodeState.membership.requestReceiver <- request
    }

    return newRing
}

func rehash(nodeState *NodeState, newRing *[100]*NodeMembership) {
    // In real implementation, hashKeys would be sorted so rehash would only
    // be over hashKeys in failure region
    oldTable := make(map[int]map[string]string)
    for key, value := range nodeState.store {
        oldTable[key] = value
    }

    nodeState.store = make(map[int]map[string]string)
    for key, value := range oldTable {
        putHashed(key, value, newRing, true)
    }
}

func checkFailureAndAdd(i int, nodeMembership *NodeMembership, deleteAfter time.Duration, newRing *[100]*NodeMembership, nodeState *NodeState, failureState *FailureState) {
    // If owned virtual node is within replica range of failed
    if _, contains := nodeState.membership.virtualAddresses[i]; failureState.failure && contains {
        failureState.rehash = true
        failureState.failure = false
        failureState.numReplicas = 3
    }

    if time.Now().Sub(nodeMembership.time) < deleteAfter {
        newRing[i] = nodeMembership
    } else {
        // Node failed
        failureState.failure = true
    }

    if failureState.failure {
        failureState.numReplicas = failureState.numReplicas - 1
    }
    if failureState.numReplicas == 0 {
        failureState.failure = false
        failureState.numReplicas = 3
    }
}

func hash(key string) int {
    sum := md5.Sum([]byte(key))
	sumInt := binary.BigEndian.Uint64(sum[:])
	return int(sumInt % 100)
}

func max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

func maxTime(a, b time.Time) time.Time {
    if a.After(b) {
        return a
    }
    return b
}

func printRing(ring *[100]*NodeMembership) {
    output := ""
    for i, n := range ring {
        if n != nil {
            output = output + strconv.Itoa(i) + " "
        }
    }
    fmt.Println("Ring: ", output)
}

func printNode(nodeState *NodeState) {
    output := "=== Nodes"
    for k, _ := range nodeState.membership.virtualAddresses {
        output = output + " " + strconv.Itoa(k)
    }
    output = output + " " + " ==="
    for i, node := range nodeState.ring {
        if node != nil {
            output = output + "\n" + strconv.Itoa(i) + ": " + strconv.Itoa(node.hbCount) + ", " + strings.Split(node.time.String(), " ")[1] + "\n"
        }
    }

    fmt.Println(output)
}

