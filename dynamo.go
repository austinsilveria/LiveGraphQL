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
    returnChan chan string
    key int
    value string
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
    store map[int]string
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
    output := ""
    for i, n := range ring {
        if n != nil {
            output = output + strconv.Itoa(i) + " "
        }
    }
    fmt.Println("Ring: ", output)
    put("Maria", "100", &ring)
    put("John", "20", &ring)
    put("Anna", "40", &ring)
    put("Tim", "100", &ring)
    put("Alex", "10", &ring)
    get("Maria", &ring)
    get("John", &ring)
    get("Anna", &ring)
    get("Tim", &ring)
    get("Alex", &ring)
    deleteNode(&ring, 81)
    time.Sleep(time.Duration(10) * time.Second)
    get("Maria", &ring)
    get("John", &ring)
    get("Anna", &ring)
    get("Tim", &ring)
    get("Alex", &ring)
}

// Return node address and value
func get(key string, ring *[100]*NodeMembership) (int, string) {
    hashKey := hash(key)

    var request Request
    request.requestType = "get"
    request.key = hashKey
    request.returnChan = make(chan string, 1)

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

    value := <- request.returnChan
    close(request.returnChan)
    fmt.Println("Got " + key + " from " + strconv.Itoa(pos) + ": " + value)
    return pos, value
}

func put(key string, value string, ring *[100]*NodeMembership) {
    hashKey := hash(key)
    fmt.Println("Putting: " + key + ": " + value)
    putHashed(hashKey, value, ring)
}

func putHashed(key int, value string, ring *[100]*NodeMembership) {
    var request Request
    request.requestType = "put"
    request.key = key
    request.value = value

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
                              make(map[int]string),
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

    go startNode(&newNodeState)
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

    // Kill node
    var request Request
    request.requestType = "kill"
    ring[choice].requestReceiver <- request
    fmt.Println("Failing: ", ring[choice].virtualAddresses)
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
                case "put":
                    if request.value == "" {
                        delete(nodeState.store, request.key)
                    } else {
                        nodeState.store[request.key] = request.value
                    }
                    nodeState.store[request.key] = request.value
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
        // In real implementation, hashKeys would be sorted so rehash would only
        // be over hashKeys in failure region
        oldTable := make(map[int]string)
        for key, value := range nodeState.store {
            oldTable[key] = value
        }

        nodeState.store = make(map[int]string)
        for key, value := range oldTable {
            putHashed(key, value, &(newRing))
        }
    }

    return newRing
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

