package main

import (
	"fmt"
	"hash/crc32"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const DEFAULT_REPLICAS = 10

type Pair struct {
	Key   int    `json:"key"`
	Value string `json:"value"`
}

type AllPair struct {
	Pairs []Pair `json:"pairs"`
}

type HashRing []uint32

func (circle HashRing) Len() int {
	return len(circle)
}

func (circle HashRing) Less(i, j int) bool {
	return circle[i] < circle[j]
}

func (circle HashRing) Swap(i, j int) {
	circle[i], circle[j] = circle[j], circle[i]
}

type Node struct {
	Id     int
	Ip     string
	Weight int
}

//
func NewNode(id int, ip string, weight int) *Node {
	return &Node{
		Id:     id,
		Ip:     ip,
		Weight: weight,
	}
}

type Consistent struct {
	Nodes     map[uint32]Node
	numReps   int
	Resources map[int]bool
	ring      HashRing
	sync.RWMutex
}

func NewConsistent() *Consistent {
	return &Consistent{
		Nodes:     make(map[uint32]Node),
		numReps:   DEFAULT_REPLICAS,
		Resources: make(map[int]bool),
		ring:      HashRing{},
	}
}

func (consistent *Consistent) Add(node *Node) bool {
	consistent.Lock()
	defer consistent.Unlock()

	if _, ok := consistent.Resources[node.Id]; ok {
		return false
	}

	count := consistent.numReps * node.Weight
	for i := 0; i < count; i++ {
		str := consistent.joinStr(i, node)
		consistent.Nodes[consistent.hashStr(str)] = *(node)
	}
	consistent.Resources[node.Id] = true
	consistent.sortHashRing()
	return true
}

func (consistent *Consistent) sortHashRing() {
	consistent.ring = HashRing{}
	for k := range consistent.Nodes {
		consistent.ring = append(consistent.ring, k)
	}
	sort.Sort(consistent.ring)
}

func (consistent *Consistent) joinStr(i int, node *Node) string {
	return node.Ip + "*" + strconv.Itoa(node.Weight) +
		"-" + strconv.Itoa(i) +
		"-" + strconv.Itoa(node.Id)
}

func (consistent *Consistent) hashStr(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (consistent *Consistent) Get(key string) Node {
	consistent.RLock()
	defer consistent.RUnlock()

	hash := consistent.hashStr(key)
	i := consistent.search(hash)

	return consistent.Nodes[consistent.ring[i]]
}

func (consistent *Consistent) search(hash uint32) int {

	i := sort.Search(len(consistent.ring), func(i int) bool { return consistent.ring[i] >= hash })
	if i < len(consistent.ring) {
		if i == len(consistent.ring)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(consistent.ring) - 1
	}
}

func Put(pair Pair, whichServer string) bool {
	urlPath := "http://localhost:"
	urlPath += whichServer
	urlPath += "/keys/" + strconv.Itoa(pair.Key) + "/" + pair.Value
	fmt.Println(urlPath)
	client := &http.Client{}
	request, err := http.NewRequest("PUT", urlPath, nil)
	response, err := client.Do(request)

	if err != nil {
		log.Fatal(err)
		fmt.Println("Put key operation wrong ", err)
		panic(err)
	}
	if response != nil {
		return true
	} else {
		return false
	}
}

func main() {
	cHashRing := NewConsistent()
	cHashRing.Add(NewNode(0, "http://localhost:3000", 1))
	cHashRing.Add(NewNode(1, "http://localhost:3001", 1))
	cHashRing.Add(NewNode(2, "http://localhost:3002", 1))

	imap := make(map[Pair]string)
	var slice []Pair
	slice = append(slice, Pair{1, "a"})
	slice = append(slice, Pair{2, "b"})
	slice = append(slice, Pair{3, "c"})
	slice = append(slice, Pair{4, "d"})
	slice = append(slice, Pair{5, "e"})
	slice = append(slice, Pair{6, "f"})
	slice = append(slice, Pair{7, "g"})
	slice = append(slice, Pair{8, "h"})
	slice = append(slice, Pair{9, "i"})
	slice = append(slice, Pair{10, "j"})

	for i := 0; i < 10; i++ {
		k := cHashRing.Get(slice[i].Value)
		imap[slice[i]] = k.Ip
	}

	for k, v := range imap {
		if strings.Contains(v, "3000") {
			Put(k, "3000")
		} else if strings.Contains(v, "3001") {
			Put(k, "3001")
		} else if strings.Contains(v, "3002") {
			Put(k, "3002")
		}
	}
}
