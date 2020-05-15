package ketama

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
)

// num of positions in the ring each node has:
// https://github.com/dustin/java-memcached-client/blob/c232307ad8e0c7ccc926e495dd7d5aad2d713318/src/main/java/net/spy/memcached/KetamaNodeLocator.java#L266
// https://github.com/dustin/java-memcached-client/blob/c232307ad8e0c7ccc926e495dd7d5aad2d713318/src/main/java/net/spy/memcached/util/DefaultKetamaNodeLocatorConfiguration.java#L36
const numRepsPerNode = 160 * 4

type KetamaNodeSelector struct {
	nodes  []net.Addr
	vNodes []VNode
	mu     sync.Mutex
}

type VNode struct {
	point uint32
	node  net.Addr
}

func NewKetamaNodeSelector(newNodes ...string) (*KetamaNodeSelector, error) {

	nodeSel := &KetamaNodeSelector{}
	nodeSel.SetNodes(newNodes...)
	return nodeSel, nil
}

func (nodeSel *KetamaNodeSelector) SetNodes(newNodes ...string) error {

	totalKetamaPoints := len(newNodes) * numRepsPerNode

	selNodes := make([]net.Addr, len(newNodes))
	selVNodes := make([]VNode, totalKetamaPoints)

	// no weights supported at the moment. all nodes are equal.

	for i, node := range newNodes {
		nodeAddress, err := toAddress(node)
		if err != nil {
			return err
		}

		selNodes[i] = nodeAddress
		for j := 0; j < numRepsPerNode; j++ {
			hash := hashForVNode(nodeAddress, j)
			selVNodes[i*numRepsPerNode+j] = VNode{
				point: hash,
				node:  nodeAddress}
		}
	}

	sort.Slice(selVNodes, func(i, j int) bool {
		return selVNodes[i].point < selVNodes[j].point
	})

	nodeSel.mu.Lock()
	defer nodeSel.mu.Unlock()

	nodeSel.nodes = selNodes
	nodeSel.vNodes = selVNodes
	return nil
}

func hashForVNode(addr net.Addr, i int) uint32 {
	// TODO benchmark against faster hashes: murmur, xxhash, metrohash, siphash1-3
	serverIterationMd5 := md5.Sum([]byte(fmt.Sprintf("%s-%di", addr, i)))
	return binary.LittleEndian.Uint32(serverIterationMd5[0:4])
}

func toAddress(server string) (net.Addr, error) {
	if strings.Contains(server, "/") {
		addr, err := net.ResolveUnixAddr("unix", server)
		if err != nil {
			return nil, err
		}
		return addr, nil
	} else {
		tcpaddr, err := net.ResolveTCPAddr("tcp", server)
		if err != nil {
			return nil, err
		}
		return tcpaddr, nil
	}
}

func (nodeSel *KetamaNodeSelector) PickServer(key string) (net.Addr, error) {

	fmt.Println("TODO: implement bradfitz/gomemcache PickServer")
	return nil, nil
}

func (nodeSel *KetamaNodeSelector) Each(f func(net.Addr) error) error {
	fmt.Println("TODO: implement bradfitz/gomemcache Each")
	return nil
}
