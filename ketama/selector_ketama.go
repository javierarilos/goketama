package ketama

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"net"
	"sort"
	"strings"
)


// num of positions in the ring each node has:
// https://github.com/dustin/java-memcached-client/blob/c232307ad8e0c7ccc926e495dd7d5aad2d713318/src/main/java/net/spy/memcached/KetamaNodeLocator.java#L266
// https://github.com/dustin/java-memcached-client/blob/c232307ad8e0c7ccc926e495dd7d5aad2d713318/src/main/java/net/spy/memcached/util/DefaultKetamaNodeLocatorConfiguration.java#L36
const numRepsPerNode = 160 * 4

type KetamaNodeSelector struct {
	nodes        []net.Addr
	virtualNodes []VNode
}

type VNode struct {
	point uint32
	node net.Addr
}

func NewKetamaNodeSelector(newNodes ...string) (*KetamaNodeSelector, error) {

	totalKetamaPoints := len(newNodes) * numRepsPerNode

	selNodes := make([]net.Addr, len(newNodes))
	selVNodes := make([]VNode, totalKetamaPoints)

	// no weights supported at the moment. all nodes are equal.

	for i, node := range newNodes {
		nodeAddress, err := toAddress(node)
		if err != nil {
			return nil, err
		}

		selNodes[i] = nodeAddress
		for j := 0; j < numRepsPerNode; j++ {
			hash := hashForVNode(node, j)
			selVNodes[i*numRepsPerNode+j] = VNode{
				point: hash,
				node:  nodeAddress}
		}
	}

	sort.Slice(selVNodes, func(i, j int) bool {
		return selVNodes[i].point < selVNodes[j].point
	})

	k := &KetamaNodeSelector{
		nodes:        selNodes,
		virtualNodes: selVNodes,
	}

	return k, nil
}

func hashForVNode(server string, i int) uint32 {
	// TODO benchmark against faster hashes: murmur, xxhash, metrohash, siphash1-3
	serverIterationMd5 := md5.Sum([]byte(fmt.Sprintf("%s-%di", server, i)))
	return binary.LittleEndian.Uint32(serverIterationMd5[0 : 4])
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

func (nn *KetamaNodeSelector) PickServer(key string) (net.Addr, error) {

	fmt.Println("TODO: implement bradfitz/gomemcache PickServer")
	return nil, nil
}

func (nn *KetamaNodeSelector) Each(f func(net.Addr) error) error {
	fmt.Println("TODO: implement bradfitz/gomemcache Each")
	return nil
}
