package ketama

import (
	"errors"
	"net"
	"reflect"
	"testing"
)

func TestNewKetamaNodeSelector(t *testing.T) {
	// given
	nodes := []string{"localhost:11211", "localhost:11212", "localhost:11213"}

	// when
	sel, err := NewKetamaNodeSelector(nodes...)

	// then
	expectSuccess(t, "Expected no error", err)
	expectEquals(t, "Expected 3 nodes", len(sel.nodes), 3)
	expectEquals(t, "Expected 1920 vNodes (3 * 160 * 4)", len(sel.vNodes), 1920)
	expectThat(t, "Expected vNodes to be sorted", vNodesSortedAsc(sel.vNodes))
}

func TestPickServerIsDeterministic(t *testing.T) {
	// given
	nodes := []string{"localhost:11211", "localhost:11212", "localhost:11213"}
	sel, err := NewKetamaNodeSelector(nodes...)
	keys := []string{"key1", "a-much-longer-key-than-previous", "some-id-for-your-app", "golang rocks", "kemtama works"}

	// when - picking servers for all keys twice
	servers := make([]net.Addr, len(keys))
	for i, key := range keys {
		servers[i], err = sel.PickServer(key)
		expectSuccess(t, "Expected success picking server", err)
	}

	servers2 := make([]net.Addr, len(keys))
	for i, key := range keys {
		servers2[i], err = sel.PickServer(key)
		expectSuccess(t, "Expected success picking server", err)
	}

	// then
	expectThat(t, "Expected servers to be the same for each key", sameServers(servers, servers2))
}

func TestPickServerBalancesBetweenServers(t *testing.T) {
	// given
	server1 := "127.0.0.1:11211"
	server2 := "127.0.0.1:11212"
	server3 := "127.0.0.1:11213"
	nodes := []string{server1, server2, server3}

	sel, err := NewKetamaNodeSelector(nodes...)

	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = "key-number-" + string(i)
	}

	// when picking servers for all keys
	servers := make([]net.Addr, len(keys))
	for i, key := range keys {
		servers[i], err = sel.PickServer(key)
		expectSuccess(t, "Expected success picking server", err)
	}

	// then
	keysForServer1 := 0
	keysForServer2 := 0
	keysForServer3 := 0

	for _, server := range servers {
		switch server.String() {
		case server1:
			keysForServer1++
			break
		case server2:
			keysForServer2++
			break
		case server3:
			keysForServer3++
			break
		default:
			panic(errors.New("Unexpected, this should never happen."))
		}
	}

	expectThat(t, "Expected total keys per server should be 1000.", keysForServer1 + keysForServer2 + keysForServer3 == 1000)
	expectThat(t, "Expected total keys for server 1 is more than 300", keysForServer1 > 300)
	expectThat(t, "Expected total keys for server 2 is more than 300", keysForServer2 > 300)
	expectThat(t, "Expected total keys for server 3 is more than 300", keysForServer3 > 300)
}


func TestRemoveServerImpactOnKeysLocation(t *testing.T) {
	// given a selector for three servers
	server1 := "127.0.0.1:11211"
	server2 := "127.0.0.1:11212"
	server3 := "127.0.0.1:11213"
	nodes := []string{server1, server2, server3}

	sel, err := NewKetamaNodeSelector(nodes...)

	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = "key-number-" + string(i)
	}

	// when picking servers for all keys, with 3 servers
	serversWith3 := make([]net.Addr, len(keys))
	for i, key := range keys {
		serversWith3[i], err = sel.PickServer(key)
		expectSuccess(t, "Expected success picking server", err)
	}

	// when server1 goes offline and picking servers for all keys, with 2 servers
	sel.SetNodes(server2, server3)
	serversWith2 := make([]net.Addr, len(keys))
	for i, key := range keys {
		serversWith2[i], err = sel.PickServer(key)
		expectSuccess(t, "Expected success picking server", err)
	}

	// then all keys that picked server 2 or 3 expected to maintain server after 1 was removed.
	for i := 0; i < 1000; i++ {
		serverWhen3 := serversWith3[i]
		serverWhen2 := serversWith2[i]

		if serverWhen3.String() != server1 {
			expectEquals(t, "When picked server 2 or 3, expected to not move when removed server 1", serverWhen3, serverWhen2)
		}
	}
}

func sameServers(servers []net.Addr, servers2 []net.Addr) bool {
	if len(servers) != len(servers2) {
		panic(errors.New("Test is broken, this should never happen."))
	}
	for i := 0; i < len(servers); i++ {
		if !reflect.DeepEqual(servers[i], servers2[i]) {
			return false
		}
	}
	return true
}

func vNodesSortedAsc(nodes []VNode) bool {
	for i, node := range nodes[:len(nodes)-2] {
		if node.point > nodes[i+1].point {
			return false
		}
	}
	return true
}

func expectEquals(t *testing.T, msg string, actual, expected interface{}) {
	if actual != expected {
		t.Error(msg, actual, "!=", expected)
	}
}

func expectSuccess(t *testing.T, s string, err error) {
	if err != nil {
		t.Error(s, err)
	}
}

func expectError(t *testing.T, s string, err error) {
	if err == nil {
		t.Error(s, err)
	}
}

func expectThat(t *testing.T, expectation string, expression bool) {
	if !expression {
		t.Error(expectation, "but expectation is", expression)
	}
}
