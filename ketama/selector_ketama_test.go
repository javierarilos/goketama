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

	// when - picking servers for all keys
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
