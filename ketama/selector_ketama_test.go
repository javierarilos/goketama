package ketama

import "testing"

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
