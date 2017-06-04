package lsm

import (
	"reflect"
	"testing"
)

func TestBlockBuilder(t *testing.T) {
	var bb blockBuilder
	bb.add([]byte("/hello/africa"), []byte("bar"))
	bb.add([]byte("/hello/milkyway"), []byte("foo"))
	bb.add([]byte("/hello/world"), []byte("baz"))
	block := bb.finish()

	m := map[string]string{}
	block.iter(func(k, v []byte) {
		m[string(k)] = string(v)
	})

	want := map[string]string{
		"/hello/africa":   "bar",
		"/hello/milkyway": "foo",
		"/hello/world":    "baz",
	}
	if !reflect.DeepEqual(m, want) {
		t.Errorf("got %#v, want %#v", m, want)
	}
}
