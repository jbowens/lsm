package lsm

import (
	"reflect"
	"testing"
)

func TestBlockBuilder(t *testing.T) {
	bb := blockBuilder{restartInterval: 10}
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

func TestBlockBuilderWithRestart(t *testing.T) {
	bb := blockBuilder{restartInterval: 4}
	bb.add([]byte("/hello/africa"), []byte("bar"))
	bb.add([]byte("/hello/america"), []byte("zap"))
	bb.add([]byte("/hello/canada"), []byte("bax"))
	bb.add([]byte("/hello/milkyway"), []byte("foo"))
	bb.add([]byte("/hello/world"), []byte("baz"))
	bb.add([]byte("/hello/zanzibar"), []byte("bop"))
	block := bb.finish()

	m := map[string]string{}
	block.iter(func(k, v []byte) {
		m[string(k)] = string(v)
	})

	want := map[string]string{
		"/hello/africa":   "bar",
		"/hello/america":  "zap",
		"/hello/canada":   "bax",
		"/hello/milkyway": "foo",
		"/hello/world":    "baz",
		"/hello/zanzibar": "bop",
	}
	if !reflect.DeepEqual(m, want) {
		t.Errorf("got %#v, want %#v", m, want)
	}
}
