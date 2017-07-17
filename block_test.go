package lsm

import (
	"bytes"
	"reflect"
	"testing"
)

func TestBlockBuilder(t *testing.T) {
	bb := blockBuilder{restartInterval: 10}
	bb.add([]byte("/hello/africa"), []byte("bar"))
	bb.add([]byte("/hello/milkyway"), []byte("foo"))
	bb.add([]byte("/hello/world"), []byte("baz"))
	raw := bb.finish()

	block, err := readBlock(raw)
	if err != nil {
		t.Fatal(err)
	}

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
	if len(block.restarts) != 0 {
		t.Errorf("len(block.restarts) = %v, want %v", len(block.restarts), 0)
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
	raw := bb.finish()

	block, err := readBlock(raw)
	if err != nil {
		t.Fatal(err)
	}

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
	if len(block.restarts) != 1 {
		t.Errorf("len(block.restarts) = %v, want %v", len(block.restarts), 1)
	}
}

func TestBlockIterAt(t *testing.T) {
	bb := blockBuilder{restartInterval: 4}
	bb.add([]byte("/hello/africa"), []byte("bar"))
	bb.add([]byte("/hello/america"), []byte("zap"))
	bb.add([]byte("/hello/canada"), []byte("bax"))
	bb.add([]byte("/hello/milkyway"), []byte("foo"))
	bb.add([]byte("/hello/world"), []byte("baz"))
	bb.add([]byte("/hello/zanzibar"), []byte("bop"))
	raw := bb.finish()

	block, err := readBlock(raw)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		lookup []byte
		want   []byte
	}{
		{lookup: []byte("/hello/"), want: []byte("/hello/africa")},
		{lookup: []byte("/hello/africa"), want: []byte("/hello/africa")},
		{lookup: []byte("/hello/america"), want: []byte("/hello/america")},
		{lookup: []byte("/hello/argentina"), want: []byte("/hello/canada")},
		{lookup: []byte("/hello/canada"), want: []byte("/hello/canada")},
		{lookup: []byte("/hello/colombia"), want: []byte("/hello/milkyway")},
		{lookup: []byte("/hello/germany"), want: []byte("/hello/milkyway")},
		{lookup: []byte("/hello/mexico"), want: []byte("/hello/milkyway")},
		{lookup: []byte("/hello/milkyway"), want: []byte("/hello/milkyway")},
		{lookup: []byte("/hello/world"), want: []byte("/hello/world")},
		{lookup: []byte("/hello/zanzibar"), want: []byte("/hello/zanzibar")},
	}

	for _, tc := range cases {
		t.Run(string(tc.lookup), func(t *testing.T) {
			iter, err := block.iterAt(tc.lookup)
			if err != nil {
				t.Errorf("block.iterAt(%q): %s", string(tc.lookup), err)
				return
			}
			if !bytes.Equal(iter.key, tc.want) {
				t.Errorf("block.iterAt(%q).key = %s, want %s", string(tc.lookup), string(iter.key), string(tc.want))
			}
		})
	}
}
