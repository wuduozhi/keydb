package keydb

import (
	"fmt"
	"testing"
)

func TestMultiSegment(t *testing.T) {
	m1 := newMemorySegment()
	for i := 0; i < 50; i++ {
		m1.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}
	m2 := newMemorySegment()
	for i := 10; i < 20; i++ {
		m2.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i+1)))
	}

	ms := newMultiSegment([]segment{m1, m2})
	itr, err := ms.Lookup(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for {
		key,val, err := itr.Next()
		fmt.Printf("%v %v\n",string(key),string(val))
		if err != nil {
			break
		}
		count++
	}
	if count != 50 {
		t.Fatal("incorrect count", count)
	}

}

func TestMultiSegment2(t *testing.T) {
	m1 := newMemorySegment()
	for i := 0; i < 1; i++ {
		m1.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}
	m2 := newMemorySegment()
	for i := 1; i < 150000; i++ {
		m2.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}

	ms := newMultiSegment([]segment{m1, m2})
	itr, err := ms.Lookup(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for {
		_, _, err := itr.Next()
		if err != nil {
			break
		}
		count++
	}
	if count != 150000 {
		t.Fatal("incorrect count", count)
	}

}
