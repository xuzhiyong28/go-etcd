package etcd

import (
	"log"
	"testing"
)

func TestDemo1(t *testing.T) {
	DemoCURD()
}

func TestDemo2(t *testing.T) {
	DemoWatch()
}

func TestDemo3(t *testing.T) {
	DemoLease()
}

func TestDemo4(t *testing.T) {
	DemoTnx()
}

func TestDemo5(t *testing.T) {
	DemoLock()
}

func TestDemo6(t *testing.T) {
	DemoLockTest()
}

func TestDemo7(t *testing.T) {
	var endpoints = []string{"localhost:2379"}
	ser, err := NewServiceRegister(endpoints, "/web/node1", "localhost:8000", 5)
	if err != nil {
		log.Fatalln(err)
	}
	//监听续租相应chan
	go ser.ListenLeaseRespChan()
	select {}
}
