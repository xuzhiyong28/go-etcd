package etcd

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"log"
	"time"
)

// etcd服务注册与发现

type ServiceRegister struct {
	client        *clientv3.Client
	leaseID       clientv3.LeaseID
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	key           string //key
	val           string //value
}

//注册服务
func NewServiceRegister(endpoints []string, key, val string, ttl int64) (*ServiceRegister, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	ser := &ServiceRegister{
		client: client,
		key:    key,
		val:    val,
	}
	if err := ser.putKeyWithLease(ttl); err != nil {
		return nil, err
	}
	return ser, nil

}

//设置租约
func (s *ServiceRegister) putKeyWithLease(ttl int64) error {
	lease, err := s.client.Grant(context.Background(), ttl)
	if err != nil {
		return err
	}
	// 注册服务
	_, err = s.client.Put(context.Background(), s.key, s.val, clientv3.WithLease(lease.ID))
	// 设置续租
	leaseRespChan, err := s.client.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		return err
	}
	s.leaseID = lease.ID
	s.keepAliveChan = leaseRespChan
	return nil
}

//监听续租
func (s *ServiceRegister) ListenLeaseRespChan() {
	for leaseKeepResp := range s.keepAliveChan {
		log.Println("续约成功", leaseKeepResp)
	}
	log.Println("关闭续租")
}

//关闭服务
func (s *ServiceRegister) Close() error {
	if _, err := s.client.Revoke(context.Background(), s.leaseID); err != nil {
		return err
	}
	log.Println("撤销租约")
	return s.client.Close()
}
