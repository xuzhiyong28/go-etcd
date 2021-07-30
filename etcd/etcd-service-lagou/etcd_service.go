package etcd_service_lagou

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"log"
	"time"
)

type ServiceReg struct {
	client        *clientv3.Client
	lease         clientv3.Lease
	leaseResp     *clientv3.LeaseGrantResponse //租约
	canclefunc    func()
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
	key           string //服务
}

// 服务端创建一个连接etcd的实例
func NewServiceReg(addr []string, ttl int64) (*ServiceReg, error) {
	conf := clientv3.Config{
		Endpoints:   addr,
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(conf)
	if err != nil {
		panic(err)
	}
	sr := &ServiceReg{
		client: client,
	}
	if err := sr.setLease(ttl); err != nil {
		return nil, err
	}
	go sr.ListenLeaseRespChan()
	return sr, nil
}

// 设置服务的租约 - 给服务一个过期时间，然后后面会监听租约情况
func (sr *ServiceReg) setLease(ttl int64) error {
	// 创建一个租约实例
	lease := clientv3.NewLease(sr.client)
	//设置租约时间
	leaseResp, err := lease.Grant(context.TODO(), ttl)
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	// 创建一个续租管道，后面通过这个channel来监听续租的情况， 可以通过cancelFunc来关闭续约
	keepAliveChan, err := lease.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		return err
	}
	sr.lease = lease
	sr.leaseResp = leaseResp
	sr.canclefunc = cancelFunc
	sr.keepAliveChan = keepAliveChan
	return nil
}

// 监听续租情况,这个方法就是用来监听服务是否还在etcd注册中心里
func (sr *ServiceReg) ListenLeaseRespChan() {
	for {
		select {
		case leaseKeepResp := <-sr.keepAliveChan:
			if leaseKeepResp == nil {
				log.Println("已经关闭续租功能")
			} else {
				log.Println("续租成功")
			}
		}
	}
}

// 服务注册到etcd中
func (sr *ServiceReg) PutService(key, val string) error {
	kv := clientv3.NewKV(sr.client)
	log.Printf("register user server for %s\n", val)
	// 服务注册
	_, err := kv.Put(context.TODO(), key, val, clientv3.WithLease(sr.leaseResp.ID))
	return err
}

func (sr *ServiceReg) DelService(key, val string) error {
	kv := clientv3.NewKV(sr.client)
	log.Printf("del server for %s\n", val)
	_, err := kv.Delete(context.TODO(), key)
	return err
}

// 撤销租约，如果撤销租约，那么同一个租约的key都会被删除
func (sr *ServiceReg) RevokeLease() error {
	sr.canclefunc()
	time.Sleep(2 * time.Second)
	_, err := sr.lease.Revoke(context.TODO(), sr.leaseResp.ID)
	return err
}
