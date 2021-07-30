package etcd

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)

var (
	conf = clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	}
)

type EtcdMutex struct {
	Ttl     int64           //租约时间
	Conf    clientv3.Config //etcd集群配置
	Key     string
	cancel  context.CancelFunc
	txn     clientv3.Txn
	lease   clientv3.Lease
	leaseID clientv3.LeaseID
}

func (e *EtcdMutex) init() error {
	var ctx context.Context
	client, err := clientv3.New(e.Conf)
	if err != nil {
		return err
	}
	e.txn = clientv3.NewKV(client).Txn(context.TODO())
	e.lease = clientv3.NewLease(client)
	leaseResp, err := e.lease.Grant(context.TODO(), e.Ttl)
	if err != nil {
		return err
	}
	ctx, e.cancel = context.WithCancel(context.TODO())
	e.leaseID = leaseResp.ID
	_, err = e.lease.KeepAlive(ctx, e.leaseID)
	return err
}

func (e *EtcdMutex) Lock() error {
	err := e.init()
	if err != nil {
		return err
	}
	e.txn.If(clientv3.Compare(clientv3.CreateRevision(e.Key), "=", 0)).
		Then(clientv3.OpPut(e.Key, "isLock", clientv3.WithLease(e.leaseID))).Else()
	txnResp, err := e.txn.Commit()
	if err != nil {
		return err
	}
	if !txnResp.Succeeded {
		return fmt.Errorf("抢锁失败")
	}
	return nil
}

func (e *EtcdMutex) UnLock() {
	// 租约自动过期，立刻过期
	// cancel取消续租，而revoke则是立即过期
	e.cancel()
	e.lease.Revoke(context.TODO(), e.leaseID)
	fmt.Println("释放了锁")
}

func DemoLockTest() {
	go func(conf clientv3.Config) {
		elock := &EtcdMutex{
			Conf: conf,
			Ttl:  10,
			Key:  "lockKey",
		}
		err := elock.Lock()
		if err != nil {
			fmt.Println("groutine1抢锁失败")
			return
		}
		defer elock.UnLock()
		fmt.Println("groutine1抢锁成功")
		time.Sleep(10 * time.Second)
	}(conf)

	time.Sleep(2 * time.Second)

	go func(conf clientv3.Config) {
		elock := &EtcdMutex{
			Conf: conf,
			Ttl:  10,
			Key:  "lockKey",
		}
		err := elock.Lock()
		if err != nil {
			fmt.Println("groutine2抢锁失败")
			return
		}
		defer elock.UnLock()
		fmt.Println("groutine2抢锁成功")
		time.Sleep(10 * time.Second)
	}(conf)

	time.Sleep(10000 * time.Second)
}
