package etcd

import (
	"context"
	"fmt"
	v3 "github.com/coreos/etcd/clientv3"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"log"
	"sync"
	"time"
)

// https://juejin.cn/post/6844903984440803341#heading-6
// https://www.cnblogs.com/jiujuan/p/10930664.html

func DemoCURD() {
	//建立连接
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("connect failed, err :", err)
		return
	}
	defer client.Close()

	// 增
	putResp, _ := client.Put(context.TODO(), "/demo/demo1_key", "demo1_value")
	fmt.Println("PUT /demo/demo1_key=demo1_value,response：", putResp)

	// 查
	getResp, _ := client.Get(context.TODO(), "/demo/demo1_key")
	for _, val := range getResp.Kvs {
		fmt.Printf("GET response：%s = %s\n", val.Key, val.Value)
		fmt.Printf("version : %d - %d - %d", val.Version, val.CreateRevision, val.ModRevision)
	}

	// 改 - 改跟增是一样的api
	updateResp, _ := client.Put(context.TODO(), "/demo/demo1_key", "update_value", clientv3.WithPrevKV())
	fmt.Println("PUT /demo/demo1_key=demo1_value,response：", updateResp)

	// 删
	delResp, _ := client.Delete(context.TODO(), "/demo/demo1_key")
	fmt.Println(delResp)
}

func DemoWatch() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("connect failed, err :", err)
		return
	}
	defer client.Close()

	client.Put(context.Background(), "/demo/demo2_key", "demo2_value")

	go func() {
		// watch
		// watchKey is a channel
		watchKey := client.Watch(context.Background(), "/demo/demo2_key")
		for resp := range watchKey {
			fmt.Println("==== watch===", resp)
		}
	}()
	time.Sleep(1 * time.Second)
	if resp, err := client.Put(context.Background(), "/demo/demo2_key", "demo2_watch"); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(resp)
	}
	time.Sleep(5 * time.Second)
}

func DemoLease() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("connect failed, err :", err)
		return
	}
	defer client.Close()
	client.Delete(context.TODO(), "/demo/demo1_key", clientv3.WithPrefix())
	//设置一个10秒的租约
	lease, err := client.Grant(context.TODO(), 10)
	if err != nil {
		log.Fatal(err)
	}
	client.Put(context.TODO(), "/demo/demo1_key", "demo1_value", clientv3.WithLease(lease.ID))

	kv, err := client.Get(context.TODO(), "/demo/demo1_key")
	if len(kv.Kvs) == 1 {
		fmt.Println("Found Key")
	}

	time.Sleep(11 * time.Second)

	kv2, err := client.Get(context.TODO(), "/demo/demo1_key")
	if len(kv2.Kvs) == 1 {
		fmt.Println("Found Key")
	} else {
		fmt.Println("no Found Key")
	}
}

func DemoTnx() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("connect failed, err :", err)
		return
	}
	defer client.Close()
	var wg sync.WaitGroup
	wg.Add(10)
	key := "key1"
	for i := 0; i < 10; i++ {
		go func(i int) {
			time.Sleep(5 * time.Second)
			_, err := client.Txn(context.TODO()).
				If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
				Then(clientv3.OpPut(key, fmt.Sprintf("%d", i))).Commit()
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	if resp, err := client.Get(context.TODO(), key); err != nil {
		log.Fatal(err)
	} else {
		log.Println(resp)
	}
}

//etcd 自带的分布式锁
func DemoLock() {
	client, err := v3.New(v3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("connect failed, err :", err)
		return
	}
	defer client.Close()
	lockKey := "/lock_key"
	var count int64 = 0 // no Thread Safe
	var threadCount = 2000
	var wg sync.WaitGroup
	wg.Add(threadCount)
	for i := 0; i < threadCount; i++ {
		go func(number int) {
			session, err2 := concurrency.NewSession(client)
			if err2 != nil {
				log.Fatal(err2)
				return
			}
			m := concurrency.NewMutex(session, lockKey)
			defer m.Unlock(context.TODO())
			if err := m.Lock(context.TODO()); err != nil {
				log.Fatal(err)
			} else {
				count++
				fmt.Println("=====线程", number, "执行完成========")
			}
			time.Sleep(2 * time.Second)
			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println("the count is = ", count)
}

//自己实现 etcd分布式锁
func DemoLockCustomize() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("connect failed, err :", err)
		return
	}
	//新建一个租约
	lease := clientv3.NewLease(client)
	leaseGrant, err := lease.Grant(context.TODO(), 5)
	if err != nil {
		panic(err)
	}
	leaseId := leaseGrant.ID
	// 创建一个可取消的租约，主要是为了退出的时候能够释放
	ctx, cancelFunc := context.WithCancel(context.TODO())
	// 释放租约
	defer cancelFunc()
	defer lease.Revoke(context.TODO(), leaseId)

	keepRespChan, err := lease.KeepAlive(ctx, leaseId) //续租
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			select {
			case keepResp := <-keepRespChan:
				if keepRespChan == nil {
					fmt.Println("租约已经失效了")
					goto END
				} else {
					// 每秒会续租一次, 所以就会收到一次应答
					fmt.Println("收到自动续租应答:", keepResp.ID)
				}
			}
		}
	END:
	}()
	//TODO 还没完

}
