package etcd_service_lagou

import (
	"context"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"
	"log"
	"sync"
	"time"
)

type ClientInfo struct {
	client     *clientv3.Client
	serverList map[string]string
	lock       sync.Mutex
}

func (cli *ClientInfo) GetService(prefix string) ([]string, error) {
	if addrs, err := cli.getServiceByName(prefix); err != nil {
		panic(err)
	} else {
		log.Println("get service ", prefix, " for instance list: ", addrs)
		// 启动一个协程监听服务的变化，当注册到etcd上的服务发生变化时可以感知
		go cli.watcher(prefix)
		return addrs, nil
	}

	return nil, nil
}

// 内部函数， watch服务的变化
func (cli *ClientInfo) watcher(prefix string) {
	rch := cli.client.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				// 如果prefix前缀的节点有新的服务注册，客户端监听到就加入到缓存中
				cli.SetServiceList(string(ev.Kv.Key), string(ev.Kv.Value))
			case mvccpb.DELETE:
				// 如果prefix前缀的节点有新的服务删除，客户端监听到就从缓存中删除
				cli.DelServiceList(string(ev.Kv.Key))
			}
		}
	}
}

// 根据服务名，获取服务实例信息
func (cli *ClientInfo) getServiceByName(prefix string) ([]string, error) {
	resp, err := cli.client.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	addrs := cli.extractAddrs(resp)
	return addrs, nil
}

// 从clientv3.GetResponse获取到key - value 存储
func (cli *ClientInfo) extractAddrs(resp *clientv3.GetResponse) []string {
	addrs := make([]string, 0)
	if resp == nil || resp.Kvs == nil {
		return addrs
	}
	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			addrs = append(addrs, string(v))
			cli.SetServiceList(string(resp.Kvs[i].Key), string(resp.Kvs[i].Value))
		}
	}
	return addrs
}

// 获取到服务key-value 存储到 serverList
func (cli *ClientInfo) SetServiceList(key, val string) {
	cli.lock.Lock()
	defer cli.lock.Unlock()
	cli.serverList[key] = val
	log.Println("set data key :", key, "val:", val)
}

// 删除本地缓存的服务实例信息
func (cli *ClientInfo) DelServiceList(key string) {
	cli.lock.Lock()
	defer cli.lock.Unlock()
	delete(cli.serverList, key) //删除本地缓存

}

// 初始化 etcd 客户端连接
func NewClientInfo(addr []string) (*ClientInfo, error) {
	conf := clientv3.Config{
		Endpoints:   addr,
		DialTimeout: 5 * time.Second,
	}
	if client, err := clientv3.New(conf); err == nil {
		return &ClientInfo{
			client:     client,
			serverList: make(map[string]string),
		}, nil
	} else {
		return nil, err
	}
}
