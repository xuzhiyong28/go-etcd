package etcd_service_lagou

import (
	"fmt"
	"log"
	"testing"
)

func TestService(t *testing.T) {
	sr, err := NewServiceReg([]string{"localhost:2379"}, 20)
	if err != nil {
		panic(err)
	}
	defer sr.RevokeLease()
	//服务注册
	if err = sr.PutService("/api/v1/user/", "user接口要注册的信息"); err != nil {
		log.Fatal("service reg is error")
	}
	if err = sr.PutService("/api/v1/class/", "class接口要注册的信息"); err != nil {
		log.Fatal("service reg is error")
	}

	select {} //阻塞
}

func TestClient(t *testing.T) {
	cli, _ := NewClientInfo([]string{"localhost:2379"})
	cli.GetService("/api/") //获取包含前缀为/api的服务
	fmt.Println(cli.serverList)
}
