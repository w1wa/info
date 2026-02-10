package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"runtime"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/tap"
)

const (
	// какой адрес-порт слушать серверу
	listenAddr string = "127.0.0.1:8082"

	// кого по каким методам пускать
	ACLData string = `{
	"logger1":          ["/main.Admin/Logging"],
	"logger2":          ["/main.Admin/Logging"],
	"stat1":            ["/main.Admin/Statistics"],
	"stat2":            ["/main.Admin/Statistics"],
	"biz_user":         ["/main.Biz/Check", "/main.Biz/Test"],
	"biz_admin":        ["/main.Biz/*"],
	"after_disconnect": ["/main.Biz/Add"]
}`
)

type serviceData struct {
	ACL map[string][]string
	Manager *LogerAndStatsServer
}

func StartMyMicroservice(ctx context.Context, addr string, acl string) error {
	log.Println("New Server")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer lis.Close()
	var sd serviceData
	sd.Manager = NewLogerAndStatsManager()
	err = json.Unmarshal([]byte(acl),&sd.ACL)
	if err != nil {
		return err
	}

	server := grpc.NewServer(
		//grpc.UnaryInterceptor(sd.checkACL),
		grpc.InTapHandle(sd.check),
	)

	RegisterAdminServer(server, sd.Manager)
	RegisterBizServer(server, NewBusines())
	err = server.Serve(lis)
	return err
}

func main() {
	go func () {
		for {
			fmt.Println("Num Goroutine: ", runtime.NumGoroutine())
			time.Sleep(time.Second)
		}
	}()
	ctx, finish := context.WithCancel(context.Background())
	err := StartMyMicroservice(ctx, listenAddr, ACLData)
	if err != nil {
		log.Println(err)
	}
	finish()
}

func (sd *serviceData) checkACL(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	fmt.Println("------------",ok)
	if ok {
		consumer := md["consumer"]
		methodFromACL,ok := sd.ACL[consumer[0]]
		fmt.Println("-----------------------",consumer, methodFromACL)
		if !ok {
			return nil,fmt.Errorf("Not Allowed")
		}
		methSplit := strings.Split(info.FullMethod,"/")
		flag := true
		for _,m := range methodFromACL {
			mSplit := strings.Split(m,"/")
			if mSplit[1] == methSplit[1] {
				if mSplit[2] == "*" || mSplit[2] == methSplit[2] {
					flag = false
				}
			}
		}
		if flag {
			return nil, fmt.Errorf("Not Allowed")
		}
	} else {
		return nil,fmt.Errorf("cannot parse meta")
	}
	reply, err := handler(ctx, req)	
	return reply, err
}

func (sd *serviceData) check(ctx context.Context, info *tap.Info) (context.Context, error) {
	
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		consumer := md["consumer"]
		methodFromACL,ok := sd.ACL[consumer[0]]
		if !ok {
			return nil,fmt.Errorf("Not Allowed")
		}
		methSplit := strings.Split(info.FullMethodName,"/")
		flag := true
		for _,m := range methodFromACL {
			mSplit := strings.Split(m,"/")
			if mSplit[1] == methSplit[1] {
				if mSplit[2] == "*" || mSplit[2] == methSplit[2] {
					flag = false
				}
			}
		}
		if flag {
			return nil, fmt.Errorf("Not Allowed")
		}
	} else {
		return nil,fmt.Errorf("cannot parse meta")
	}
	if ok {
		
		sd.Manager.lmu.Lock()
		sd.Manager.e.Consumer = md["consumer"][0]
		sd.Manager.e.Method = info.FullMethodName
		sd.Manager.lmu.Unlock()
		fmt.Println(sd.Manager.s.ByConsumer)

		sd.Manager.smu.Lock()
		sd.Manager.s.ByConsumer[md["consumer"][0]] += 1
		sd.Manager.s.ByMethod[info.FullMethodName] += 1
		sd.Manager.smu.Unlock()

		fmt.Println("after: ",sd.Manager.s.ByConsumer)
		//time.Sleep(time.Duration(10) * 10 * time.Millisecond)
	} else {
		return nil, fmt.Errorf("Not Allowed")
	}
	return ctx, nil
}