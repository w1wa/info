package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func timingInterceptor(
	ctx context.Context,
	method string,
	req interface{},
	reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	// 	start := time.Now()
	// 	err := invoker(ctx, method, req, reply, cc, opts...)
	// 	fmt.Printf(`--
	// 	call=%v
	// 	req=%#v
	// 	reply=%#v
	// 	time=%v
	// 	err=%v
	// `, method, req, reply, time.Since(start), err)
	return nil
}

// -----

// -----

func main() {
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
	 	grcpConnAdmin, err := grpc.NewClient(
			"127.0.0.1:8082",
			//grpc.WithUnaryInterceptor(timingInterceptor),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		adminCli := NewAdminClient(grcpConnAdmin)
		adminCtx := context.Background()
		// adminCtx, cancel := context.WithCancel(adminCtx)
		// defer cancel()
		ctxOut := metadata.NewOutgoingContext(adminCtx,metadata.Pairs("consumer", "logger1",))
		admLogging, err := adminCli.Logging(ctxOut, &Nothing{})
		
		if err != nil {
			log.Fatal(err)
		}
		for {
			msg, err := admLogging.Recv()
			fmt.Println(msg)
			
			if err == io.EOF {
				fmt.Println("Stream Closed")
				break
			} else if err != nil {
				fmt.Println(err)
				return
			}
		}
	}(wg)
	time.Sleep(10 * time.Millisecond)
	
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
	 	grcpConnAdmin, err := grpc.NewClient(
			"127.0.0.1:8082",
			//grpc.WithUnaryInterceptor(timingInterceptor),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		adminCli := NewAdminClient(grcpConnAdmin)
		adminCtx := context.Background()
		// adminCtx, cancel := context.WithCancel(adminCtx)
		// defer cancel()
		ctxOut := metadata.NewOutgoingContext(adminCtx,metadata.Pairs("consumer", "stat1",))
		admStat, err := adminCli.Statistics(ctxOut, &StatInterval{IntervalSeconds: 3})
		
		if err != nil {
			log.Fatal(err)
		}
		for {
			msg, err := admStat.Recv()
			fmt.Println("stat:",msg)
			
			if err == io.EOF {
				fmt.Println("stat Stream Closed")
				break
			} else if err != nil {
				fmt.Println("stat:",err)
				return
			}
		}
	}(wg)
	time.Sleep(10 * time.Millisecond)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
	 	grcpConnBiz, err := grpc.NewClient(
			"127.0.0.1:8082",
			//grpc.WithUnaryInterceptor(timingInterceptor),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		bizCli := NewBizClient(grcpConnBiz)
		bizCtx := context.Background()
		ctxOut := metadata.NewOutgoingContext(bizCtx,metadata.Pairs("consumer", "biz_admin",))
		_, err = bizCli.Test(ctxOut, &Nothing{})
		if err != nil {
			log.Fatal(err)
		}
	}(wg)
	time.Sleep(10 * time.Millisecond)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
	 	grcpConnBiz, err := grpc.NewClient(
			"127.0.0.1:8082",
			//grpc.WithUnaryInterceptor(timingInterceptor),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		bizCli := NewBizClient(grcpConnBiz)
		bizCtx := context.Background()
		ctxOut := metadata.NewOutgoingContext(bizCtx,metadata.Pairs("consumer", "biz_admin",))
		_, err = bizCli.Test(ctxOut, &Nothing{})
		if err != nil {
			log.Fatal(err)
		}
	}(wg)

	time.Sleep(10 * time.Millisecond)


		wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
	 	grcpConnBiz, err := grpc.NewClient(
			"127.0.0.1:8082",
			//grpc.WithUnaryInterceptor(timingInterceptor),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		bizCli := NewBizClient(grcpConnBiz)
		bizCtx := context.Background()
		ctxOut := metadata.NewOutgoingContext(bizCtx,metadata.Pairs("consumer", "biz_admin",))
		_, err = bizCli.Test(ctxOut, &Nothing{})
		if err != nil {
			log.Fatal(err)
		}
	}(wg)

	time.Sleep(10 * time.Millisecond)
	wg.Wait()
}



























	// wg := sync.WaitGroup{}
	
	// wg.Add(1)
	// go func(){
	// 	defer wg.Done()
	// 	grcpConnTest1, err := grpc.NewClient(
	// 	"127.0.0.1:8082",
	// 	//grpc.WithUnaryInterceptor(timingInterceptor),
	// 	grpc.WithTransportCredentials(insecure.NewCredentials()),
	// )
	// if err != nil {
	// 	fmt.Println("cant connect to grpc")
	// 	return
	// }
	// 	ctxBiz := context.Background()
	// 	mdBiz := metadata.Pairs(
	// 		"consumer", "biz_admin",
	// 	)
	// 	ctxBiz = metadata.NewOutgoingContext(ctxBiz, mdBiz)
	// 	bizCli := NewBizClient(grcpConnTest1)
	// 	fmt.Println("1")
	// 	_, err = bizCli.Test(ctxBiz, &Nothing{})
	// 	log.Println("error: ",err)
	// 	grcpConnTest1.Close()
	// }()



	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	grcpConnLogging, err := grpc.NewClient(
	// 		"127.0.0.1:8082",
	// 		//grpc.WithUnaryInterceptor(timingInterceptor),
	// 		grpc.WithTransportCredentials(insecure.NewCredentials()),
	// 	)
	// 	ctxAdm := context.Background()
	// 	mdAdm := metadata.Pairs(
	// 		"consumer", "logger1",
	// 	)
	// 	ctxAdm = metadata.NewOutgoingContext(ctxAdm, mdAdm)
	// 	admManager := NewAdminClient(grcpConnLogging)
	// 	alc, err := admManager.Logging(ctxAdm, &Nothing{})
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
		
	// 	for {
	// 		fmt.Println("LOOP")
	// 		e, err := alc.Recv()
			
	// 		if err != nil {
	// 			fmt.Println(err)
	// 		}
	// 		log.Println(e)
	// 		if err == io.EOF {
	// 			fmt.Println("Stream Closed")
	// 			break
	// 		}
	// 	}
	// 	grcpConnLogging.Close()
	// }()

	// time.Sleep(time.Duration(1) * time.Second)
	// fmt.Println("-------------")

	//wg.Add(1)
	//go func() {
	//	defer wg.Done()
	// 		grcpConnTest1, err := grpc.NewClient(
	// 	"127.0.0.1:8082",
	// 	//grpc.WithUnaryInterceptor(timingInterceptor),
	// 	grpc.WithTransportCredentials(insecure.NewCredentials()),
	// )
	// if err != nil {
	// 	fmt.Println("cant connect to grpc")
	// 	return
	// }
	// 	ctxBiz := context.Background()
	// 	mdBiz := metadata.Pairs(
	// 		"consumer", "biz_admin",
	// 	)
	// 	ctxBiz = metadata.NewOutgoingContext(ctxBiz, mdBiz)
	// 	bizCli := NewBizClient(grcpConnTest1)
	// 	fmt.Println("1")
	// 	_, err = bizCli.Test(ctxBiz, &Nothing{})
	// 	log.Println("error: ",err)
	// 	grcpConnTest1.Close()
	// //}()

	// wg.Add(1)
	// go func() {
		
	// 	defer wg.Done()

	// 		grcpConnTest2, err := grpc.NewClient(
	// 	"127.0.0.1:8082",
	// 	//grpc.WithUnaryInterceptor(timingInterceptor),
	// 	grpc.WithTransportCredentials(insecure.NewCredentials()),
	// )
	// if err != nil {
	// 	fmt.Println("cant connect to grpc")
	// 	return
	// }
	// 	ctxBiz1 := context.Background()
	// 	mdBiz1 := metadata.Pairs(
	// 		"consumer", "biz_user",
	// 	)
	// 	ctxBiz1 = metadata.NewOutgoingContext(ctxBiz1, mdBiz1)
	// 	bizCli1 := NewBizClient(grcpConnTest2)
	// 	fmt.Println("2")
	// 	_, err = bizCli1.Test(ctxBiz1, &Nothing{})
	// 	fmt.Println("error: ",err)
	// 	grcpConnTest2.Close()
		
	// }()
	

