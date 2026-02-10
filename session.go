package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type LogerAndStatsServer struct {
	lmu sync.RWMutex
	smu sync.RWMutex
	e Event
	s Stat
	lwg *sync.WaitGroup
	swg *sync.WaitGroup
}

func NewLogerAndStatsManager() *LogerAndStatsServer {
	return &LogerAndStatsServer{
		lmu: sync.RWMutex{},
		smu: sync.RWMutex{},
		e: Event{},
		s: Stat{
			ByMethod: make(map[string]uint64),
			ByConsumer: make(map[string]uint64),
		},
		lwg: &sync.WaitGroup{},
		swg: &sync.WaitGroup{},
	}
}

// func (m *LogerAndStatsManager) AddLogEvent (cons string, meth string, host string) {
// 	m.logEntry = append(m.logEntry, Event{Timestamp: 0,
// 	Consumer: cons,
// 	Method: meth,
// 	Host: host,})
// }

func (m *LogerAndStatsServer) Logging (n *Nothing, stream Admin_LoggingServer) error {
	log.Println("LOGGING SERVER")
	var err error
	err = nil
	m.lwg.Add(1)
	go func(*sync.WaitGroup) {
		defer m.lwg.Done()
		for {
			if m.lmu.TryLock() && m.e.Consumer != "" {
				err = stream.Send(&m.e)
				m.e = Event{}
				m.lmu.Unlock()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}(m.lwg)
	m.lwg.Wait()
	return err
}

func (m *LogerAndStatsServer) Statistics (t *StatInterval, stream Admin_StatisticsServer) error {
	log.Println("STATISTICS SERVER")
	var err error
	err = nil
	m.swg.Add(1)
	go func(*sync.WaitGroup) {
		defer m.swg.Done()
		for {
			time.Sleep(time.Duration(t.IntervalSeconds) * time.Second)
			m.smu.RLock()
			err = stream.Send(&m.s)
			m.smu.RUnlock()	
		}
	}(m.swg)
	m.swg.Wait()
	return err
}

func (m *LogerAndStatsServer) mustEmbedUnimplementedAdminServer() {}

type BizSrv struct {
}

func (b* BizSrv) Check (ctx context.Context, n* Nothing) (*Nothing, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	fmt.Println("CHECK", md)
	return &Nothing{Dummy: true}, nil
}

func (b* BizSrv) Add (ctx context.Context, n* Nothing) (*Nothing, error) {
	fmt.Println("ADD")
	return &Nothing{Dummy: true}, nil
}

func (b* BizSrv) Test (ctx context.Context, n* Nothing) (*Nothing, error) {
	//md, _ := metadata.FromIncomingContext(ctx)
	fmt.Println("TEST")
	return &Nothing{Dummy: true}, nil
}

func (b* BizSrv) mustEmbedUnimplementedBizServer() {}

func NewBusines() *BizSrv {
	return &BizSrv{}
}

type AdminCli struct {
}

func (ac *AdminCli) Logging (ctx context.Context, in *Nothing, opts ...grpc.CallOption) (Admin_LoggingClient, error) {
	log.Println("LOGGING CLIENT")
	return nil,nil
}


func (ac *AdminCli) Statistics(ctx context.Context, in *StatInterval, opts ...grpc.CallOption) (Admin_StatisticsClient, error) {
	log.Println("STATISTICS CLIENT")
	return nil,nil
}