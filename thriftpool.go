package thriftpool

import (
	"container/list"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/pkg/errors"
)

const INTERVALCHECKING = 60

var (
	nowFunc = time.Now

	ErrOverMax          = errors.New("Exeed the max connections")
	ErrInvalidConn      = errors.New("Invalid connection")
	ErrPoolClosed       = errors.New("Pool has been closed")
	ErrSocketDisconnect = errors.New("Socket is disconnected")

	infoLog, errLog *log.Logger
)

func init() {
	infoLog = log.New(os.Stdout, "POOL INFO: ", log.Lshortfile|log.Ldate|log.Ltime)
	errLog = log.New(os.Stdout, "POOL ERROR: ", log.Lshortfile|log.Ldate|log.Ltime)
}

type IdleClient struct {
	Socket *thrift.TSocket
	Client interface{}
}

type IdleConn struct {
	c *IdleClient
	t time.Time
}

type ThriftDial func(hostport string, connTimeOut time.Duration) (*IdleClient, error)
type ThriftClientClose func(c *IdleClient) error

type ThriftPool struct {
	Dial  ThriftDial
	Close ThriftClientClose

	lock        *sync.Mutex
	idle        list.List
	idleTimeout time.Duration
	connTimeout time.Duration
	maxConn     uint32
	count       uint32
	hostport    string
	closed      bool
}

func NewThriftPool(hostport string, maxConn, connTimeout, idleTimeout uint32, dial ThriftDial, closeFunc ThriftClientClose) *ThriftPool {
	thriftPool := &ThriftPool{
		Dial:        dial,
		Close:       closeFunc,
		hostport:    hostport,
		lock:        new(sync.Mutex),
		maxConn:     maxConn,
		idleTimeout: time.Second * time.Duration(idleTimeout),
		connTimeout: time.Second * time.Duration(connTimeout),
		closed:      false,
		count:       0,
	}

	go thriftPool.ClearConn()

	return thriftPool
}

func (p *ThriftPool) Get() (*IdleClient, error) {
	p.lock.Lock()

	if p.closed {
		p.lock.Unlock()
		return nil, ErrPoolClosed
	}

	if p.idle.Len() == 0 && p.count >= p.maxConn {
		p.lock.Unlock()
		return nil, ErrOverMax
	}

	p.lock.Unlock()
	p.CheckTimeout()
	p.lock.Lock()

	if p.idle.Len() == 0 {
		infoLog.Println("Make new connection: ", p.hostport)
		dial := p.Dial
		p.count += 1
		p.lock.Unlock()

		client, err := dial(p.hostport, p.connTimeout)

		if err != nil {
			errLog.Println(err.Error())

			p.lock.Lock()
			p.count -= 1
			p.lock.Unlock()
			return nil, err
		}

		if !client.Check() {
			return nil, ErrSocketDisconnect
		}

		return client, nil
	} else {
		infoLog.Println("Get a connection from pool, number of idle connections = ", p.idle.Len())
		ele := p.idle.Front()
		idlec := ele.Value.(*IdleConn)
		p.idle.Remove(ele)
		p.lock.Unlock()

		if !idlec.c.Check() {
			return nil, ErrSocketDisconnect
		}

		return idlec.c, nil
	}
}

func (p *ThriftPool) Put(client *IdleClient) error {
	if client == nil {
		return ErrInvalidConn
	}

	p.lock.Lock()

	if p.closed {
		err := p.Close(client)
		client = nil
		p.lock.Unlock()
		return err
	}

	if p.count > p.maxConn {
		p.count -= 1
		err := p.Close(client)
		p.lock.Unlock()
		client = nil
		return err
	}

	p.idle.PushBack(&IdleConn{
		c: client,
		t: nowFunc(),
	})
	p.lock.Unlock()
	return nil
}

func (p *ThriftPool) DecreaseCount() {
	p.count -= 1
}

func (p *ThriftPool) CheckTimeout() {
	p.lock.Lock()
	for p.idle.Len() != 0 {
		ele := p.idle.Back()
		if ele == nil {
			break
		}

		v := ele.Value.(*IdleConn)
		//infoLog.Println("v.t: ", v.t, " idletimeout: ", p.idleTimeout, " Timeout", v.t.Add(p.idleTimeout), " Now=", nowFunc(), " isTimeout:", v.t.Add(p.idleTimeout).After(nowFunc()))

		if v.t.Add(p.idleTimeout).After(nowFunc()) {
			break
		}

		// timeout & clear
		p.idle.Remove(ele)
		p.lock.Unlock()

		p.Close(v.c)
		p.lock.Lock()
		p.count -= 1

		//infoLog.Println("Removed a connection, number of idle connections = ", p.idle.Len(), " Count = ", p.count)
	}
	p.lock.Unlock()
	return
}

func (p *ThriftPool) GetIdleCount() uint32 {
	return uint32(p.idle.Len())
}

func (p *ThriftPool) GetConnCount() uint32 {
	return p.count
}

func (p *ThriftPool) ClearConn() {
	for {
		//infoLog.Println("Clear timeouted connections, number of idle connections: ", p.idle.Len(), " count = ", p.count)
		p.CheckTimeout()
		time.Sleep(INTERVALCHECKING * time.Second)
	}
}

func (p *ThriftPool) Release() {
	p.lock.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.count -= uint32((idle.Len()))
	p.lock.Unlock()

	for iter := idle.Front(); iter != nil; iter = iter.Next() {
		p.Close(iter.Value.(*IdleConn).c)
	}
}

func (p *ThriftPool) Recover() {
	p.lock.Lock()
	if p.closed == true {
		p.closed = false
	}
	p.lock.Unlock()
}

func (c *IdleClient) SetConnTimeout(connTimeout uint32) {
	c.Socket.SetTimeout(time.Duration(connTimeout) * time.Second)
}

func (c *IdleClient) LocalAddr() net.Addr {
	return c.Socket.Conn().LocalAddr()
}

func (c *IdleClient) RemoveAddr() net.Addr {
	return c.Socket.Conn().RemoteAddr()
}

func (c *IdleClient) Check() bool {
	if c.Socket == nil || c.Client == nil {
		return false
	}
	return c.Socket.IsOpen()
}
