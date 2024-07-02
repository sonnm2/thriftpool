package thriftpool

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
)

// MapPool ...
type MapPool struct {
	Dial  ThriftDial
	Close ThriftClientClose

	lock *sync.Mutex

	idleTimeout uint32
	connTimeout uint32
	maxConn     uint32

	pools map[string]*ThriftPool
}

// NewMapPool ...
func NewMapPool(maxConn, connTimeout, idleTimeout uint32, dial ThriftDial, closeFunc ThriftClientClose) *MapPool {

	return &MapPool{
		Dial:        dial,
		Close:       closeFunc,
		maxConn:     maxConn,
		idleTimeout: idleTimeout,
		connTimeout: connTimeout,
		lock:        new(sync.Mutex),

		pools: make(map[string]*ThriftPool),
	}
}

func (mp *MapPool) getServerPool(hostport string) (*ThriftPool, error) {
	mp.lock.Lock()
	serverPool, ok := mp.pools[hostport]

	if !ok {
		mp.lock.Unlock()
		err := errors.New(fmt.Sprintf("Addr: %s thrift pool not exist!", hostport))
		return nil, err
	}
	mp.lock.Unlock()
	return serverPool, nil
}

// Get ...
func (mp *MapPool) Get(hostport string) *ThriftPool {
	serverPool, err := mp.getServerPool(hostport)
	if err != nil {
		serverPool = NewThriftPool(hostport, mp.maxConn, mp.connTimeout, mp.idleTimeout, mp.Dial, mp.Close)

		mp.lock.Lock()
		mp.pools[hostport] = serverPool

		mp.lock.Unlock()
	}
	return serverPool
}

// Release ...
func (mp *MapPool) Release(hostport string) error {
	serverPool, err := mp.getServerPool(hostport)
	if err != nil {
		return err
	}

	mp.lock.Lock()
	delete(mp.pools, hostport)
	mp.lock.Unlock()

	serverPool.Release()

	return nil
}

// ReleaseAll ...
func (mp *MapPool) ReleaseAll() {
	mp.lock.Lock()
	defer mp.lock.Unlock()

	for _, serverPool := range mp.pools {
		serverPool.Release()
	}
	mp.pools = make(map[string]*ThriftPool)
}
