package tcplisten

import (
	"container/heap"
	"fmt"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	"net"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

// Output plugin that sends messages via Tcp using the Heka protocol.
type TcpListenOutput struct {
	processMessageCount int64
	dropMessageCount    int64
	keepAliveDuration   time.Duration
	conf                *TcpListenOutputConfig
	address             string
	localAddress        net.Addr
	name                string
	reportLock          sync.Mutex
	or                  OutputRunner
	clients             *clients
}

// ConfigStruct for TcpListenOutput plugin.
type TcpListenOutputConfig struct {
	// String representation of the Tcp address to which this output should be
	// sending data.
	Address      string
	LocalAddress string `toml:"local_address"`
	// Allows for a default encoder.
	Encoder string
	// Set to true if Tcp Keep Alive should be used.
	KeepAlive bool `toml:"keep_alive"`
	// Integer indicating seconds between keep alives.
	KeepAlivePeriod int `toml:"keep_alive_period"`
	// Specifies whether or not Heka's stream framing wil be applied to the
	// output. We do some magic to default to true if ProtobufEncoder is used,
	// false otherwise.
	UseFraming *bool `toml:"use_framing"`
	// Specifies size of queue buffer for output. 0 means that buffer is
	// unlimited.
	QueueMaxBufferSize uint64 `toml:"queue_max_buffer_size"`
	// Specifies action which should be executed if queue is full. Possible
	// values are "shutdown", "drop", or "block".
	QueueFullAction string `toml:"queue_full_action"`
}

func (t *TcpListenOutput) ConfigStruct() interface{} {
	return &TcpListenOutputConfig{
		Address:            "localhost:9125",
		Encoder:            "ProtobufEncoder",
		QueueMaxBufferSize: 0,
		QueueFullAction:    "shutdown",
	}
}

func (t *TcpListenOutput) SetName(name string) {
	re := regexp.MustCompile("\\W")
	t.name = re.ReplaceAllString(name, "_")
}

func (t *TcpListenOutput) Init(config interface{}) (err error) {
	t.clients = new(clients)
	t.conf = config.(*TcpListenOutputConfig)
	t.address = t.conf.Address

	if t.conf.LocalAddress != "" {
		t.localAddress, err = net.ResolveTCPAddr("tcp", t.conf.LocalAddress)
	}

	if t.conf.KeepAlivePeriod != 0 {
		t.keepAliveDuration = time.Duration(t.conf.KeepAlivePeriod) * time.Second
	}

	return
}

func (t *TcpListenOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	var (
		ok     = true
		pack   *PipelinePack
		inChan = or.InChan()
		errsCh = make(chan error, 1)
	)

	if t.conf.UseFraming == nil {
		// Nothing was specified, we'll default to framing IFF ProtobufEncoder
		// is being used.
		if _, ok := or.Encoder().(*ProtobufEncoder); ok {
			or.SetUseFraming(true)
		}
	}
	t.or = or

	listener, err := net.Listen("tcp4", t.address)
	if err != nil {
		return fmt.Errorf("cannot listen at %s: %v", t.address, err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go t.accept(listener, errsCh, &wg)

	defer wg.Wait()
	defer t.clients.Close()
	defer listener.Close()

	for {
		select {
		case pack, ok = <-inChan:
			if !ok {
				return
			}
			outBytes, err := or.Encode(pack)
			if err != nil {
				or.LogError(fmt.Errorf("Error encoding message: %s", err.Error()))
				pack.Recycle()
				continue
			}
			if outBytes == nil {
				pack.Recycle()
				continue
			}
			t.clients.broadcast(outBytes)
			atomic.AddInt64(&t.processMessageCount, 1)
			pack.Recycle()
		case err = <-errsCh:
			or.LogError(err)
			return
		}
	}

	return
}

type temporary interface {
	Temporary() bool
}

func sendErr(err error, errsCh chan error) {
	select {
	case errsCh <- err:
	default:
		// fallthrough only possible if there are already
		// errors in flight in the errsCh
		// no need to add this one
	}
}

func (t *TcpListenOutput) accept(listener net.Listener, errsCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if t, ok := err.(temporary); ok && t.Temporary() {
				continue
			}
			listener.Close()
			sendErr(err, errsCh)
			return
		}

		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			t.or.LogError(fmt.Errorf("KeepAlive only supported for Tcp Connections."))
		} else {
			tcpConn.SetKeepAlive(t.conf.KeepAlive)
			if t.keepAliveDuration != 0 {
				tcpConn.SetKeepAlivePeriod(t.keepAliveDuration)
			}
		}
		t.clients.add(conn, t.or, wg)
	}
}

type idxConn struct {
	conn net.Conn
	msgs chan []byte
	idx  int
}

type clients struct {
	lk    sync.Mutex
	conns []*idxConn
}

func (cs *clients) Pop() interface{} {
	c := cs.conns[len(cs.conns)-1]
	cs.conns = cs.conns[:len(cs.conns)-1]
	c.idx = -1
	return c
}

func (cs *clients) Push(v interface{}) {
	c := v.(*idxConn)
	c.idx = len(cs.conns)
	cs.conns = append(cs.conns, v.(*idxConn))
}

func (cs *clients) Len() int {
	return len(cs.conns)
}

func (cs *clients) Less(i, j int) bool {
	return cs.conns[i].idx < cs.conns[j].idx
}

func (cs *clients) Swap(i, j int) {
	cs.conns[i], cs.conns[j] = cs.conns[j], cs.conns[i]
}

func (cs *clients) Close() error {
	cs.lk.Lock()
	defer cs.lk.Unlock()
	for _, c := range cs.conns {
		c.Close()
	}
	return nil
}

func (cs *clients) add(conn net.Conn, or OutputRunner, wg *sync.WaitGroup) {
	cs.lk.Lock()
	defer cs.lk.Unlock()
	// buffer up to 10 messages to the client before
	// closing the connection
	connCh := make(chan []byte, 10)
	c := &idxConn{conn, connCh, len(cs.conns)}
	cs.conns = append(cs.conns, c)

	wg.Add(1)
	go deliver_msgs(c, or, wg)
}

func deliver_msgs(c *idxConn, or OutputRunner, wg *sync.WaitGroup) {
	defer wg.Done()
	defer c.conn.Close()

	for buf := range c.msgs {
		// retry on temporary errors
		for {
			if _, err := c.conn.Write(buf); err != nil {
				if t, ok := err.(temporary); ok && t.Temporary() {
					continue
				}
				or.LogError(err)
				return
			}
			break
		}
	}
}

func (c *idxConn) Close() error {
	close(c.msgs)
	return nil
}

func (cs *clients) broadcast(buf []byte) {
	cs.lk.Lock()
	defer cs.lk.Unlock()

	var laggers []*idxConn
	for i := range cs.conns {
		c := cs.conns[i]
		select {
		case c.msgs <- buf:
		default:
			laggers = append(laggers, c)
		}
	}

	for _, lagger := range laggers {
		// this connection lagged too much
		lagger.Close()
		heap.Remove(cs, lagger.idx)
	}
}

func init() {
	RegisterPlugin("TcpListenOutput", func() interface{} {
		return new(TcpListenOutput)
	})
}

// Satisfies the `pipeline.ReportingPlugin` interface to provide plugin state
// information to the Heka report and dashboard.
func (t *TcpListenOutput) ReportMsg(msg *message.Message) error {
	t.reportLock.Lock()
	defer t.reportLock.Unlock()

	message.NewInt64Field(msg, "ProcessMessageCount",
		atomic.LoadInt64(&t.processMessageCount), "count")
	message.NewInt64Field(msg, "DropMessageCount",
		atomic.LoadInt64(&t.dropMessageCount), "count")

	return nil
}
