package liteclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/tonkeeper/tongo/tl"
)

type PriorityClient struct {
	timeout      time.Duration
	connections  []*Connection
	queries      map[queryID]chan []byte
	queriesMutex sync.Mutex
}

func NewPriorityClient(connections []*Connection, opts ...Options) *PriorityClient {
	pc := &PriorityClient{
		timeout:     defaultTimeout,
		connections: connections,
		queries:     make(map[queryID]chan []byte),
	}

	// Since opts take *Client, we can use a temporary Client to apply options,
	// then copy the timeout to our pc.
	tempClient := &Client{timeout: defaultTimeout}
	for _, f := range opts {
		f(tempClient)
	}
	pc.timeout = tempClient.timeout

	for _, conn := range pc.connections {
		go pc.reader(conn)
	}
	return pc
}

// IsOK returns true if there is at least one working connection.
func (c *PriorityClient) IsOK() bool {
	for _, conn := range c.connections {
		if conn.Status() == Connected {
			return true
		}
	}
	return false
}

// Request sends q as query and receives answer in top-down priority order with failover.
func (c *PriorityClient) Request(ctx context.Context, q []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var id queryID
	rand.Read(id[:])
	data := make([]byte, 4, 44+len(q)) //create with small overhead for reducing garbage collector calls
	binary.LittleEndian.PutUint32(data, magicADNLQuery)
	data = append(data, id[:]...)
	data = append(data, encodeLength(len(q))...)
	data = append(data, q...)
	data = alignBytes(data)
	p, err := NewPacket(data)
	if err != nil {
		return nil, newClientError("NewPacket() failed: %v", err)
	}

	resp := c.registerCallback(id)
	defer c.unregisterCallback(id)

	var lastErr error
	for _, conn := range c.connections {
		if conn.Status() != Connected {
			lastErr = fmt.Errorf("connection %s is not connected", conn.host)
			continue
		}

		err = conn.Send(p)
		if err != nil {
			lastErr = err
			continue
		}

		// Wait for response with a timeout of 5 seconds for each individual connection attempt
		attemptCtx, attemptCancel := context.WithTimeout(ctx, 5*time.Second)
		select {
		case <-attemptCtx.Done():
			attemptCancel()
			if ctx.Err() != nil {
				return nil, newClientError("request timeout: %v", ctx.Err())
			}
			lastErr = fmt.Errorf("connection %s request timeout", conn.host)
			continue
		case b := <-resp:
			attemptCancel()
			return b, nil
		}
	}

	if lastErr != nil {
		return nil, newClientError("all connections failed, last error: %v", lastErr)
	}
	return nil, newClientError("all connections failed")
}

func (c *PriorityClient) registerCallback(id queryID) chan []byte {
	resp := make(chan []byte, 1)
	c.queriesMutex.Lock()
	c.queries[id] = resp
	c.queriesMutex.Unlock()
	return resp
}

func (c *PriorityClient) unregisterCallback(id queryID) {
	c.queriesMutex.Lock()
	delete(c.queries, id)
	c.queriesMutex.Unlock()
}

func (c *PriorityClient) reader(conn *Connection) {
	for p := range conn.Responses() {
		if p.MagicType() != magicADNLAnswer {
			continue
		}
		err := c.processQueryAnswer(p)
		if err != nil {
			slog.Info("priority_client.reader() error", "err", err)
		}
	}
}

func (c *PriorityClient) processQueryAnswer(p Packet) error {
	if len(p.Payload) < 37 {
		return fmt.Errorf("too short payload")
	}
	var id queryID
	copy(id[:], p.Payload[4:36])
	c.queriesMutex.Lock()
	resp, prs := c.queries[id]
	delete(c.queries, id)
	c.queriesMutex.Unlock()
	if !prs {
		return fmt.Errorf("unknown query %x with id %x", p.Payload[:4], id)
	}
	length, data, err := decodeLength(p.Payload[36:])
	if err != nil {
		return err
	}
	if len(data) < length {
		return fmt.Errorf("payload is smaller than should be according to length")
	}
	resp <- data[:length]
	return nil
}

func (c *PriorityClient) liteServerRequest(ctx context.Context, q []byte) ([]byte, error) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, magicLiteServerQuery)
	data = append(data, tl.EncodeLength(len(q))...)
	data = append(data, q...)
	data = alignBytes(data)
	return c.Request(ctx, data)
}

func (c *PriorityClient) AverageRoundTrip() time.Duration {
	if len(c.connections) == 0 {
		return 0
	}
	var total time.Duration
	for _, conn := range c.connections {
		total += conn.AverageRoundTrip()
	}
	return total / time.Duration(len(c.connections))
}

func (c *PriorityClient) WaitMasterchainSeqno(ctx context.Context, seqno uint32, timeout uint32) error {
	data := make([]byte, 0, 12)
	data = binary.LittleEndian.AppendUint32(data, magicLiteServerWaitMasterchainSeqno)
	data = binary.LittleEndian.AppendUint32(data, seqno)
	data = binary.LittleEndian.AppendUint32(data, timeout)
	resp, err := c.liteServerRequest(ctx, data)
	if err != nil {
		return err
	}
	if len(resp) < 4 {
		return fmt.Errorf("not enough bytes for tag")
	}
	tag := binary.LittleEndian.Uint32(resp[:4])
	if tag == 0xbba9e148 {
		var errRes LiteServerErrorC
		if err = tl.Unmarshal(bytes.NewReader(resp[4:]), &errRes); err != nil {
			return err
		}
		if errRes.Code == 0 {
			return nil
		}
		return errRes
	}
	return fmt.Errorf("invalid tag")
}

func (c *PriorityClient) WaitMasterchainBlock(ctx context.Context, seqno uint32, timeout uint32) (res LiteServerBlockHeaderC, err error) {
	var (
		mc     int    = -1
		uintMc uint32 = uint32(mc)
	)
	request := LiteServerLookupBlockRequest{
		Mode: 1,
		Id: TonNodeBlockIdC{
			Workchain: uintMc,
			Shard:     0x8000000000000000,
			Seqno:     seqno,
		},
	}
	data := make([]byte, 0, 38)
	data = binary.LittleEndian.AppendUint32(data, magicLiteServerWaitMasterchainSeqno)
	data = binary.LittleEndian.AppendUint32(data, seqno)
	data = binary.LittleEndian.AppendUint32(data, timeout)
	payload, err := tl.Marshal(struct {
		tl.SumType
		Req LiteServerLookupBlockRequest `tlSumType:"fac8f71e"`
	}{SumType: "Req", Req: request})
	if err != nil {
		return res, err
	}
	data = append(data, payload...)
	resp, err := c.liteServerRequest(ctx, data)
	if err != nil {
		return res, err
	}
	if len(resp) < 4 {
		return res, fmt.Errorf("not enough bytes for tag")
	}
	tag := binary.LittleEndian.Uint32(resp[:4])
	if tag == 0xbba9e148 {
		var errRes LiteServerErrorC
		if err = tl.Unmarshal(bytes.NewReader(resp[4:]), &errRes); err != nil {
			return res, err
		}
		return res, errRes
	}
	if tag == 0x752d8219 {
		err = tl.Unmarshal(bytes.NewReader(resp[4:]), &res)
		return res, err
	}
	return res, fmt.Errorf("invalid tag")
}
