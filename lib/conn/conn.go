// Package conn (The Bench Connection) provides helper functions
// for the Bench Gossip Protocol, mostly dealing with buffering,
// authentication and signing.
//
// Protocol:
//
//   S: <greet> <s-challenge>
//   C: <id> hmac(key, s-challenge) <c-challenge>
//   S: <new joblist> hmac(key, joblist + c-challenge)
//   C: <logs> hmac(key, logs + s-challenge)
//
// The same hash buffer is used for reading and writing.  Therefore the
// loop should be:
//   Read(buf)
//   CheckSig()
//   Write(buf)
//   SendSig()
package conn

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"net"
	"time"
)

const (
	Port    = ":25198" // 0x626e == 'b'<<8 | 'n' ("bn" for benchnet)
	KeySize = sha256.Size
	Greet   = "bench-gossip-0\n"
)

// Node data.  The client knows it, the server has mapping from Id to key.
type Node struct {
	ClientId, NodeId uint64
	LastSeen         uint64
	Key              []byte
}

var (
	ErrProto   = errors.New("protocol error")
	ErrSig     = errors.New("signature mismatch")
	ErrKeySize = errors.New("invalid key size")
)

// Conn represents a connection on either side.
type Conn struct {
	c        net.Conn
	r        *bufio.Reader
	w        *bufio.Writer
	h        hash.Hash
	chalThem []byte // challenge we send them
	chalUs   []byte // they challenge us
}

// Reset resets the hash function.
func (c *Conn) Reset() {
	if c.h != nil {
		c.h.Reset()
	}
}

// Flush writes any buffered data to the network.
func (c *Conn) Flush() error {
	return c.w.Flush()
}

// WriteToHash adds more data to the hash.
func (c *Conn) WriteToHash(buf []byte) {
	if c.h != nil {
		c.h.Write(buf)
	}
}

// Write writes data to the hash and to the network connection.
func (c *Conn) Write(buf []byte) (int, error) {
	c.WriteToHash(buf)
	return c.w.Write(buf)
}

// Sign adds data in buf to the hash and then appends the current hash
// to buf.  Then it resets the hash.
func (c *Conn) Sign(buf []byte) []byte {
	c.WriteToHash(buf)
	c.WriteToHash(c.chalUs)
	buf = c.h.Sum(buf)
	c.h.Reset()
	return buf
}

// SendSig sends the current hash to the network and resets the hash.
func (c *Conn) SendSig() error {
	if c.h == nil {
		return ErrProto
	}
	buf := c.Sign(make([]byte, 0, KeySize))
	if _, err := c.w.Write(buf); err != nil {
		return err
	}
	return c.w.Flush()
}

// Read receives data from the network and appends it to the hash.
func (c *Conn) Read(buf []byte) (int, error) {
	n, err := c.r.Read(buf)
	if err != nil {
		return n, err
	}
	c.WriteToHash(buf[:n])
	return n, nil
}

// ReadByte is here to implement io.ByteReader, because if we
// don't, the gob decoder will wrap us in a bufio.Reader and
// overread the data from the connection.  However, gob never
// actually calls ReadByte.
func (c *Conn) ReadByte() (byte, error) {
	b, err := c.r.ReadByte()
	if err != nil {
		return 0, err
	}
	c.WriteToHash([]byte{b})
	return b, nil
}

// CheckSig receives signature from network, checks it
// and resets the hash buffer.  ErrSig is returned on mismatch.
func (c *Conn) CheckSig() error {
	var rsig, csig [KeySize]byte // received/computed sig
	if _, err := io.ReadFull(c.r, rsig[:]); err != nil {
		return err
	}
	c.WriteToHash(c.chalThem)
	c.h.Sum(csig[:0])
	c.h.Reset()
	if bytes.Compare(rsig[:], csig[:]) != 0 {
		return ErrSig
	}
	return nil
}

// SendChallenge generates a random challenge, appends it to buf
// and sends the resulting buf to the network.
func (c *Conn) SendChallenge(buf []byte) error {
	c.chalThem = make([]byte, KeySize)
	if _, err := io.ReadFull(rand.Reader, c.chalThem); err != nil {
		return err
	}
	if _, err := c.w.Write(buf); err != nil {
		return err
	}
	if _, err := c.w.Write(c.chalThem); err != nil {
		return err
	}
	return c.w.Flush()
}

// ReceiveChallenge reads challenge from the network.
func (c *Conn) ReceiveChallenge() error {
	buf := make([]byte, KeySize)
	if _, err := io.ReadFull(c.r, buf); err != nil {
		return err
	}
	c.chalUs = buf
	return nil
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.c.RemoteAddr()
}

// SetKey sets the hash key.
func (c *Conn) SetKey(key []byte) error {
	if len(key) != KeySize {
		return ErrKeySize
	}
	c.h = hmac.New(sha256.New, key)
	return nil
}

// New wraps net.Conn and returns *Conn.
// You may want to call SetKey() later.
func New(nc net.Conn) (*Conn, error) {
	// XXX: setting deadline to now + 10 min
	if err := nc.SetDeadline(time.Now().Add(10 * time.Minute)); err != nil {
		return nil, err
	}
	return &Conn{c: nc, r: bufio.NewReader(nc), w: bufio.NewWriter(nc)}, nil
}

// Dial calls net.Dial to establish the connection and creates a hash from key.
func Dial(af, addr string, key []byte) (*Conn, error) {
	nc, err := net.Dial(af, addr)
	if err != nil {
		return nil, err
	}
	c, err := New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	if err = c.SetKey(key); err != nil {
		nc.Close()
		return nil, err
	}
	return c, nil
}

// Close closes the connection.
func (c *Conn) Close() {
	c.c.Close()
}
