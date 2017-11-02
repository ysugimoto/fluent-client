// Package fluent implements a client for the fluentd data logging daemon.
package fluent

import (
	"context"
	"time"

	pdebug "github.com/lestrrat/go-pdebug"
	"github.com/pkg/errors"
)

// New creates a new client. Options may be one of the following:
//
//   * fluent.WithAddress
//   * fluent.WithBufferLimit
//   * fluent.WithDialTimeout
//   * fluent.WithJSONMarshaler
//   * fluent.WithMaxConnAttempts
//   * fluent.WithMsgpackMarshaler
//   * fluent.WithNetwork
//   * fluent.WithTagPrefix
//   * fluent.WithWriteThreshold
//   * fluent.WithWriteQueueSize
//
// Please see their respective documentation for details.
func New(options ...Option) (*Client, error) {
	m, err := newMinion(options...)
	if err != nil {
		return nil, err
	}

	var c Client
	ctx, cancel := context.WithCancel(context.Background())

	var subsecond bool
	for _, opt := range options {
		switch opt.Name() {
		case "subsecond":
			subsecond = opt.Value().(bool)
		}
	}
	c.minionDone = m.done
	c.minionQueue = m.incoming
	c.minionCancel = cancel
	c.subsecond = subsecond

	go m.runReader(ctx)
	go m.runWriter(ctx)

	return &c, nil
}

// Post posts the given structure after encoding it along with the given tag.
//
// An error is returned if the client has already been closed.
//
// If you would like to specify options to `Post()`, you may pass them at the end of
// the method. Currently you can use the following:
//
//   fluent.WithContext: specify context.Context to use
//   fluent.WithTimestamp: allows you to set arbitrary timestamp values
//   fluent.WithSyncAppend: allows you to verify if the append was successful
//
// If fluent.WithSyncAppend is provide and is true, the following errors
// may be returned:
//
//   1. If the current underlying pending buffer is is not large enough to
//      hold this new data, an error will be returned
//   2. If the marshaling into msgpack/json failed, it is returned
//
func (c *Client) Post(tag string, v interface{}, options ...Option) (err error) {
	if pdebug.Enabled {
		g := pdebug.Marker("fluent.Client.Post").BindError(&err)
		defer g.End()
	}
	// Do not allow processing at all if we have closed
	c.muClosed.RLock()
	defer c.muClosed.RUnlock()

	if c.closed {
		return errors.New(`client has already been closed`)
	}

	var syncAppend bool
	var subsecond = c.subsecond
	var t time.Time
	var ctx = context.Background()
	for _, opt := range options {
		switch opt.Name() {
		case optkeyTimestamp:
			t = opt.Value().(time.Time)
		case optkeySyncAppend:
			syncAppend = opt.Value().(bool)
		case optkeySubSecond:
			subsecond = opt.Value().(bool)
		case optkeyContext:
			if pdebug.Enabled {
				pdebug.Printf("client: using user-supplied context")
			}
			ctx = opt.Value().(context.Context)
		}
	}
	if t.IsZero() {
		t = time.Now()
	}

	msg := getMessage()
	msg.Tag = tag
	msg.Time.Time = t
	msg.Record = v
	msg.subsecond = subsecond

	// This has to be separate from msg.replyCh, b/c msg would be
	// put back to the pool
	var replyCh chan error
	if syncAppend {
		replyCh = make(chan error)
		msg.replyCh = replyCh
	}

	// Because case statements in a select is evaluated in random
	// order, writing to c.minionQueue in the subsequent select
	// may succeed or fail depending on the run.
	//
	// This extra check ensures that if the context is canceled
	// well in advance, we never get into the ambiguous situation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.minionDone:
		return errors.New("writer has been closed. Shutdown called?")
	case c.minionQueue <- msg:
		if pdebug.Enabled {
			pdebug.Printf("client: wrote message to queue")
		}
	}

	if syncAppend {
		if pdebug.Enabled {
			pdebug.Printf("client: Post is waiting for return status")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.minionDone:
			return errors.New("writer has been closed. Shutdown called?")
		case e := <-replyCh:
			if pdebug.Enabled {
				pdebug.Printf("client: synchronous result received")
			}
			return e
		}
	}

	return nil
}

// Close closes the connection, but does not wait for the pending buffers
// to be flushed. If you want to make sure that background minion has properly
// exited, you should probably use the Shutdown() method
func (c *Client) Close() error {
	c.muClosed.Lock()
	c.closed = true
	if c.minionQueue != nil {
		close(c.minionQueue)
		c.minionQueue = nil
	}
	c.muClosed.Unlock()

	c.minionCancel()
	return nil
}

// Shutdown closes the connection, and notifies the background worker to
// flush all existing buffers. This method will block until the
// background minion exits, or the provided context object is canceled.
func (c *Client) Shutdown(ctx context.Context) error {
	if pdebug.Enabled {
		pdebug.Printf("client: shutdown requested")
		defer pdebug.Printf("client: shutdown completed")
	}

	if ctx == nil {
		ctx = context.Background() // no cancel...
	}

	if err := c.Close(); err != nil {
		return errors.Wrap(err, `failed to close`)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.minionDone:
		return nil
	}
}
