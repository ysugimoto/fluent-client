package fluent

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

func New(options ...Option) (*Client, error) {
	c := &Client{
		address:     "127.0.0.1:24224",
		bufferLimit: 8 * 1024 * 1024,
		dialTimeout: 3 * time.Second,
		marshaler:   marshalFunc(msgpackMarshal),
		network:     "tcp",
	}

	for _, opt := range options {
		switch opt.Name() {
		case "network":
			v := opt.Value().(string)
			switch v {
			case "tcp", "unix":
			default:
				return nil, errors.Errorf(`invalid network type: %s`, v)
			}
			c.network = v
		case "address":
			c.address = opt.Value().(string)
		case "dialTimeout":
			c.dialTimeout = opt.Value().(time.Duration)
		case "marshaler":
			c.marshaler = opt.Value().(marshaler)
		case "tag_prefix":
			c.tagPrefix = opt.Value().(string)
		}
	}

	return c, nil
}

// called from the writer process
func (c *Client) updateBufsize(v int) {
	c.muBufsize.Lock()
	c.bufferSize = v
	c.muBufsize.Unlock()
}

// Post posts the given structure after encoding it along with the given
// tag. If the current underlying pending buffer is not enough to hold
// this new data, an error will be returned
//
// If you would like to specify options, you may pass them at the end of
// the method. Currently you can use the following:
//
// fluent.WithTimestamp: allows you to set arbitrary timestamp values
func (c *Client) Post(tag string, v interface{}, options ...Option) error {
	c.startWriter()

	if p := c.tagPrefix; len(p) > 0 {
		tag = p + "." + tag
	}

	t := time.Now()
	for _, opt := range options {
		switch opt.Name() {
		case "timestamp":
			t = opt.Value().(time.Time)
		}
	}
	buf, err := c.marshaler.Marshal(tag, t.Unix(), v, nil)
	if err != nil {
		return errors.Wrap(err, `failed to marshal payload`)
	}

	c.muBufsize.RLock()
	isFull := c.bufferSize+len(buf) > c.bufferLimit
	c.muBufsize.RUnlock()

	if isFull {
		return errors.New("buffer full")
	}

	c.muWriter.Lock()
	if c.writerCancel == nil {
		c.muWriter.Unlock()
		return errors.New("writer has been closed. Shutdown called?")
	}
	c.writerQueue <- buf
	c.muWriter.Unlock()

	return nil
}

// Close closes the connection, but does not wait for the pending buffers
// to be flushed. If you want to make sure that background writer has properly
// exited, you should probably use the Shutdown() method
func (c *Client) Close() error {
	c.muWriter.Lock()
	defer c.muWriter.Unlock()
	if c.writerCancel == nil {
		return nil
	}
	c.writerCancel()
	c.writerCancel = nil
	return nil
}

// Shutdown closes the connection. This method will block until the
// background writer exits, or the caller explicitly cancels the
// provided context object.
func (c *Client) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background() // no cancel...
	}

	c.muWriter.Lock()
	if c.writerCancel == nil {
		c.muWriter.Unlock()
		return nil
	}

	// fire the cancel function. the background writer should
	// attempt to flush all of its pending buffers
	c.writerCancel()
	c.writerCancel = nil
	writerDone := c.writerExit
	c.muWriter.Unlock()

	select {
	case <-ctx.Done():
	case <-writerDone:
		return nil
	}
	return ctx.Err()
}