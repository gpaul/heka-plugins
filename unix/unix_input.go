/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012-2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Rob Miller (rmiller@mozilla.com)
#   Carlos Diaz-Padron (cpadron@mozilla.com,carlos@carlosdp.io)
#
# ***** END LICENSE BLOCK *****/

package unix

import (
	"fmt"
	. "github.com/mozilla-services/heka/pipeline"
	"net"
	"strings"
	"sync"
	"time"
)

// Input plugin implementation that listens for Heka protocol messages on a
// specified Unix socket. Creates a separate goroutine for each Unix connection.
type UnixInput struct {
	listener net.Listener
	wg       sync.WaitGroup
	stopChan chan bool
	ir       InputRunner
	config   *UnixInputConfig
}

type UnixInputConfig struct {
	// String representation of the address of the network connection on which
	// the listener should be listening (e.g. "127.0.0.1:5565").
	Address string
	// So we can default to using ProtobufDecoder.
	Decoder string
	// So we can default to using HekaFramingSplitter.
	Splitter string
}

func (t *UnixInput) ConfigStruct() interface{} {
	config := &UnixInputConfig{
		Decoder:  "ProtobufDecoder",
		Splitter: "HekaFramingSplitter",
	}
	return config
}

func (t *UnixInput) Init(config interface{}) error {
	var err error
	t.config = config.(*UnixInputConfig)
	if !strings.HasPrefix(t.config.Address, "@") {
		return fmt.Errorf("only ephemeral sockets supported - prefix your path with '@'")
	}
	address, err := net.ResolveUnixAddr("unix", t.config.Address)
	if err != nil {
		return fmt.Errorf("ResolveUnixAddress failed: %s\n", err.Error())
	}
	t.listener, err = net.ListenUnix("unix", address)
	if err != nil {
		return fmt.Errorf("ListenUnix failed: %s\n", err.Error())
	}
	// We're already listening, make sure we clean up if init fails later on.
	closeIt := true
	defer func() {
		if closeIt {
			t.listener.Close()
		}
	}()
	t.stopChan = make(chan bool)
	closeIt = false
	return nil
}

// Listen on the provided Unix connection, extracting messages from the incoming
// data until the connection is closed or Stop is called on the input.
func (t *UnixInput) handleConnection(conn net.Conn) {
	deliverer := t.ir.NewDeliverer("")
	sr := t.ir.NewSplitterRunner("")

	defer func() {
		conn.Close()
		t.wg.Done()
		deliverer.Done()
	}()

	if !sr.UseMsgBytes() {
		name := t.ir.Name()
		packDec := func(pack *PipelinePack) {
			pack.Message.SetHostname("localhost")
			pack.Message.SetType(name)
		}
		sr.SetPackDecorator(packDec)
	}

	stopped := false
	for !stopped {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		select {
		case <-t.stopChan:
			stopped = true
		default:
			err := sr.SplitStream(conn, deliverer)
			if err != nil {
				if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
					// keep the connection open, we are just checking to see if
					// we are shutting down: Issue #354
				} else {
					stopped = true
				}
			}
		}
	}
}

func (t *UnixInput) Run(ir InputRunner, h PluginHelper) error {
	t.ir = ir
	var conn net.Conn
	var e error
	for {
		if conn, e = t.listener.Accept(); e != nil {
			if netErr, ok := e.(net.Error); ok && netErr.Temporary() {
				t.ir.LogError(fmt.Errorf("Unix accept failed: %s", e))
				continue
			} else {
				break
			}
		}
		t.wg.Add(1)
		go t.handleConnection(conn)
	}
	t.wg.Wait()
	return nil
}

func (t *UnixInput) Stop() {
	if err := t.listener.Close(); err != nil {
		t.ir.LogError(fmt.Errorf("Error closing listener: %s", err))
	}
	close(t.stopChan)
}

func init() {
	RegisterPlugin("UnixInput", func() interface{} {
		return new(UnixInput)
	})
}
