/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

package unix

import (
	"github.com/mozilla-services/heka/pipeline"
	pipeline_ts "github.com/mozilla-services/heka/pipeline/testsupport"
	"github.com/mozilla-services/heka/plugins"
	plugins_ts "github.com/mozilla-services/heka/plugins/testsupport"
	"github.com/rafrombrc/gomock/gomock"
	gs "github.com/rafrombrc/gospec/src/gospec"
	"io/ioutil"
	"net"
	"os"
	"runtime"
	"sync"
)

func UnixOutputSpec(c gs.Context) {
	t := new(pipeline_ts.SimpleT)
	ctrl := gomock.NewController(t)

	c.Specify("A UnixOutput", func() {
		unixOutput := new(UdpOutput)
		config := unixOutput.ConfigStruct().(*UdpOutputConfig)

		oth := plugins_ts.NewOutputTestHelper(ctrl)
		encoder := new(plugins.PayloadEncoder)
		encoder.Init(new(plugins.PayloadEncoderConfig))

		inChan := make(chan *pipeline.PipelinePack, 1)
		rChan := make(chan *pipeline.PipelinePack, 1)
		msg := pipeline_ts.GetTestMessage()
		payload := "Write me out to the network."
		msg.SetPayload(payload)
		pack := pipeline.NewPipelinePack(rChan)
		pack.Message = msg

		oth.MockOutputRunner.EXPECT().InChan().Return(inChan)
		oth.MockOutputRunner.EXPECT().Encoder().Return(encoder)
		oth.MockOutputRunner.EXPECT().Encode(pack).Return(encoder.Encode(pack))

		c.Specify("using UDP", func() {
			addr := "@/heka/unix-out"
			config.Address = addr
			ch := make(chan string, 1)
			var wg sync.WaitGroup
			var rAddr net.Addr

			collectData := func() {
				conn, err := net.ListenPacket("unix", addr)
				if err != nil {
					ch <- err.Error()
					return
				}

				ch <- "ready"
				b := make([]byte, 1000)
				var n int
				n, rAddr, _ = conn.ReadFrom(b)
				ch <- string(b[:n])
				conn.Close()
			}

			go collectData()
			result := <-ch // Wait for server to be ready.
			c.Assume(result, gs.Equals, "ready")

			c.Specify("writes out to the network", func() {
				err := unixOutput.Init(config)
				c.Assume(err, gs.IsNil)

				wg.Add(1)
				go func() {
					err = unixOutput.Run(oth.MockOutputRunner, oth.MockHelper)
					c.Expect(err, gs.IsNil)
					wg.Done()
				}()

				inChan <- pack
				result = <-ch

				c.Expect(result, gs.Equals, payload)
				close(inChan)
				wg.Wait()
			})

			c.Specify("uses the specified local address", func() {
				config.LocalAddress = "@/heka/unix-out"
				err := unixOutput.Init(config)
				c.Assume(err, gs.IsNil)

				wg.Add(1)
				go func() {
					err = unixOutput.Run(oth.MockOutputRunner, oth.MockHelper)
					c.Expect(err, gs.IsNil)
					wg.Done()
				}()

				inChan <- pack
				result = <-ch

				c.Expect(result, gs.Equals, payload)
				c.Expect(rAddr.Network(), gs.Equals, "unix")
				c.Expect(rAddr.String(), gs.Equals, "@/heka/unix-out")
				close(inChan)
				wg.Wait()
			})
		})
	})
}
