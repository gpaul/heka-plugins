/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012-2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

package unix

import (
	"errors"
	"fmt"
	"github.com/mozilla-services/heka/pipeline"
	"net"
)

// This is our plugin struct.
type UnixOutput struct {
	*UnixOutputConfig
	conn net.Conn
}

// This is our plugin's config struct
type UnixOutputConfig struct {
	// String representation of the address of the network connection to which
	// we will be sending out packets (e.g. "192.168.64.48:3336").
	Address string
	// Optional address to use as the local address for the connection.
	LocalAddress string `toml:"local_address"`
}

// Provides pipeline.HasConfigStruct interface.
func (o *UnixOutput) ConfigStruct() interface{} {
	return &UnixOutputConfig{}
}

// Initialize Unix connection
func (o *UnixOutput) Init(config interface{}) (err error) {
	o.UnixOutputConfig = config.(*UnixOutputConfig) // assert we have the right config type

	var unixAddr, lAddr *net.UnixAddr
	unixAddr, err = net.ResolveUnixAddr("unix", o.Address)
	if err != nil {
		return fmt.Errorf("Error resolving unix address '%s': %s", o.Address,
			err.Error())
	}
	if o.LocalAddress != "" {
		lAddr, err = net.ResolveUnixAddr("unix", o.LocalAddress)
		if err != nil {
			return fmt.Errorf("Error resolving local unix address '%s': %s",
				o.LocalAddress, err.Error())
		}
	}
	if o.conn, err = net.DialUnix("unix", lAddr, unixAddr); err != nil {
		return fmt.Errorf("Can't connect to '%s': %s", o.Address,
			err.Error())
	}
	return nil
}

func (o *UnixOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) (err error) {

	if or.Encoder() == nil {
		return errors.New("Encoder required.")
	}
	var (
		outBytes []byte
		e        error
	)
	for pack := range or.InChan() {
		if outBytes, e = or.Encode(pack); e != nil {
			or.LogError(fmt.Errorf("Error encoding message: %s", e.Error()))
		} else if outBytes != nil {
			o.conn.Write(outBytes)
		}
		pack.Recycle()
	}
	return
}

func init() {
	pipeline.RegisterPlugin("UnixOutput", func() interface{} {
		return new(UnixOutput)
	})
}
