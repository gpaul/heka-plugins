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
	gs "github.com/rafrombrc/gospec/src/gospec"
	"testing"
)

func TestAllSpecs(t *testing.T) {
	r := gs.NewRunner()
	r.Parallel = false

	r.AddSpec(UnixInputSpec)
	r.AddSpec(UnixInputSpecFailure)
	r.AddSpec(UnixOutputSpec)

	gs.MainGoTest(r, t)
}
