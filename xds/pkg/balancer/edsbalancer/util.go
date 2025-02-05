/*
 * Copyright 2019 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edsbalancer

import (
	"google.golang.org/grpc/internal/wrr"
	xdsclient "google.golang.org/grpc/xds/pkg/client"
)

var newRandomWRR = wrr.NewRandom

type dropper struct {
	c xdsclient.OverloadDropConfig
	w wrr.WRR
}

func newDropper(c xdsclient.OverloadDropConfig) *dropper {
	w := newRandomWRR()
	w.Add(true, int64(c.Numerator))
	w.Add(false, int64(c.Denominator-c.Numerator))

	return &dropper{
		c: c,
		w: w,
	}
}

func (d *dropper) drop() (ret bool) {
	return d.w.Next().(bool)
}
