package transport

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	bytesPacketSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   "sipgo",
		Subsystem:   "transport",
		Name:        "packet_size_bytes",
		Help:        "Size of sent and received SIP packets",
		ConstLabels: nil,
		Buckets: []float64{
			250, 500, 1000,
			1100, 1200, 1300,
			1400, 1450,
			1500, // typical MTU
			1550, 1600,
			1700, 1800,
			1900, 2000,
			3000, 4000,
		},
	}, []string{"transport", "type"})
)
