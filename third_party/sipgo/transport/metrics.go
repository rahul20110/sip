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

	// parseErrors counts unrecoverable SIP message parse errors. On stream
	// transports (tcp/ws) an unrecoverable error means the connection lost
	// framing and is closed to recover ("connection_closed"); on datagram
	// transports (udp) the offending packet is simply dropped ("dropped").
	parseErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sipgo",
		Subsystem: "transport",
		Name:      "parse_errors_total",
		Help:      "Number of unrecoverable SIP message parse errors, by transport and outcome (connection_closed | dropped)",
	}, []string{"transport", "outcome"})
)
