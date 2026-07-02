package sip

import (
	"errors"
	"net"

	sipgo "github.com/emiago/sipgo/sip"
)

// ASCIIToLower is faster than go version. It avoids one more loop
func ASCIIToLower(s string) string {
	return sipgo.ASCIIToLower(s)
}

func ASCIIToLowerInPlace(s []byte) {
	sipgo.ASCIIToLowerInPlace(s)
}

// HeaderToLower is fast ASCII lower string
func HeaderToLower(s string) string {
	return sipgo.HeaderToLower(s)
}

// Forked from github.com/StefanKopieczek/gossip by @StefanKopieczek
func ResolveSelfIP() (net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip, nil
		}
	}
	return nil, errors.New("server not connected to any network")
}

func NonceWrite(buf []byte) {
	sipgo.NonceWrite(buf)
}

// MessageShortString dumps short version of msg. Used only for logging
func MessageShortString(msg Message) string {
	switch m := msg.(type) {
	case *Request:
		return m.Short()
	case *Response:
		return m.Short()
	}
	return "Unknown message type"
}
