// Package netaddr tách listen (bind) và advertise/resolve (đích UDP).
// Không phải service discovery: chỉ IP literal, localhost, hoặc LookupIP khi host là tên.
package netaddr

import (
	"context"
	"fmt"
	"net"
	"strings"
	"unicode"
)

// LookupIPFunc tra cứu A/AAAA. Test thay bằng bảng cố định, không gọi Internet.
type LookupIPFunc func(ctx context.Context, host string) ([]net.IP, error)

func defaultLookup(ctx context.Context, host string) ([]net.IP, error) {
	return net.DefaultResolver.LookupIP(ctx, "ip", host)
}

// ParseHost chấp nhận IP literal hoặc hostname DNS (nhãn RFC 1123).
// Không lookup mạng. Không chấp nhận host:port.
func ParseHost(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("host is empty")
	}
	if ip := net.ParseIP(s); ip != nil {
		return s, nil
	}
	if strings.Contains(s, ":") {
		return "", fmt.Errorf("host %q must not include a port", s)
	}
	if !validHostname(s) {
		return "", fmt.Errorf("host %q is not a valid IP or hostname", s)
	}
	return s, nil
}

// IsUnspecified là true với 0.0.0.0 / :: — bind được, không dùng làm đích advertise.
func IsUnspecified(s string) bool {
	ip := net.ParseIP(strings.TrimSpace(s))
	return ip != nil && ip.IsUnspecified()
}

// ResolveIP trả về một địa chỉ IP để gửi UDP / REGISTER.
// IP literal: không lookup. localhost / ::1: loopback, không DNS.
// Hostname khác: lookup (môi trường Docker DNS hoặc /etc/hosts).
func ResolveIP(ctx context.Context, host string) (net.IP, error) {
	return ResolveIPLookup(ctx, host, defaultLookup)
}

// ResolveIPLookup giống ResolveIP với lookup thay thế (test).
func ResolveIPLookup(ctx context.Context, host string, lookup LookupIPFunc) (net.IP, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	host, err := ParseHost(host)
	if err != nil {
		return nil, err
	}
	if IsUnspecified(host) {
		return nil, fmt.Errorf("host %q is unspecified; set a reachable advertise/gateway address", host)
	}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			return v4, nil
		}
		return ip, nil
	}
	if strings.EqualFold(host, "localhost") {
		return net.IPv4(127, 0, 0, 1), nil
	}
	if lookup == nil {
		lookup = defaultLookup
	}
	ips, err := lookup(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve host %q: %w", host, err)
	}
	if ip := preferIPv4(ips); ip != nil {
		if ip.IsUnspecified() {
			return nil, fmt.Errorf("host %q resolved to unspecified address", host)
		}
		return ip, nil
	}
	return nil, fmt.Errorf("resolve host %q: no addresses", host)
}

// ResolveUDPAddr ghép IP đã resolve với port.
func ResolveUDPAddr(ctx context.Context, host string, port int) (*net.UDPAddr, error) {
	return ResolveUDPAddrLookup(ctx, host, port, defaultLookup)
}

// ResolveUDPAddrLookup giống ResolveUDPAddr với lookup thay thế.
func ResolveUDPAddrLookup(ctx context.Context, host string, port int, lookup LookupIPFunc) (*net.UDPAddr, error) {
	if port <= 0 || port > 65535 {
		return nil, fmt.Errorf("port %d out of range", port)
	}
	ip, err := ResolveIPLookup(ctx, host, lookup)
	if err != nil {
		return nil, err
	}
	return &net.UDPAddr{IP: ip, Port: port}, nil
}

func preferIPv4(ips []net.IP) net.IP {
	for _, ip := range ips {
		if ip == nil {
			continue
		}
		if v4 := ip.To4(); v4 != nil {
			return v4
		}
	}
	for _, ip := range ips {
		if ip != nil {
			return ip
		}
	}
	return nil
}

func validHostname(s string) bool {
	if len(s) == 0 || len(s) > 253 {
		return false
	}
	s = strings.TrimSuffix(s, ".")
	labels := strings.Split(s, ".")
	if len(labels) == 0 {
		return false
	}
	for _, lab := range labels {
		if !validDNSLabel(lab) {
			return false
		}
	}
	return true
}

func validDNSLabel(s string) bool {
	if n := len(s); n < 1 || n > 63 {
		return false
	}
	for i, r := range s {
		if r > unicode.MaxASCII {
			return false
		}
		ok := unicode.IsLetter(r) || unicode.IsDigit(r) || (r == '-' && i > 0 && i < len(s)-1)
		if !ok {
			return false
		}
	}
	return true
}
