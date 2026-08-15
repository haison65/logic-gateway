package netaddr

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestParseHost(t *testing.T) {
	t.Parallel()
	ok := []string{"127.0.0.1", "::1", "localhost", "http2gw", "logic", "logic.svc"}
	for _, h := range ok {
		if _, err := ParseHost(h); err != nil {
			t.Fatalf("%q: %v", h, err)
		}
	}
	bad := []string{"", "  ", "http2gw:9000", "???", "a_b", "host/name"}
	for _, h := range bad {
		if _, err := ParseHost(h); err == nil {
			t.Fatalf("%q: expected error", h)
		}
	}
}

func TestResolveIPLiteralAndLocalhost(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lookup := func(context.Context, string) ([]net.IP, error) {
		t.Fatal("lookup must not run for IP or localhost")
		return nil, errors.New("unexpected lookup")
	}
	ip, err := ResolveIPLookup(ctx, "127.0.0.1", lookup)
	if err != nil || !ip.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("127.0.0.1: ip=%v err=%v", ip, err)
	}
	ip, err = ResolveIPLookup(ctx, "localhost", lookup)
	if err != nil || !ip.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("localhost: ip=%v err=%v", ip, err)
	}
}

func TestResolveIPHostname(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lookup := func(_ context.Context, host string) ([]net.IP, error) {
		if host != "http2gw" {
			t.Fatalf("host = %s", host)
		}
		return []net.IP{net.ParseIP("10.0.0.8"), net.ParseIP("2001:db8::1")}, nil
	}
	ip, err := ResolveIPLookup(ctx, "http2gw", lookup)
	if err != nil {
		t.Fatal(err)
	}
	if !ip.Equal(net.IPv4(10, 0, 0, 8)) {
		t.Fatalf("prefer IPv4 got %v", ip)
	}
}

func TestResolveIPInvalidAndUnspecified(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	nop := func(context.Context, string) ([]net.IP, error) {
		return nil, errors.New("no lookup")
	}
	if _, err := ResolveIPLookup(ctx, "0.0.0.0", nop); err == nil {
		t.Fatal("0.0.0.0: expected error")
	}
	if _, err := ResolveIPLookup(ctx, "???", nop); err == nil {
		t.Fatal("invalid: expected error")
	}
}

func TestResolveIPLookupFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lookup := func(context.Context, string) ([]net.IP, error) {
		return nil, errors.New("nxdomain")
	}
	if _, err := ResolveIPLookup(ctx, "missing.example", lookup); err == nil {
		t.Fatal("expected resolve error")
	}
	empty := func(context.Context, string) ([]net.IP, error) { return nil, nil }
	if _, err := ResolveIPLookup(ctx, "emptyhost", empty); err == nil {
		t.Fatal("expected no addresses")
	}
}

func TestResolveUDPAddr(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lookup := func(context.Context, string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("192.0.2.10")}, nil
	}
	addr, err := ResolveUDPAddrLookup(ctx, "http2gw", 9000, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if addr.Port != 9000 || !addr.IP.Equal(net.IPv4(192, 0, 2, 10)) {
		t.Fatalf("addr = %v", addr)
	}
	if _, err := ResolveUDPAddrLookup(ctx, "http2gw", 0, lookup); err == nil {
		t.Fatal("port 0: expected error")
	}
}
