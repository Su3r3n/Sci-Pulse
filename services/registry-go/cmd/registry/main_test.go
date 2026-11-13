package main

import "testing"

func TestListenAddrPrefersHTTPAddr(t *testing.T) {
	cases := []struct {
		httpAddr, port, want string
	}{
		{":8080", "9000", ":8080"},
		{"", "8080", ":8080"},
		{" ", "7000", ":7000"},
	}
	for _, c := range cases {
		if got := listenAddr(c.httpAddr, c.port); got != c.want {
			t.Errorf("listenAddr(%q,%q) = %q, want %q", c.httpAddr, c.port, got, c.want)
		}
	}
}
