package main

import "testing"

func TestListenAddr(t *testing.T) {
	cases := []struct {
		name, explicit, port, loaded, want string
	}{
		{"explicit HTTP_ADDR wins over PORT", ":9000", "8084", ":8084", ":9000"},
		{"PORT used when HTTP_ADDR not set", "", "7000", ":8084", ":7000"},
		{"whitespace HTTP_ADDR ignored", "  ", "7000", ":8084", ":7000"},
		{"neither set uses loaded default", "", "", ":8084", ":8084"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := listenAddr(tc.explicit, tc.port, tc.loaded); got != tc.want {
				t.Fatalf("listenAddr(%q,%q,%q)=%q want %q", tc.explicit, tc.port, tc.loaded, got, tc.want)
			}
		})
	}
}
