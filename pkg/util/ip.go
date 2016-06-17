package util

import (
	"fmt"
	"github.com/toolkits/net"
	"strings"
)

func ConvertToIp(addr string) string {
	if strings.HasPrefix(addr, ":") {
		ips, err := net.IntranetIP()

		if err == nil {
			addr = strings.Replace(addr, ":", fmt.Sprintf("%s:", ips[0]), 1)
		}
	}

	return addr
}
