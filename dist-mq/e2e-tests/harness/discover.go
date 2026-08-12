package harness

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
)

// Discover resolves a headless Service into one base URL per node.
//
// This is what keeps a run indifferent to cluster size: nothing is told how
// many nodes there are, it is asked. A single-node deployment answers with one
// name and a five-node deployment with five, and every layer above works the
// same either way.
//
// SRV is tried first because it yields the per-pod DNS names along with the
// port, and those names outlive the pod. The A-record fallback returns pod IPs
// instead, which are correct right now but stale after a reschedule — fine for
// a short-lived run, wrong to cache.
func Discover(ctx context.Context, service string, port int) ([]string, error) {
	if strings.TrimSpace(service) == "" {
		return nil, fmt.Errorf("harness: empty service name")
	}

	var r net.Resolver
	if _, addrs, err := r.LookupSRV(ctx, "http", "tcp", service); err == nil && len(addrs) > 0 {
		nodes := make([]string, 0, len(addrs))
		for _, a := range addrs {
			host := strings.TrimSuffix(a.Target, ".")
			p := int(a.Port)
			if p == 0 {
				p = port
			}
			nodes = append(nodes, fmt.Sprintf("http://%s:%d", host, p))
		}
		sort.Strings(nodes)
		return nodes, nil
	}

	hosts, err := r.LookupHost(ctx, service)
	if err != nil {
		return nil, fmt.Errorf("harness: resolving %q: %w", service, err)
	}
	if len(hosts) == 0 {
		return nil, fmt.Errorf("harness: %q resolved to nothing", service)
	}

	nodes := make([]string, 0, len(hosts))
	for _, h := range hosts {
		nodes = append(nodes, fmt.Sprintf("http://%s", net.JoinHostPort(h, fmt.Sprint(port))))
	}
	sort.Strings(nodes)
	return nodes, nil
}
