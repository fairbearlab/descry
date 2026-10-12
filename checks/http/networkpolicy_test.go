package http

import (
	"net/netip"
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestNetworkPolicyExceptsBlockedRanges keeps the example NetworkPolicy's
// egress except lists in step with isBlockedIP, which its comments and README
// say they mirror. A range added to extraBlocked, or a netip predicate class
// the policy forgets, fails here instead of leaving the network layer more
// permissive than the dial guard.
func TestNetworkPolicyExceptsBlockedRanges(t *testing.T) {
	raw, err := os.ReadFile("../../examples/kubernetes/networkpolicy.yaml")
	if err != nil {
		t.Fatalf("read NetworkPolicy: %v", err)
	}
	var np struct {
		Spec struct {
			Egress []struct {
				To []struct {
					IPBlock *struct {
						CIDR   string   `yaml:"cidr"`
						Except []string `yaml:"except"`
					} `yaml:"ipBlock"`
				} `yaml:"to"`
			} `yaml:"egress"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(raw, &np); err != nil {
		t.Fatalf("parse NetworkPolicy: %v", err)
	}
	var excepts []netip.Prefix
	for _, rule := range np.Spec.Egress {
		for _, peer := range rule.To {
			if peer.IPBlock == nil {
				continue
			}
			for _, s := range peer.IPBlock.Except {
				p, err := netip.ParsePrefix(s)
				if err != nil {
					t.Fatalf("except %q: %v", s, err)
				}
				excepts = append(excepts, p)
			}
		}
	}
	if len(excepts) == 0 {
		t.Fatal("no ipBlock except entries found")
	}

	// The ranges the netip predicates in isBlockedIP classify, plus every
	// extraBlocked prefix.
	want := []netip.Prefix{
		netip.MustParsePrefix("127.0.0.0/8"),    // IsLoopback
		netip.MustParsePrefix("10.0.0.0/8"),     // IsPrivate
		netip.MustParsePrefix("172.16.0.0/12"),  // IsPrivate
		netip.MustParsePrefix("192.168.0.0/16"), // IsPrivate
		netip.MustParsePrefix("169.254.0.0/16"), // IsLinkLocalUnicast
		netip.MustParsePrefix("224.0.0.0/4"),    // IsMulticast
		netip.MustParsePrefix("::/128"),         // IsUnspecified
		netip.MustParsePrefix("::1/128"),        // IsLoopback
		netip.MustParsePrefix("fc00::/7"),       // IsPrivate
		netip.MustParsePrefix("fe80::/10"),      // IsLinkLocalUnicast
		netip.MustParsePrefix("ff00::/8"),       // IsMulticast
	}
	want = append(want, extraBlocked...)
	for _, w := range want {
		if !isBlockedIP(w.Addr()) {
			t.Errorf("%s: isBlockedIP allows its first address; this list is out of date", w)
		}
		covered := false
		for _, e := range excepts {
			if e.Bits() <= w.Bits() && e.Contains(w.Addr()) {
				covered = true
				break
			}
		}
		if !covered {
			t.Errorf("networkpolicy.yaml does not except %s, which isBlockedIP blocks", w)
		}
	}
}
