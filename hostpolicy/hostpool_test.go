package hostpolicy

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"

	"github.com/hailocab/go-hostpool"

	"github.com/gocql/gocql"
)

// Tests of the host pool host selection policy implementation
func TestHostPolicy_HostPool(t *testing.T) {
	policy := HostPool(hostpool.New(nil))

	//hosts := []*gocql.HostInfo{
	//	{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 0)},
	//	{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 1)},
	//}
	firstHost := gocql.HostInfoBuilder{
		HostId:         "a0000000-0000-0000-0000-000000000000",
		ConnectAddress: net.IPv4(10, 0, 0, 0),
	}.Build()
	secHost := gocql.HostInfoBuilder{
		HostId:         "a0000000-0000-0000-0000-000000000001",
		ConnectAddress: net.IPv4(10, 0, 0, 1),
	}.Build()
	hosts := []*gocql.HostInfo{&firstHost, &secHost}
	// Using set host to control the ordering of the hosts as calling "AddHost" iterates the map
	// which will result in an unpredictable ordering
	policy.(*hostPoolHostPolicy).SetHosts(hosts)

	// the first host selected is actually at [1], but this is ok for RR
	// interleaved iteration should always increment the host
	iter := policy.Pick(nil)
	actualA := iter()
	if actualA.Info().HostID() != "a0000000-0000-0000-0000-000000000000" {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actualA.Info().HostID())
	}
	actualA.Mark(nil)

	actualB := iter()
	if actualB.Info().HostID() != "a0000000-0000-0000-0000-000000000001" {
		t.Errorf("Expected hosts[1] but was hosts[%s]", actualB.Info().HostID())
	}
	actualB.Mark(fmt.Errorf("error"))

	actualC := iter()
	if actualC.Info().HostID() != "a0000000-0000-0000-0000-000000000000" {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actualC.Info().HostID())
	}
	actualC.Mark(nil)

	actualD := iter()
	if actualD.Info().HostID() != "a0000000-0000-0000-0000-000000000000" {
		t.Errorf("Expected hosts[0] but was hosts[%s]", actualD.Info().HostID())
	}
	actualD.Mark(nil)
}

func TestHostPoolAddressChanges(t *testing.T) {
	for _, setHosts := range []bool{false, true} {
		t.Run(fmt.Sprintf("SetHosts=%t", setHosts), func(t *testing.T) {
			hp := hostpool.New(nil)
			t.Cleanup(hp.Close)
			policy := HostPool(hp).(*hostPoolHostPolicy)
			first := gocql.HostInfoBuilder{HostId: "a0000000-0000-0000-0000-000000000000", ConnectAddress: net.IPv4(10, 0, 0, 1)}
			second := gocql.HostInfoBuilder{HostId: "a0000000-0000-0000-0000-000000000001", ConnectAddress: net.IPv4(10, 0, 0, 2)}
			a, b := first.Build(), second.Build()
			if setHosts {
				policy.SetHosts([]*gocql.HostInfo{&a, &b})
			} else {
				policy.AddHost(&a)
				policy.AddHost(&b)
			}

			setHostPoolTestTranslation(t, &first, "192.168.0.1")
			setHostPoolTestTranslation(t, &second, "192.168.0.2")
			a, b = first.Build(), second.Build()
			policy.HostUp(&a)
			if got := len(hp.Hosts()); got != 2 {
				t.Fatalf("HostUp after address change registered %d peers, want 2", got)
			}
			policy.HostDown(&a)
			third := gocql.HostInfoBuilder{HostId: "a0000000-0000-0000-0000-000000000002", ConnectAddress: net.IPv4(10, 0, 0, 3)}.Build()
			policy.AddHost(&third)
			policy.RemoveHost(&third)
			for i := 0; i < 10; i++ {
				selected := policy.Pick(nil)()
				if selected == nil || selected.Info() != &b {
					t.Fatalf("Pick after removals = %v, want remaining host", selected)
				}
				selected.Mark(nil)
			}
			policy.RemoveHost(&b)
			if selected := policy.Pick(nil)(); selected != nil {
				t.Fatalf("Pick after removing all hosts = %v, want nil", selected)
			}
		})
	}
}

func TestHostPoolMarksAfterAddressChange(t *testing.T) {
	hp := hostpool.New(nil)
	t.Cleanup(hp.Close)
	policy := HostPool(hp).(*hostPoolHostPolicy)
	first := gocql.HostInfoBuilder{HostId: "a0000000-0000-0000-0000-000000000000", ConnectAddress: net.IPv4(10, 0, 0, 1)}
	a := first.Build()
	b := gocql.HostInfoBuilder{HostId: "a0000000-0000-0000-0000-000000000001", ConnectAddress: net.IPv4(10, 0, 0, 2)}.Build()
	policy.SetHosts([]*gocql.HostInfo{&a, &b})
	selected := policy.Pick(nil)()
	if selected == nil || selected.Info() != &a {
		t.Fatalf("first Pick = %v, want first host", selected)
	}
	setHostPoolTestTranslation(t, &first, "192.168.0.1")
	a = first.Build()
	selected.Mark(fmt.Errorf("request failed"))
	for i := 0; i < 10; i++ {
		selected = policy.Pick(nil)()
		if selected == nil || selected.Info() != &b {
			t.Fatalf("Pick after failed mark = %v, want healthy host", selected)
		}
		selected.Mark(nil)
	}
}

func TestHostPoolWithoutHostIDs(t *testing.T) {
	for _, source := range []string{"connect", "rpc", "unspecified", "preferred", "broadcast", "peer", "translated-only"} {
		t.Run(source, func(t *testing.T) {
			hp := hostpool.New(nil)
			t.Cleanup(hp.Close)
			policy := HostPool(hp).(*hostPoolHostPolicy)
			first := gocql.HostInfoBuilder{ConnectAddress: net.IPv4(10, 0, 0, 1)}
			second := gocql.HostInfoBuilder{ConnectAddress: net.IPv4(10, 0, 0, 2)}
			for _, builder := range []*gocql.HostInfoBuilder{&first, &second} {
				address := builder.ConnectAddress
				switch source {
				case "rpc", "unspecified":
					builder.RpcAddress, builder.ConnectAddress = address, nil
					if source == "unspecified" {
						builder.ConnectAddress = net.IPv4zero
					}
				case "preferred":
					builder.PreferredIP, builder.ConnectAddress = address, nil
				case "broadcast":
					builder.BroadcastAddress, builder.ConnectAddress = address, nil
				case "peer":
					builder.Peer, builder.ConnectAddress = address, nil
				case "translated-only":
					setHostPoolTestTranslation(t, builder, address.String())
					builder.ConnectAddress = nil
				}
			}
			a, b := first.Build(), second.Build()
			policy.SetHosts([]*gocql.HostInfo{&a, &b})
			setHostPoolTestTranslation(t, &first, "192.168.0.1")
			setHostPoolTestTranslation(t, &second, "192.168.0.2")
			a, b = first.Build(), second.Build()
			policy.HostUp(&a)
			if got := len(policy.hostMap); got != 2 {
				t.Fatalf("registered %d hosts without IDs, want 2", got)
			}
			policy.RemoveHost(&a)
			if selected := policy.Pick(nil)(); selected == nil || selected.Info() != &b {
				t.Fatalf("Pick = %v, want remaining host without ID", selected)
			}
		})
	}
}

func TestHostPoolEquivalentTranslatedOnlyHosts(t *testing.T) {
	for _, register := range []string{"SetHosts", "AddHost"} {
		t.Run(register, func(t *testing.T) {
			hp := &hostPoolRegistrationSpy{HostPool: hostpool.New(nil)}
			t.Cleanup(hp.Close)
			policy := HostPool(hp).(*hostPoolHostPolicy)
			builder := gocql.HostInfoBuilder{}
			setHostPoolTestTranslation(t, &builder, "192.168.0.1")
			first, equivalent := builder.Build(), builder.Build()
			if register == "SetHosts" {
				policy.SetHosts([]*gocql.HostInfo{&first})
			} else {
				policy.AddHost(&first)
			}
			policy.RemoveHost(&equivalent)
			if selected := policy.Pick(nil)(); selected != nil {
				t.Fatal("equivalent host did not remove registration")
			}
			policy.AddHost(&first)
			setHostPoolTestTranslation(t, &builder, "192.168.0.2")
			first = builder.Build()
			equivalent = builder.Build()
			updates := hp.updates
			policy.AddHost(&equivalent)
			if hp.updates != updates || len(policy.hostMap) != 1 {
				t.Fatal("equivalent translated host created another backend peer")
			}
			if selected := policy.Pick(nil)(); selected == nil || selected.Info() != &equivalent {
				t.Fatal("equivalent host did not refresh registration")
			}
			policy.RemoveHost(&first)
			if selected := policy.Pick(nil)(); selected != nil {
				t.Fatal("equivalent translated host did not remove registration")
			}
		})
	}
}

func TestHostPoolRegisteredIdentityBeforeEquivalentEndpoint(t *testing.T) {
	hp := hostpool.New(nil)
	t.Cleanup(hp.Close)
	policy := HostPool(hp).(*hostPoolHostPolicy)
	builder := gocql.HostInfoBuilder{}
	setHostPoolTestTranslation(t, &builder, "192.168.0.1")
	first := builder.Build()
	builder = gocql.HostInfoBuilder{}
	setHostPoolTestTranslation(t, &builder, "192.168.0.2")
	second := builder.Build()
	policy.SetHosts([]*gocql.HostInfo{&first, &second})
	keys := make(map[*gocql.HostInfo]string)
	for key, host := range policy.hostMap {
		keys[host] = key
	}
	first = builder.Build()
	for i := 0; i < 256; i++ {
		policy.HostUp(&first)
		if policy.hostMap[keys[&first]] != &first || policy.hostMap[keys[&second]] != &second {
			t.Fatal("colliding endpoint displaced another registered host")
		}
	}
	policy.RemoveHost(&first)
	if selected := policy.Pick(nil)(); selected == nil || selected.Info() != &second {
		t.Fatal("removing colliding host did not preserve the other registration")
	}
}

func TestHostPoolTranslatedEndpointReuse(t *testing.T) {
	for _, update := range []string{"SetHosts", "AddHost"} {
		t.Run(update, func(t *testing.T) {
			hp := hostpool.New(nil)
			t.Cleanup(hp.Close)
			policy := HostPool(hp).(*hostPoolHostPolicy)
			builder := gocql.HostInfoBuilder{}
			setHostPoolTestTranslation(t, &builder, "192.168.0.1")
			first := builder.Build()
			policy.SetHosts([]*gocql.HostInfo{&first})
			setHostPoolTestTranslation(t, &builder, "192.168.0.2")
			first = builder.Build()
			builder = gocql.HostInfoBuilder{}
			setHostPoolTestTranslation(t, &builder, "192.168.0.1")
			second := builder.Build()
			if update == "SetHosts" {
				policy.SetHosts([]*gocql.HostInfo{&first, &second})
			} else {
				policy.AddHost(&second)
			}
			registered := make(map[*gocql.HostInfo]bool)
			for _, host := range policy.hostMap {
				registered[host] = true
			}
			if len(hp.Hosts()) != 2 || !registered[&first] || !registered[&second] {
				t.Fatal("reused endpoint lost a distinct registration")
			}
			policy.RemoveHost(&first)
			if selected := policy.Pick(nil)(); selected == nil || selected.Info() != &second {
				t.Fatal("removal did not preserve the host reusing the endpoint")
			}
		})
	}
}

func TestHostPoolEquivalentTranslatedBatch(t *testing.T) {
	for _, register := range []string{"SetHosts", "AddHost"} {
		t.Run(register, func(t *testing.T) {
			hp := hostpool.New(nil)
			t.Cleanup(hp.Close)
			policy := HostPool(hp).(*hostPoolHostPolicy)
			builder := gocql.HostInfoBuilder{}
			setHostPoolTestTranslation(t, &builder, "192.168.0.1")
			first, equivalent := builder.Build(), builder.Build()
			if register == "SetHosts" {
				policy.SetHosts([]*gocql.HostInfo{&first, &equivalent})
			} else {
				policy.AddHost(&first)
				policy.AddHost(&equivalent)
			}
			if len(policy.hostMap) != 1 {
				t.Fatal("equivalent records created distinct registrations")
			}
			if selected := policy.Pick(nil)(); selected == nil || selected.Info() != &equivalent {
				t.Fatal("registration did not retain the last equivalent record")
			}
			policy.RemoveHost(&first)
			if selected := policy.Pick(nil)(); selected != nil {
				t.Fatal("removal left an equivalent registration selectable")
			}
		})
	}
}

func TestHostPoolEquivalentBatchWithExistingEndpointAliases(t *testing.T) {
	for i := 0; i < 128; i++ {
		hp := hostpool.New(nil)
		t.Cleanup(hp.Close)
		policy := HostPool(hp).(*hostPoolHostPolicy)
		builder := gocql.HostInfoBuilder{}
		setHostPoolTestTranslation(t, &builder, "192.168.0.1")
		first := builder.Build()
		builder = gocql.HostInfoBuilder{}
		setHostPoolTestTranslation(t, &builder, "192.168.0.2")
		second := builder.Build()
		policy.SetHosts([]*gocql.HostInfo{&first, &second})
		builder = gocql.HostInfoBuilder{}
		setHostPoolTestTranslation(t, &builder, "192.168.0.3")
		first, second = builder.Build(), builder.Build()
		fresh, equivalent := builder.Build(), builder.Build()
		policy.SetHosts([]*gocql.HostInfo{&fresh, &equivalent})
		if len(policy.hostMap) != 1 {
			t.Fatal("old endpoint aliases split equivalent batch registrations")
		}
		if selected := policy.Pick(nil)(); selected == nil || selected.Info() != &equivalent {
			t.Fatal("equivalent batch did not retain the last record")
		}
		policy.RemoveHost(&fresh)
		if selected := policy.Pick(nil)(); selected != nil {
			t.Fatal("equivalent batch removal left an endpoint registered")
		}
	}
}

func TestHostPoolIDlessHostsSharingRawAddress(t *testing.T) {
	for _, register := range []string{"SetHosts", "AddHost"} {
		t.Run(register, func(t *testing.T) {
			hp := hostpool.New(nil)
			t.Cleanup(hp.Close)
			policy := HostPool(hp).(*hostPoolHostPolicy)
			firstBuilder := gocql.HostInfoBuilder{ConnectAddress: net.IPv4(10, 0, 0, 1)}
			secondBuilder := gocql.HostInfoBuilder{ConnectAddress: net.IPv4(10, 0, 0, 1)}
			setHostPoolTestTranslation(t, &firstBuilder, "192.168.0.1")
			setHostPoolTestTranslation(t, &secondBuilder, "192.168.0.2")
			first, second := firstBuilder.Build(), secondBuilder.Build()
			if register == "SetHosts" {
				policy.SetHosts([]*gocql.HostInfo{&first, &second})
			} else {
				policy.AddHost(&first)
				policy.AddHost(&second)
			}
			if len(policy.hostMap) != 2 {
				t.Fatal("distinct translated endpoints sharing a raw address collapsed")
			}
			policy.RemoveHost(&first)
			if selected := policy.Pick(nil)(); selected == nil || selected.Info() != &second {
				t.Fatal("removal did not preserve the distinct translated endpoint")
			}
		})
	}
}

func TestHostPoolKeyOutlivesOriginalHostStorage(t *testing.T) {
	hp := hostpool.New(nil)
	t.Cleanup(hp.Close)
	policy := HostPool(hp).(*hostPoolHostPolicy)
	builder := gocql.HostInfoBuilder{}
	setHostPoolTestTranslation(t, &builder, "192.168.0.1")
	original, equivalent := builder.Build(), builder.Build()
	policy.AddHost(&original)
	policy.AddHost(&equivalent)
	builder = gocql.HostInfoBuilder{}
	setHostPoolTestTranslation(t, &builder, "192.168.0.2")
	original = builder.Build()
	policy.AddHost(&original)
	if len(policy.hostMap) != 2 {
		t.Fatal("reused host storage collided with a retained registration key")
	}
	policy.RemoveHost(&original)
	if selected := policy.Pick(nil)(); selected == nil || selected.Info() != &equivalent {
		t.Fatal("reused storage displaced the equivalent replacement")
	}
	policy.SetHosts([]*gocql.HostInfo{&equivalent})
	policy.AddHost(&original)
	if len(policy.hostMap) != 2 {
		t.Fatal("SetHosts reset registration key uniqueness")
	}
}

func TestHostPoolDuplicateBatchFailureMark(t *testing.T) {
	for _, withIDs := range []bool{false, true} {
		t.Run(fmt.Sprintf("withIDs=%v", withIDs), func(t *testing.T) {
			hp := hostpool.New(nil)
			t.Cleanup(hp.Close)
			policy := HostPool(hp).(*hostPoolHostPolicy)
			builder := gocql.HostInfoBuilder{ConnectAddress: net.IPv4(10, 0, 0, 1)}
			healthyBuilder := gocql.HostInfoBuilder{ConnectAddress: net.IPv4(10, 0, 0, 3)}
			if withIDs {
				builder.HostId = "a0000000-0000-0000-0000-000000000001"
				healthyBuilder.HostId = "a0000000-0000-0000-0000-000000000002"
			}
			first := builder.Build()
			if withIDs {
				builder.ConnectAddress = net.IPv4(10, 0, 0, 2)
			}
			equivalent, healthy := builder.Build(), healthyBuilder.Build()
			policy.SetHosts([]*gocql.HostInfo{&first, &equivalent, &healthy})
			selected := policy.Pick(nil)()
			if selected == nil || selected.Info() != &equivalent {
				t.Fatalf("withIDs=%v: first pick did not retain equivalent replacement", withIDs)
			}
			selected.Mark(fmt.Errorf("request failed"))
			for i := 0; i < 10; i++ {
				selected = policy.Pick(nil)()
				if selected == nil || selected.Info() != &healthy {
					t.Fatalf("withIDs=%v: failed duplicate remained selectable", withIDs)
				}
				selected.Mark(nil)
			}
		})
	}
}

func TestHostPoolIDlessDifferentRecordAndAddress(t *testing.T) {
	hp := hostpool.New(nil)
	t.Cleanup(hp.Close)
	policy := HostPool(hp).(*hostPoolHostPolicy)
	builder := gocql.HostInfoBuilder{}
	setHostPoolTestTranslation(t, &builder, "192.168.0.1")
	first := builder.Build()
	policy.AddHost(&first)
	builder = gocql.HostInfoBuilder{}
	setHostPoolTestTranslation(t, &builder, "192.168.0.2")
	second := builder.Build()
	policy.AddHost(&second)
	if len(policy.hostMap) != 2 {
		t.Fatal("different ID-less record and address lost its independent registration")
	}
	policy.RemoveHost(&second)
	if selected := policy.Pick(nil)(); selected == nil || selected.Info() != &first {
		t.Fatal("removing new record affected the original registration")
	}
	policy.RemoveHost(&first)
	if selected := policy.Pick(nil)(); selected != nil {
		t.Fatal("explicit removal left the original registration selectable")
	}
}

func TestHostPoolReplacesRegisteredInfo(t *testing.T) {
	hp := &hostPoolRegistrationSpy{HostPool: hostpool.New(nil)}
	t.Cleanup(hp.Close)
	policy := HostPool(hp).(*hostPoolHostPolicy)
	builder := gocql.HostInfoBuilder{HostId: "a0000000-0000-0000-0000-000000000000", ConnectAddress: net.IPv4(10, 0, 0, 1)}
	old := builder.Build()
	policy.AddHost(&old)
	builder.ConnectAddress = net.IPv4(10, 0, 0, 2)
	current := builder.Build()
	policy.AddHost(&current)
	if selected := policy.Pick(nil)(); selected == nil || selected.Info() != &current {
		t.Fatalf("Pick = %v, want replacement host information", selected)
	}
	if hp.updates != 1 || len(hp.Hosts()) != 1 {
		t.Fatalf("backend updates/peers = %d/%d, want 1/1", hp.updates, len(hp.Hosts()))
	}
}

type hostPoolRegistrationSpy struct {
	hostpool.HostPool
	updates int
}

func (p *hostPoolRegistrationSpy) SetHosts(hosts []string) {
	p.updates++
	p.HostPool.SetHosts(hosts)
}

func setHostPoolTestTranslation(t *testing.T, builder *gocql.HostInfoBuilder, address string) {
	t.Helper()
	data := fmt.Sprintf(`{"TranslatedAddresses":{"CQL":{"Address":%q,"Port":9042}}}`, address)
	if err := json.Unmarshal([]byte(data), builder); err != nil {
		t.Fatal(err)
	}
}
