package nacos

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// These tests hold the write side to its wire shape without a server: what field
// names leave this package, and what the server's answers get turned into. The
// behaviour of a real 3.2.3 is pinned by config_write_live_test.go instead.

func writeKey() ConfigKey {
	return ConfigKey{DataID: "routes.json", Group: "DEFAULT_GROUP"}
}

// TestConfigPublishWireShape reads back the JSON the stub was handed, so a rename
// of a Go field cannot quietly change what Jackson binds on the server.
// casMd5 and additionMap are the two fields that only exist on the write path,
// and additionMap is where type and appName actually live.
func TestConfigPublishWireShape(t *testing.T) {
	stub := newConfigStub("", "")
	addr := startConfigStub(t, stub)
	c := newTestConfigClient(t, addr)

	err := c.Publish(context.Background(), writeKey(), PublishInput{
		Content:   `{"routes":[]}`,
		Type:      "json",
		AppName:   "sweet",
		ExpectMD5: "md5-seen-earlier",
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	bodies := stub.rawBodies("ConfigPublishRequest")
	if len(bodies) != 1 {
		t.Fatalf("server saw %d ConfigPublishRequest frames, want 1", len(bodies))
	}
	body := bodies[0]
	for _, want := range []string{
		`"dataId":"routes.json"`,
		`"group":"DEFAULT_GROUP"`,
		`"tenant":"public"`,
		`"content":"{\"routes\":[]}"`,
		`"casMd5":"md5-seen-earlier"`,
		`"additionMap":{"appName":"sweet","type":"json"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("request body %s lacks %s — these are the exact names the server binds", body, want)
		}
	}
	if strings.Contains(body, `"headers"`) {
		t.Errorf("headers must not ride in the body (GrpcUtils.java:108-113): %s", body)
	}
}

// TestConfigPublishLeavesOutCasAndAddition: an unconditional write should not send
// an empty casMd5, because ConfigRequestInfo.getCasMd5() is tested with
// isNotBlank and an empty string would take the publish down the CAS branch of the
// SQL anyway. Same for an additionMap nobody filled in.
func TestConfigPublishLeavesOutCasAndAddition(t *testing.T) {
	stub := newConfigStub("", "")
	addr := startConfigStub(t, stub)
	c := newTestConfigClient(t, addr)

	if err := c.Publish(context.Background(), writeKey(), PublishInput{Content: "v"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	body := stub.rawBodies("ConfigPublishRequest")[0]
	for _, absent := range []string{`"casMd5"`, `"additionMap"`} {
		if strings.Contains(body, absent) {
			t.Errorf("request body %s should omit %s entirely", body, absent)
		}
	}
}

// TestConfigPublishCasConflictIsErrConflict pins the classification the live server
// forced on us: a CAS loss arrives as resultCode 500 and errorCode 500 —
// RESOURCE_CONFLICT never reaches the wire — with the message below. Measured on
// 3.2.3: "nacos: md5 conflict: publish DEFAULT_GROUP/… (errorCode=500): Cas publish
// fail, server md5 may have changed."
func TestConfigPublishCasConflictIsErrConflict(t *testing.T) {
	stub := newConfigStub("", "")
	stub.denyPublish(failWith(500, casConflictMessage))
	addr := startConfigStub(t, stub)
	c := newTestConfigClient(t, addr)

	err := c.Publish(context.Background(), writeKey(), PublishInput{Content: "v", ExpectMD5: "stale"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("Publish with a stale md5: want ErrConflict, got %v", err)
	}
	if !strings.Contains(err.Error(), casConflictMessage) {
		t.Errorf("the server's own reason should survive: %v", err)
	}
}

// TestConfigPublishRefusalIsNotAConflict keeps the message match honest: the only
// conflict signal is that one sentence, so every other refusal has to stay
// ErrRefused rather than being swept into ErrConflict by a loose substring test.
func TestConfigPublishRefusalIsNotAConflict(t *testing.T) {
	stub := newConfigStub("", "")
	stub.denyPublish(failWith(400, "param validation error"))
	addr := startConfigStub(t, stub)
	c := newTestConfigClient(t, addr)

	err := c.Publish(context.Background(), writeKey(), PublishInput{Content: "v"})
	if errors.Is(err, ErrConflict) {
		t.Fatalf("a parameter refusal must not classify as a conflict: %v", err)
	}
	if !errors.Is(err, ErrRefused) {
		t.Fatalf("Publish: want ErrRefused, got %v", err)
	}
}

// TestConfigPublishDeniedStillClassifiesAsDenied: errorCode 403 must still reach the
// caller as ErrDenied. The credential refresh itself happens one layer down, in
// Unary, which is why a write inherits it exactly like a read does
// (conn.go:581-600, and only when ConfigOptions.Access is configured).
func TestConfigPublishDeniedStillClassifiesAsDenied(t *testing.T) {
	stub := newConfigStub("", "")
	stub.denyPublish(failWith(403, "token expired"))
	addr := startConfigStub(t, stub)
	c := newTestConfigClient(t, addr)

	err := c.Publish(context.Background(), writeKey(), PublishInput{Content: "v"})
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("Publish with an expired token: want ErrDenied, got %v", err)
	}
}

// TestConfigPublishBlankContentNeverReachesTheWire is the offline half of the same
// guard the live test checks: the caller learns it without spending a request, so a
// mistaken Publish("") cannot be sent at all — and it must not be turned into a
// Remove by the library either.
func TestConfigPublishBlankContentNeverReachesTheWire(t *testing.T) {
	stub := newConfigStub("", "")
	addr := startConfigStub(t, stub)
	c := newTestConfigClient(t, addr)

	for _, content := range []string{"", "   ", "\n\t"} {
		if err := c.Publish(context.Background(), writeKey(), PublishInput{Content: content}); err == nil {
			t.Fatalf("Publish(%q) returned nil; blank content is rejected by the server, so it is rejected here", content)
		}
	}
	if got := len(stub.rawBodies("ConfigPublishRequest")); got != 0 {
		t.Fatalf("server saw %d ConfigPublishRequest frames, want 0 — the guard is local", got)
	}
}

// TestConfigRemoveSendsNoTag is the wire half of the trap on ConfigRemoveRequest:
// the handler passes its tag field to deleteConfig as grayName
// (ConfigRemoveRequestHandler.java:76-83), so a request that carries a tag deletes a
// gray variant instead of the config. The field stays off the wire.
func TestConfigRemoveSendsNoTag(t *testing.T) {
	stub := newConfigStub("", "")
	addr := startConfigStub(t, stub)
	c := newTestConfigClient(t, addr)

	if err := c.Remove(context.Background(), writeKey()); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	bodies := stub.rawBodies("ConfigRemoveRequest")
	if len(bodies) != 1 {
		t.Fatalf("server saw %d ConfigRemoveRequest frames, want 1", len(bodies))
	}
	body := bodies[0]
	if strings.Contains(body, `"tag"`) {
		t.Errorf("request body %s must not send tag — the server reads it as grayName", body)
	}
	for _, want := range []string{`"dataId":"routes.json"`, `"group":"DEFAULT_GROUP"`, `"tenant":"public"`} {
		if !strings.Contains(body, want) {
			t.Errorf("request body %s lacks %s", body, want)
		}
	}
}

// TestConfigRemoveIsIdempotentAndRefusable covers both answers a delete can get: the
// stub's default success for a config that is already gone, and a real refusal on a
// namespace the caller cannot write to.
func TestConfigRemoveIsIdempotentAndRefusable(t *testing.T) {
	stub := newConfigStub("", "")
	addr := startConfigStub(t, stub)
	c := newTestConfigClient(t, addr)

	for i := 0; i < 2; i++ {
		if err := c.Remove(context.Background(), writeKey()); err != nil {
			t.Fatalf("Remove #%d: %v", i+1, err)
		}
	}

	stub.denyRemove(failWith(500, "delete config failed"))
	if err := c.Remove(context.Background(), writeKey()); !errors.Is(err, ErrRefused) {
		t.Fatalf("Remove against a refusing server: want ErrRefused, got %v", err)
	}
}
