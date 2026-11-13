package nacos

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The live write tests are the acceptance for the config domain's write side:
// they put the two class names in front of a real server and read back what it
// says, so the wire shapes in config_types.go are measurements rather than a
// reading of the Java source. Everything lands on throwaway dataIds in
// DEFAULT_GROUP and is removed again, including on failure.
//
//	NACOS_GRPC_ADDR=127.0.0.1:9848 go test ./nacos -run LivePublish -v
//
// With auth enabled on the server these need ConfigOptions.Access, so they assume
// the same nacos.core.auth.enabled=false instance the other live tests use.

// liveConfigClient builds a config client against the running server and returns
// a context that outlives each call but is cancelled by t.Cleanup.
func liveConfigClient(t *testing.T) (*ConfigClient, context.Context) {
	t.Helper()
	c, err := NewConfigClient(ConfigOptions{Addr: liveAddr(t)})
	if err != nil {
		t.Fatalf("NewConfigClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("connection not ready at %s: %v", liveAddr(t), err)
	}
	return c, ctx
}

func liveKey(dataID string) ConfigKey {
	return ConfigKey{DataID: dataID, Group: "DEFAULT_GROUP"}
}

func testDataID(prefix string) string {
	return fmt.Sprintf("sweet-%s-live-%d.json", prefix, time.Now().UnixNano())
}

// writeVisible is how long this server takes to make a finished publish readable.
// It is a measurement, not a timeout the library imposes.
const writeVisibleTimeout = 5 * time.Second

// queryReadable polls Query for a key that was just written and logs the delay. A
// read that is still absent when the deadline passes fails with the last error, so
// the number and the symptom stay in the same report.
//
// The wait is the server's own behaviour, not a workaround for a bad write. A
// publish commits to the database and only then raises a ConfigDataChangeEvent
// (ConfigOperationService.java:172), which a separate task manager turns into
// an in-memory cache entry (DumpService.java:133-156). Meanwhile the query side
// answers from a handler chain whose entry handler looks at that cache and nothing
// else — a cache miss is returned as CONFIG_NOT_FOUND with no database fallback
// (ConfigChainEntryHandler.java:52-76). So "Publish returned success" and "Query
// can see it" are two different instants, and a caller that treats them as one is
// coding against a read path that is only eventually consistent.
func queryReadable(t *testing.T, c *ConfigClient, ctx context.Context, key ConfigKey) Config {
	t.Helper()
	start := time.Now()
	deadline := start.Add(writeVisibleTimeout)
	for {
		got, err := c.Query(ctx, key)
		if err == nil {
			t.Logf("%s readable after %dms", key.DataID, time.Since(start).Milliseconds())
			return got
		}
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Query %s: %v", key.DataID, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s still not readable after %dms: %v", key.DataID,
				time.Since(start).Milliseconds(), err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// configGone is the other half of the same eventual window: Remove is durable
// before the cache stops serving the old value.
func configGone(t *testing.T, c *ConfigClient, ctx context.Context, key ConfigKey) {
	t.Helper()
	start := time.Now()
	deadline := start.Add(writeVisibleTimeout)
	for {
		_, err := c.Query(ctx, key)
		if errors.Is(err, ErrNotFound) {
			t.Logf("%s unreadable after %dms", key.DataID, time.Since(start).Milliseconds())
			return
		}
		if err != nil {
			t.Fatalf("Query %s: %v", key.DataID, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s still readable after %dms", key.DataID, time.Since(start).Milliseconds())
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// queryShows waits for the readable content to become want, which is a different
// wait than queryReadable: overwriting a config that is already cached does not
// make it disappear first, so a poll for "any value" returns the previous one
// immediately. Measured here on 3.2.3 standalone: a winning CAS returned success,
// and a Query one millisecond later still served the old content and the old md5.
// A publish-then-read on the same key therefore has to compare against the value it
// wrote, not against existence — which is the trap in a read-modify-write loop that
// checks only "did the query succeed".
func queryShows(t *testing.T, c *ConfigClient, ctx context.Context, key ConfigKey, want string) Config {
	t.Helper()
	start := time.Now()
	deadline := start.Add(writeVisibleTimeout)
	var last Config
	var err error
	for {
		last, err = c.Query(ctx, key)
		if err == nil && last.Content == want {
			t.Logf("%s shows %q after %dms", key.DataID, want, time.Since(start).Milliseconds())
			return last
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			t.Fatalf("Query %s: %v", key.DataID, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s still %q after %dms, want %q (err=%v)", key.DataID,
				last.Content, time.Since(start).Milliseconds(), want, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestLivePublishQueryRemoveRoundTrip is the one shot that decides whether the
// whole write side exists on this port: a ConfigPublishRequest that the server
// refuses to dispatch would come back as "answered with type EmptyResponse" or a
// parse failure, not as a classified error.
func TestLivePublishQueryRemoveRoundTrip(t *testing.T) {
	c, ctx := liveConfigClient(t)
	key := liveKey(testDataID("publish"))

	// A route-table-shaped body rather than a word: Nacos does not look inside the
	// content, and this keeps a real payload (quotes, braces, a long dataId) on the
	// wire instead of a string too small to expose an encoding mistake.
	body := `{"routes":[{"method":"POST","path":"/live/` + key.DataID + `"}]}`
	t.Cleanup(func() {
		if err := c.Remove(context.Background(), key); err != nil {
			t.Logf("cleanup remove %s: %v", key.DataID, err)
		}
	})

	if err := c.Publish(ctx, key, PublishInput{Content: body, Type: "json"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := queryReadable(t, c, ctx, key)
	if got.Content != body {
		t.Fatalf("content read back differs:\n got %q\nwant %q", got.Content, body)
	}
	if got.MD5 == "" {
		t.Fatal("server returned an empty md5 for a config just published")
	}
	t.Logf("published %s: md5=%s lastModified=%d", key.DataID, got.MD5, got.LastModified)

	if err := c.Remove(ctx, key); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	configGone(t, c, ctx, key)

	// Deleting twice must stay quiet: the server looks the row up and does nothing
	// when it is absent (EmbeddedConfigInfoPersistServiceImpl.java:494-519).
	if err := c.Remove(ctx, key); err != nil {
		t.Fatalf("second Remove of a gone config should not fail, got %v", err)
	}
}

// TestLivePublishCas reads the two directions of a conditional write: the losing
// publish, which is what ErrConflict exists for, and the winning one.
func TestLivePublishCas(t *testing.T) {
	c, ctx := liveConfigClient(t)
	key := liveKey(testDataID("cas"))
	t.Cleanup(func() {
		if err := c.Remove(context.Background(), key); err != nil {
			t.Logf("cleanup remove %s: %v", key.DataID, err)
		}
	})

	if err := c.Publish(ctx, key, PublishInput{Content: "first"}); err != nil {
		t.Fatalf("seed Publish: %v", err)
	}
	cur := queryReadable(t, c, ctx, key)

	// Lose: an md5 that is not the server's.
	if err := c.Publish(ctx, key, PublishInput{Content: "lost", ExpectMD5: "00000000000000000000000000000000"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("Publish with a stale md5: want ErrConflict, got %v", err)
	} else {
		// The reading this test exists to capture: the server has no dedicated
		// errorCode for a CAS loss, so the message is the only signal.
		t.Logf("cas loss reported as: %v", err)
		if !strings.Contains(err.Error(), "errorCode=500") {
			t.Logf("warning: expected errorCode=500 in the conflict text, got %q", err)
		}
	}

	// Still the first value: a lost CAS must not have written anything.
	after := queryReadable(t, c, ctx, key)
	if after.Content != "first" || after.MD5 != cur.MD5 {
		t.Fatalf("losing CAS changed the config: content=%q md5=%s", after.Content, after.MD5)
	}

	// Win: the md5 we hold, so the write lands and the md5 moves.
	if err := c.Publish(ctx, key, PublishInput{Content: "won", ExpectMD5: cur.MD5}); err != nil {
		t.Fatalf("Publish with the current md5: %v", err)
	}
	won := queryShows(t, c, ctx, key, "won")
	if won.MD5 == cur.MD5 {
		t.Fatalf("md5 did not move after a successful CAS write: %s", won.MD5)
	}
	t.Logf("cas win: md5 %s → %s", cur.MD5, won.MD5)
}

// TestLivePublishCasCreatesAbsent pins the asymmetry documented on
// PublishInput.ExpectMD5: CAS is not "update only". Against a key that does not
// exist the server creates it and ignores the md5 it was given
// (insertOrUpdateCas → addConfigInfo), which is why a first publish cannot lose.
func TestLivePublishCasCreatesAbsent(t *testing.T) {
	c, ctx := liveConfigClient(t)
	key := liveKey(testDataID("cas-create"))
	t.Cleanup(func() {
		if err := c.Remove(context.Background(), key); err != nil {
			t.Logf("cleanup remove %s: %v", key.DataID, err)
		}
	})

	if _, err := c.Query(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("precondition: %s should not exist, got %v", key.DataID, err)
	}
	if err := c.Publish(ctx, key, PublishInput{Content: "created-by-cas", ExpectMD5: "irrelevant"}); err != nil {
		t.Fatalf("CAS publish against an absent config: want success, got %v", err)
	}
	got := queryReadable(t, c, ctx, key)
	if got.Content != "created-by-cas" {
		t.Fatalf("content = %q", got.Content)
	}
}

// TestLivePublishBlankContentIsRejected checks the local guard against the server
// rather than trusting it: blank content is refused before the round trip, and
// the tail of the test asserts the key really was not created.
func TestLivePublishBlankContentIsRejected(t *testing.T) {
	c, ctx := liveConfigClient(t)
	key := liveKey(testDataID("blank"))

	err := c.Publish(ctx, key, PublishInput{Content: "   "})
	if err == nil {
		t.Fatal("Publish with blank content returned nil; the server rejects that (ParamUtils.java:98-101)")
	}
	if !strings.Contains(err.Error(), "content is required") {
		t.Fatalf("want the local guard's error, got %v", err)
	}
	// The guard must not have spent a round trip on it.
	if _, qerr := c.Query(ctx, key); !errors.Is(qerr, ErrNotFound) {
		t.Fatalf("a blank publish should not have created %s: %v", key.DataID, qerr)
	}
}
