package nacos

import (
	"context"
	"fmt"
)

// ConnectResetRequest is the server's push asking a client to move to another
// node. Fields mirror api/.../remote/request/ConnectResetRequest.java:29-33 —
// note serverPort is a *string* on the wire. All three are absent when the
// server had no redirect target (ConnectionManager.java:299-305 fills them only
// when redirectAddress carried a colon), so the no-recommendation branch is the
// common one, not an edge case.
type ConnectResetRequest struct {
	RequestID    string `json:"requestId,omitempty"`
	ServerIP     string `json:"serverIp,omitempty"`
	ServerPort   string `json:"serverPort,omitempty"`
	ConnectionID string `json:"connectionId,omitempty"`
}

func (ConnectResetRequest) typeName() string { return TypeConnectResetRequest }

// resetTarget is the slice of a domain client that the reset sequence needs.
// NamingClient and ConfigClient both implement it, so the sequence is written
// once instead of twice.
type resetTarget interface {
	// currentConn returns the live connection and the address it points at
	// (nil, "" while a reconnect is in flight).
	currentConn() (*sdkConn, string)
	// switchTo dials addr, publishes it as current, and retires the old
	// connection only after the new one is handshaked.
	switchTo(ctx context.Context, addr string) error
	log(msg string, kv ...any)
	// backgroundCtx is the client's lifetime context, not the push loop's.
	backgroundCtx() context.Context
	// servers is the list the reset sequence chooses its target from.
	servers() *serverList
}

// handleConnectReset performs the official sequence for a ConnectReset push.
//
//  1. Ack on the *current* (old) stream first. The official handler only queues
//     the switch (RpcClient.java:404-424 → switchServerAsync offers a
//     capacity-1 signal), and the response is then sent through the connection
//     that is still live (GrpcClient.java:282-290). Acking after the switch would
//     put it on a stream the server has already forgotten, and the sender is
//     blocked on a 3s ack wait (ConnectionManager.java:306 →
//     GrpcConnection.java:168-179).
//  2. Choose the target: the recommended address, but only if it is one of ours —
//     RpcClient.java:319-339 drops a recommendation that is not in the server
//     list and, when it is, takes the LIST's port rather than the recommended
//     one. Otherwise move to the next entry.
//  3. Connect before closing: RpcClient.java:522-544 establishes the new
//     connection, then marks the previous one abandoned and closes it. The
//     abandon flag exists so the retired stream's error callback does not trigger
//     a second reconnect (GrpcClient.java:310-322); here the same job is done by
//     comparing the dead connection against the current one.
//
// This is routine load migration, not a fault: the official client logs it at
// info ("Try to reconnect to a new server", RpcClient.java:502-506), so we log
// at debug too.
func handleConnectReset(t resetTarget, push Push) {
	var note ConnectResetRequest
	if err := decodeBody(push, &note); err != nil {
		t.log("ConnectResetRequest undecodable; still acking to stop the re-push", "err", err)
		if c, _ := t.currentConn(); c != nil {
			_ = c.Ack(push)
		}
		return
	}
	c, addr := t.currentConn()
	if c == nil {
		return // already switching; the supervisor owns it
	}
	// Step 1: ack on the stream that is still alive.
	if err := c.Ack(push); err != nil {
		t.log("acking ConnectResetRequest failed; the supervisor will recover", "err", err)
		return
	}
	t.log("server asked this connection to reset; moving to another node",
		"from", addr, "recommended", fmt.Sprintf("%s:%s", note.ServerIP, note.ServerPort))

	target := ""
	if note.ServerIP != "" {
		target = t.servers().pickPreferred(note.ServerIP, note.ServerPort)
		if target == "" {
			t.log("recommended address is not in our server list; ignoring it",
				"recommended", fmt.Sprintf("%s:%s", note.ServerIP, note.ServerPort))
		}
	}
	if target == "" {
		target = t.servers().nextExcluding(addr)
	}
	if target == "" {
		// Single-node list: there is nowhere to move. Re-establishing the same
		// node is still the right answer, because the server asked for a reset and
		// may have dropped state tied to this connectionId.
		target = addr
	}
	if err := t.switchTo(t.backgroundCtx(), target); err != nil {
		t.log("switching after ConnectResetRequest failed; supervisor will retry",
			"target", target, "err", err)
	}
}
