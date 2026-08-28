package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"hash/adler32"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/vmihailenco/msgpack/v5"
)

type PeerInfo struct {
	ID   string `msgpack:"id"`
	Addr string `msgpack:"a"`
}

type Message struct {
	Type   string     `msgpack:"t"`
	From   string     `msgpack:"f"`
	Target string     `msgpack:"tg"` // 💡 THE FIX: Ensure this line exists in BOTH files
	Data   string     `msgpack:"d"`
	Role   string     `msgpack:"r"`
	Peers  []PeerInfo `msgpack:"p"`
}

var activePeers sync.Map

func main() {
	ln, err := quic.ListenAddr("0.0.0.0:12345", generateTLS(), &quic.Config{MaxIdleTimeout: 2 * time.Hour})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("🚀 Automated Mesh Rendezvous Server running on port 12345...")

	for {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			continue
		}
		go handleClient(conn)
	}
}

func handleClient(conn *quic.Conn) {
	id := fmt.Sprint(adler32.Checksum([]byte(conn.RemoteAddr().String())))
	myAddrStr := conn.RemoteAddr().String()

	// 1. Gather a snapshot of all currently connected clients before storing ourselves
	var currentFleet []PeerInfo
	activePeers.Range(func(key, value any) bool {
		peerID := key.(string)
		peerConn := value.(*quic.Conn)
		currentFleet = append(currentFleet, PeerInfo{ID: peerID, Addr: peerConn.RemoteAddr().String()})
		return true
	})

	activePeers.Store(id, conn)
	log.Printf("👥 Peer Registered: %s (%s)\n", id, myAddrStr)

	defer func() {
		activePeers.Delete(id)
		conn.CloseWithError(0, "offline")
		log.Printf("❌ Peer Offline: %s\n", id)
	}()

	for {
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			return
		}

		go func(str *quic.Stream) {
			defer str.Close()
			var msg Message
			if err := msgpack.NewDecoder(str).Decode(&msg); err != nil {
				return
			}

			if msg.Type == "hello" {
				// Send them their assigned ID along with the entire current fleet roster
				_ = msgpack.NewEncoder(str).Encode(Message{
					Type:  "hello_ack",
					Data:  id,
					Peers: currentFleet,
				})

				// Broadcast a non-blocking alert to all existing clients to introduce the newcomer
				activePeers.Range(func(key, value any) bool {
					existingID := key.(string)
					if existingID == id {
						return true
					}
					existingConn := value.(*quic.Conn)
					go func(pc *quic.Conn) {
						tStr, err := pc.OpenStream()
						if err != nil {
							return
						}
						defer tStr.Close()
						_ = msgpack.NewEncoder(tStr).Encode(Message{
							Type: "connect_instruction", Data: myAddrStr, Target: id,
						})
					}(existingConn)
					return true
				})
			}
		}(stream)
	}
}

func generateTLS() *tls.Config {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	template := x509.Certificate{SerialNumber: big.NewInt(1), KeyUsage: x509.KeyUsageDigitalSignature}
	cert, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{cert}, PrivateKey: key}}, NextProtos: []string{"p2p"}}
}
