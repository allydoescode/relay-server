package main

import (
	"context"
	"fmt"
	"hash/adler32"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/vmihailenco/msgpack/v5"
)

type Server struct {
	ID    string
	peers map[string]*Peer
	mu    sync.Mutex
}

func main() {
	ln, err := quic.ListenAddr("0.0.0.0:12345", GenerateTLSConfig(), &quic.Config{
		MaxIdleTimeout: 2 * time.Hour,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	server := Server{peers: make(map[string]*Peer)}
	for {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			log.Printf("quic accept error: %v\n", err)
			continue
		}

		go server.handleClient(conn)
	}
}

func (s *Server) handleClient(conn *quic.Conn) {
	defer conn.CloseWithError(0, "Connection closed")
	clientAddrStr := conn.RemoteAddr().String()

	for {
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			log.Printf("quic stream accept error: %v\n", err)
			return
		}

		go func(str *quic.Stream) {
			defer stream.Close()

			msg := Message{}
			decoder := msgpack.NewDecoder(stream)
			if err := decoder.Decode(&msg); err != nil {
				log.Printf("msgpack decode error: %v\n", err)
				return
			}

			log.Printf("%s sent: %v\n", clientAddrStr, msg)

			switch msg.Type {
			case "hello":
				s.handleHello(str, conn)
			case "request":
				s.handleRequest(str, msg, conn)
			case "goodbye": // 💡 ADD THIS CASE TO YOUR SERVER
				s.mu.Lock()
				delete(s.peers, msg.From) // Instantly clear the client from active pools
				s.mu.Unlock()

				s.broadcast(Message{From: "SERVER", To: "ALL", Type: "leave", Body: msg.From}, []string{})
			}

		}(stream)
	}
}

func (s *Server) broadcast(msg Message, excludeIds []string) {
	s.mu.Lock()
	for peerId, peer := range s.peers {
		if slices.Contains(excludeIds, peerId) {
			continue
		}

		go func(pc *quic.Conn) {
			pStream, err := pc.OpenStream()
			if err != nil {
				log.Printf("quic open stream error: %v\n", err)
				return
			}

			defer pStream.Close()
			_ = msgpack.NewEncoder(pStream).Encode(msg)
		}(peer.Conn)
	}
	s.mu.Unlock()
}

func (s *Server) handleHello(stream *quic.Stream, conn *quic.Conn) {
	id := fmt.Sprint(adler32.Checksum([]byte(conn.RemoteAddr().String())))

	s.mu.Lock()
	s.peers[id] = &Peer{ID: id, Conn: conn, Addr: conn.RemoteAddr()}
	s.mu.Unlock()

	encoder := msgpack.NewEncoder(stream)
	_ = encoder.Encode(Message{From: "SERVER", Type: "hello_ack", Body: id})
	s.broadcast(Message{From: "SERVER", To: "ALL", Type: "join", Body: id}, []string{id})
}

func (s *Server) handleRequest(stream *quic.Stream, msg Message, conn *quic.Conn) {
	s.mu.Lock()
	peer, ok := s.peers[msg.Body]
	if !ok {
		log.Printf("no peer with id %s\n", msg.Body)
		return
	}

	you, ok := s.peers[msg.From]
	if !ok {
		log.Printf("no peer with id %s\n", msg.From)
		return
	}

	log.Printf("Matchmaking: Coordinating P2P hole punch between Client %s and Client %s\n", you.ID, peer.ID)

	// 3. Send target coordinates back to the Requesting Client (Client A)
	// We explicitly include the target's PeerID so Client A can run the local tie-breaker math
	encoder := msgpack.NewEncoder(stream)
	err := encoder.Encode(Message{
		Type: "request_ack",
		From: "SERVER",
		To:   msg.From, // 💡 Crucial for the client's ID tie-breaker logic
		Body: strings.Join([]string{peer.ID, peer.Addr.String()}, " "),
	})
	if err != nil {
		log.Printf("Failed sending coordinates to requester: %v\n", err)
	}

	// 4. Asynchronously open a new stream to inform the Target Client (Client B)
	// This tells Client B to prepare its firewall for Client A's arrival
	go func() {
		pStream, err := peer.Conn.OpenStream()
		if err != nil {
			log.Printf("Failed to open background notification stream to target %s: %v\n", peer.ID)
			return
		}

		enc := msgpack.NewEncoder(pStream)
		err = enc.Encode(Message{
			Type: "request_ack",
			From: "SERVER",
			To:   peer.ID,
			Body: strings.Join([]string{you.ID, you.Addr.String()}, " "),
		})
		if err != nil {
			log.Printf("Failed writing coordinate payload to target stream: %v\n", err)
		}

		pStream.Close()
	}()
}
