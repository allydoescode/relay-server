package main

import (
	"fmt"
	"hash/adler32"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/xtaci/kcp-go/v5"
)

type Peer struct {
	id    string
	addr  *net.UDPAddr
	conn  *kcp.UDPSession
	timer *time.Timer
}

var (
	peers = make(map[string]*Peer)
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {})
	go http.ListenAndServe(":3000", nil)

	// laddr, err := net.ResolveUDPAddr("udp", "0.0.0.0:12345")
	// if err != nil {
	// 	log.Fatalf("error resolve local addr: %v\n", err)
	// }

	ln, err := kcp.ListenWithOptions("0.0.0.0:12345", nil, 10, 3)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer ln.Close()

	// conn, err := net.ListenUDP("udp", laddr)
	// if err != nil {
	// 	log.Fatalf("error listen udp: %v\n", err)
	// }
	// defer conn.Close()

	// log.Printf("listening on %s\n", conn.LocalAddr())

	for {
		s, err := ln.AcceptKCP()
		if err != nil {
			log.Printf("accept kcp error: %v\n", err)
			continue
		}

		// n, raddr, err := conn.ReadFromUDP(buf)
		buf, err := Read(s)
		if err != nil {
			log.Printf("error read udp for %s: %v\n", s.RemoteAddr().String(), err)
			continue
		}

		msg := string(buf)

		params := strings.Split(msg, " ")
		switch params[0] {

		case "HELLO":
			remoteId := fmt.Sprint(adler32.Checksum([]byte(s.RemoteAddr().String())))

			b := fmt.Appendf(nil, "HELLO %s", remoteId)
			// _, err = conn.WriteToUDP(b, raddr)
			err = Write(s, b)
			if err != nil {
				log.Printf("[udp] error writing to %s: %v\n", s.RemoteAddr().String(), err)
				continue
			}

			raddr, err := net.ResolveUDPAddr("udp", s.RemoteAddr().String())
			if err != nil {
				log.Printf("[udp] error resolving addr %s: %v\n", s.RemoteAddr().String(), err)
				continue
			}

			peer := &Peer{
				id:    remoteId,
				addr:  raddr,
				conn:  s,
				timer: time.NewTimer(10 * time.Second),
			}
			peers[remoteId] = peer

			go func() {
				for range peer.timer.C {
					log.Printf("[udp] %s timed out, removing...\n", peer.id)
					delete(peers, peer.id)

					for id, peer := range peers {
						b := fmt.Appendf(nil, "LEAVE %s", peer.id)
						// _, err = conn.WriteToUDP(b, peer.addr)
						err = Write(peer.conn, b)
						if err != nil {
							log.Printf("[udp] error writing to %s: %v\n", id, err)
							continue
						}
					}

					return
				}
			}()

			for id, peer := range peers {
				if id == remoteId {
					continue
				}

				b := fmt.Appendf(nil, "JOIN %s", remoteId)
				// _, err = conn.WriteToUDP(b, peer.addr)
				err = Write(peer.conn, b)
				if err != nil {
					log.Printf("[udp] error writing to %s: %v\n", id, err)
					continue
				}
			}

			continue

		case "REQUEST":
			senderId := params[1]
			receiverId := params[2]

			you, ok := peers[senderId]
			if !ok {
				log.Printf("[udp] unknown id %s in request, ignoring...\n", senderId)
			}
			senderAddr := you.addr

			peer, ok := peers[receiverId]
			if !ok {
				log.Printf("[udp] unknown id %s in request, ignoring...", receiverId)
				continue
			}
			receiverAddr := peer.addr

			b := fmt.Appendf(nil, "REQUEST %s %s", receiverId, receiverAddr.String())
			// _, err = conn.WriteToUDP(b, senderAddr)
			err = Write(peer.conn, b)
			if err != nil {
				log.Printf("[udp] error writing to %s: %v\n", receiverAddr.String(), err)
				return
			}

			b = fmt.Appendf(nil, "REQUEST %s %s", senderId, senderAddr.String())
			// _, err = conn.WriteToUDP(b, receiverAddr)
			err = Write(peer.conn, b)
			if err != nil {
				log.Printf("[udp] error writing to %s: %v\n", senderAddr.String(), err)
				return
			}

			log.Printf("[udp] request process completed for %s and %s\n", senderId, receiverId)
			continue

		case "PING":
			senderId := params[1]
			senderTime, _ := strconv.ParseInt(params[3], 10, 64)

			peer, ok := peers[senderId]
			if !ok {
				log.Printf("[udp] peer %s already timed out\n", senderId)
				// _, err = conn.WriteToUDP([]byte("BAD"), raddr)
				err = Write(peer.conn, []byte("BAD"))
				continue
			}

			peer.timer.Reset(10 * time.Second)

			now := time.Now().UnixMilli()
			b := fmt.Appendf(nil, "PONG SERVER %s %d %d", senderId, now-senderTime, now)
			// _, err := conn.WriteToUDP(b, raddr)
			err = Write(peer.conn, b)
			if err != nil {
				log.Printf("[udp] error write to udp: %v\n", err)
				return
			}

		case "PONG":
			senderId := params[1]
			senderMs, _ := strconv.ParseInt(params[3], 10, 64)
			senderTime, _ := strconv.ParseInt(params[4], 10, 64)

			now := time.Now().UnixMilli()
			log.Printf("RTT %s: %dms %dms (%d)\n", senderId, senderMs, now-senderTime, senderMs+(now-senderTime))

		default:
			log.Printf("[udp] unrecognized command parameter %s from %s in: %s\n", params[0], s.RemoteAddr().String(), msg)
		}

	}
}

func Write(conn *kcp.UDPSession, b []byte) error {
	_, err := conn.Write(b)
	if err != nil {
		return err
	}

	log.Printf("write %s: %s", conn.RemoteAddr().String(), string(b))
	return nil
}

func Read(conn *kcp.UDPSession) ([]byte, error) {
	buf := make([]byte, 1500)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}

	log.Printf("read  %s: %s", conn.RemoteAddr().String(), string(buf[:n]))
	return buf[:n], nil
}
