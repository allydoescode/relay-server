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
)

type Peer struct {
	id    string
	addr  *net.UDPAddr
	timer *time.Timer
}

var (
	peers = make(map[string]*Peer)
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {})
	go http.ListenAndServe(":3000", nil)

	laddr, err := net.ResolveUDPAddr("udp", "0.0.0.0:12345")
	if err != nil {
		log.Fatalf("error resolve local addr: %v\n", err)
	}

	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		log.Fatalf("error listen udp: %v\n", err)
	}
	defer conn.Close()

	log.Printf("listening on %s\n", conn.LocalAddr())

	buf := make([]byte, 64)
	for {
		n, raddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("error read udp for %s: %v\n", raddr, err)
			continue
		}

		msg := string(buf[:n])
		log.Printf("recv %d bytes from %s: %s\n", n, raddr, msg)

		params := strings.Split(msg, " ")
		switch params[0] {

		case "HELLO":
			remoteId := fmt.Sprint(adler32.Checksum(fmt.Append(raddr.IP, raddr.Port)))
			b := fmt.Appendf(nil, "HELLO %s", remoteId)
			_, err = conn.WriteToUDP(b, raddr)
			if err != nil {
				log.Printf("[udp] error writing to %s: %v\n", raddr.String(), err)
				continue
			}

			peer := &Peer{
				id:    remoteId,
				addr:  raddr,
				timer: time.NewTimer(10 * time.Second),
			}
			peers[remoteId] = peer

			go func() {
				for range peer.timer.C {
					log.Printf("[udp] %s timed out, removing...\n", peer.id)
					delete(peers, peer.id)

					for id, peer := range peers {
						b := fmt.Appendf(nil, "LEAVE %s", peer.id)
						_, err = conn.WriteToUDP(b, peer.addr)
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
				_, err = conn.WriteToUDP(b, peer.addr)
				if err != nil {
					log.Printf("[udp] error writing to %s: %v\n", id, err)
					continue
				}
			}

			continue

		case "REQUEST":
			senderId := params[1]
			receiverId := params[2]

			senderAddr := raddr
			peer, ok := peers[receiverId]
			if !ok {
				log.Printf("[udp] unknown id in request, ignoring...")
				continue
			}
			receiverAddr := peer.addr

			b := fmt.Appendf(nil, "REQUEST %s %s", receiverId, receiverAddr.String())
			_, err = conn.WriteToUDP(b, senderAddr)
			if err != nil {
				log.Printf("[udp] error writing to %s: %v\n", receiverAddr.String(), err)
				return
			}

			b = fmt.Appendf(nil, "REQUEST %s %s", senderId, senderAddr.String())
			_, err = conn.WriteToUDP(b, receiverAddr)
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
				_, err = conn.WriteToUDP([]byte("BAD"), raddr)
				continue
			}

			peer.timer.Reset(10 * time.Second)

			now := time.Now().UnixMilli()
			b := fmt.Appendf(nil, "PONG SERVER %s %d %d", senderId, now-senderTime, now)
			_, err := conn.WriteToUDP(b, raddr)
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
			log.Printf("[udp] unrecognized command parameter %s from %s in: %s\n", params[0], raddr, msg)
		}

	}
}
