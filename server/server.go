// Package server is the TCP/TLS front end: it accepts connections, gates
// them on authentication, tracks per-connection metrics, and hands each
// parsed command to a commands.Handler. It does not know what any command does.
package server

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"redis-glass/commands"
	"redis-glass/resp"
)

// Config configures a Server instance.
type Config struct {
	Addr        string
	Handler     *commands.Handler
	RequireAuth bool
	TLSCertFile string
	TLSKeyFile  string
}

// Server is a RESP TCP server with basic auth gating and metrics.
type Server struct {
	cfg       Config
	ln        net.Listener
	startTime time.Time
	connCount int64
	cmdCount  int64
}

// New builds a Server and binds its listener (optionally wrapped in TLS),
// but does not yet start accepting connections.
func New(cfg Config) (*Server, error) {
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}

	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			ln.Close()
			return nil, err
		}
		ln = tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}})
		log.Println("TLS enabled")
	} else {
		log.Println("TLS disabled (set TLS_CERT_FILE and TLS_KEY_FILE to enable)")
	}

	return &Server{cfg: cfg, ln: ln, startTime: time.Now()}, nil
}

// Uptime returns how long the server has been running.
func (s *Server) Uptime() time.Duration { return time.Since(s.startTime) }

// Connections returns the current number of open connections.
func (s *Server) Connections() int64 { return atomic.LoadInt64(&s.connCount) }

// Commands returns the lifetime count of commands processed.
func (s *Server) Commands() int64 { return atomic.LoadInt64(&s.cmdCount) }

// Close stops accepting new connections.
func (s *Server) Close() error {
	return s.ln.Close()
}

// Run accepts connections forever until the listener is closed.
func (s *Server) Run() error {
	log.Printf("Server listening on %s", s.cfg.Addr)
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return err
		}
		atomic.AddInt64(&s.connCount, 1)
		log.Printf("New connection from %s", conn.RemoteAddr())
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		atomic.AddInt64(&s.connCount, -1)
	}()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	state := &commands.ConnState{Authenticated: !s.cfg.RequireAuth}

	for {
		if !s.readAndHandle(r, w, state, conn) {
			log.Printf("Connection closed: %s", conn.RemoteAddr())
			return
		}
	}
}

// readAndHandle parses and executes one command. It returns false when the
// connection loop should stop (EOF or unrecoverable write/flush error).
func (s *Server) readAndHandle(r *bufio.Reader, w *bufio.Writer, state *commands.ConnState, conn net.Conn) (ok bool) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("recovered from panic handling connection %s: %v", conn.RemoteAddr(), rec)
			if err := resp.WriteError(w, "ERR internal error"); err == nil {
				w.Flush()
			}
			ok = true
		}
	}()

	val, err := resp.Parse(r)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false
		}
		resp.WriteError(w, "ERR Protocol error: "+err.Error())
		w.Flush()
		return false
	}

	if val.Typ != '*' || len(val.Array) == 0 {
		resp.WriteError(w, "ERR unknown command")
		w.Flush()
		return true
	}

	cmd := val.Array[0].Str
	args := val.Array[1:]
	atomic.AddInt64(&s.cmdCount, 1)

	upper := strings.ToUpper(cmd)
	if s.cfg.RequireAuth && !state.Authenticated && upper != "PING" && upper != "AUTH" {
		resp.WriteError(w, "NOAUTH Authentication required.")
		w.Flush()
		return true
	}

	if err := s.cfg.Handler.Handle(cmd, args, w, state, conn.RemoteAddr().String()); err != nil {
		return false
	}
	if err := w.Flush(); err != nil {
		return false
	}
	return true
}
