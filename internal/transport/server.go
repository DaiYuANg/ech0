package transport

import (
	"log/slog"
	"time"

	"github.com/panjf2000/gnet/v2"
	"github.com/samber/oops"
)

type ech0Server struct {
	gnet.BuiltinEventEngine
	eng       gnet.Engine
	addr      string
	multicore bool
	logger    *slog.Logger
}

func newEchoServer(addr string, logger *slog.Logger) *ech0Server {
	return &ech0Server{
		addr:   addr,
		logger: logger,
	}
}

func (es *ech0Server) OnBoot(eng gnet.Engine) gnet.Action {
	es.eng = eng
	es.logger.Info("echo v_server with multi-core=%t is listening on", es.multicore, es.addr)
	return gnet.None
}

func (es *ech0Server) OnTraffic(c gnet.Conn) gnet.Action {
	buf, _ := c.Next(-1)
	_, err := c.Write(buf)
	if err != nil {
		es.logger.Error("Error", oops.Wrap(err))
		return 0
	}
	es.logger.Info("conn %s read", c.RemoteAddr(), string(buf))
	return gnet.None
}

func (es *ech0Server) OnShutdown(eng gnet.Engine) {
	es.logger.Info("echo test_server is closing")
}

func (es *ech0Server) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	es.logger.Info("conn %s open", c.RemoteAddr())
	return nil, 0
}

func (es *ech0Server) OnTick() (delay time.Duration, action gnet.Action) {
	return 0, 0
}
