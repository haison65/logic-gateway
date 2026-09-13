package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/haison65/logic-gateway/internal/config"
	"github.com/haison65/logic-gateway/internal/controlplane"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "", "YAML LoadClient (http2_client / performance); nếu set thì ưu tiên hơn flag load")
	masterURL := flag.String("master-url", "", "override master_url (control-plane)")
	var opt options
	flag.StringVar(&opt.addr, "addr", "http://127.0.0.1:8080", "địa chỉ http2gw")
	flag.UintVar(&opt.messageID, "message-id", 1001, "X-Message-Id")
	flag.StringVar(&opt.sessionID, "session-id", "sess-1", "X-Session-Id (nền consistent-hash)")
	flag.BoolVar(&opt.uniqueSession, "unique-session", false, "mỗi request một session-id riêng (trải đều Logic)")
	flag.StringVar(&opt.traceID, "trace-id", "", "X-Trace-Id (để trống thì tự tạo)")
	flag.StringVar(&opt.body, "body", "hello", "payload gửi Logic")
	flag.DurationVar(&opt.timeout, "timeout", 10*time.Second, "timeout mỗi request")
	flag.IntVar(&opt.n, "n", 1, "tổng số request (0 = không giới hạn, dùng với -d)")
	flag.IntVar(&opt.c, "c", 1, "số goroutine / stream song song")
	flag.DurationVar(&opt.duration, "d", 0, "chạy trong khoảng thời gian (0 = theo -n)")
	flag.Float64Var(&opt.qps, "qps", 0, "giới hạn start request/giây đều (token bucket burst=1; 0 = không giới hạn)")
	flag.BoolVar(&opt.verbose, "v", false, "in từng request lỗi khi đẩy tải")
	flag.StringVar(&opt.failLogPath, "fail-log", "", "file JSONL ghi các request fail (append)")
	flag.IntVar(&opt.failLogMax, "fail-log-max", 100000, "giới hạn số dòng fail log (0 = không giới hạn)")
	flag.StringVar(&opt.summaryLogPath, "summary-log", "", "file JSON tóm tắt cuối run")
	flag.StringVar(&opt.clientName, "client-name", "", "tên client ghi vào summary JSON")
	flag.Float64Var(&opt.clientCPUs, "client-cpus", 0, "CPU quota client (ghi vào summary)")
	flag.Parse()

	setFlags := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { setFlags[f.Name] = true })

	var loadCfg *config.LoadClient
	if strings.TrimSpace(*configPath) != "" {
		cfg, err := config.LoadLoadClient(*configPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "config:", err)
			os.Exit(2)
		}
		loadCfg = &cfg
		if !setFlags["addr"] {
			opt.addr = cfg.Target
		}
		if !setFlags["message-id"] {
			opt.messageID = uint(cfg.MessageID)
		}
		if !setFlags["session-id"] {
			opt.sessionID = cfg.SessionID
		}
		if !setFlags["unique-session"] {
			opt.uniqueSession = cfg.UniqueSession
		}
		if !setFlags["body"] {
			opt.body = cfg.Body
		}
		if !setFlags["timeout"] {
			opt.timeout = cfg.Timeout.Duration()
		}
		if !setFlags["c"] {
			opt.c = cfg.Concurrency
		}
		if !setFlags["qps"] {
			opt.qps = cfg.QPS
		}
		if !setFlags["d"] {
			opt.duration = cfg.Duration.Duration()
		}
		if !setFlags["n"] {
			opt.n = cfg.N
		}
		if *masterURL == "" {
			*masterURL = cfg.MasterURL
		}
	}
	if *masterURL == "" && loadCfg != nil {
		*masterURL = loadCfg.MasterURL
	}

	if opt.messageID == 0 {
		fmt.Fprintln(os.Stderr, "message-id must be > 0")
		os.Exit(2)
	}
	if opt.c < 1 {
		fmt.Fprintln(os.Stderr, "-c must be >= 1")
		os.Exit(2)
	}
	if opt.n < 0 {
		fmt.Fprintln(os.Stderr, "-n must be >= 0")
		os.Exit(2)
	}
	if opt.n == 0 && opt.duration <= 0 {
		fmt.Fprintln(os.Stderr, "cần -n > 0 hoặc -d > 0")
		os.Exit(2)
	}
	if opt.qps < 0 {
		fmt.Fprintln(os.Stderr, "-qps must be >= 0")
		os.Exit(2)
	}
	opt.addr = strings.TrimRight(opt.addr, "/") + "/v1/data"

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if loadCfg != nil && strings.TrimSpace(*masterURL) != "" {
		agentCtx, agentCancel := context.WithCancel(ctx)
		defer agentCancel()
		go func() {
			body := controlplane.BodyFromLoadClient(*loadCfg)
			_ = (&controlplane.Agent{
				MasterURL: *masterURL,
				Body:      body,
				Interval:  time.Second,
				Log:       zap.NewNop(),
			}).Run(agentCtx)
		}()
	}

	client := newH2CClient(opt.timeout)
	single := opt.n == 1 && opt.c == 1 && opt.duration == 0
	if single {
		r := doRequest(ctx, client, opt, 1)
		if r.err != nil {
			fmt.Fprintln(os.Stderr, "request:", r.err)
			os.Exit(1)
		}
		fmt.Printf("proto: HTTP/%d\n", r.protoMajor)
		fmt.Printf("status: %d\n", r.status)
		fmt.Printf("X-Transaction-Id: %s\n", r.txID)
		fmt.Printf("latency: %s\n", r.latency.Round(time.Microsecond))
		fmt.Printf("body: %s\n", string(r.body))
		if r.protoMajor != 2 {
			fmt.Fprintln(os.Stderr, "cảnh báo: không phải HTTP/2")
		}
		if r.status >= 400 {
			os.Exit(1)
		}
		return
	}

	fmt.Fprintf(os.Stderr, "load HTTP/2 h2c  n=%d c=%d d=%s qps=%.0f -> %s\n", opt.n, opt.c, opt.duration, opt.qps, opt.addr)
	start := time.Now()
	st := runLoad(ctx, client, opt)
	elapsed := time.Since(start)
	st.print(elapsed, opt.c)
	if path := strings.TrimSpace(opt.summaryLogPath); path != "" {
		sum := st.buildSummary(opt, start, elapsed)
		if err := writeSummaryJSON(path, sum); err != nil {
			fmt.Fprintln(os.Stderr, "summary log:", err)
		} else {
			fmt.Fprintf(os.Stderr, "summary_log: %s\n", path)
		}
	}
	if st.fail > 0 {
		os.Exit(1)
	}
}
