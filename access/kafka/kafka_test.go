package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/zavierswong/go-infra/metrics"
)

func TestClassifyErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want metrics.Reason
	}{
		{"成功", nil, metrics.ReasonNone},
		{"ctx 超时", context.DeadlineExceeded, metrics.ReasonTimeout},
		{"ctx 取消", context.Canceled, metrics.ReasonCanceled},
		{"客户端已关闭", ErrClosed, metrics.ReasonClosed},
		{"配置非法", ErrInvalidConfig, metrics.ReasonInvalid},

		// 本包哨兵 + kerr 双重 %w 包装：kerr 语义必须能穿透。
		{"消息过大（穿透 ErrProduce）",
			fmt.Errorf("%w: %w", ErrProduce, kerr.MessageTooLarge),
			metrics.ReasonInvalid},
		{"主题不存在（穿透 ErrProduce）",
			fmt.Errorf("%w: %w", ErrProduce, kerr.UnknownTopicOrPartition),
			metrics.ReasonNotFound},
		{"位移越界", kerr.OffsetOutOfRange, metrics.ReasonNotFound},
		{"主题已存在", kerr.TopicAlreadyExists, metrics.ReasonConflict},
		{"非法主题名", kerr.InvalidTopicException, metrics.ReasonInvalid},

		// retriable 错误归 connect：它们都会被客户端自动重试。
		{"分区 leader 迁移中", kerr.NotLeaderForPartition, metrics.ReasonConnect},
		{"协调器迁移中", kerr.NotCoordinator, metrics.ReasonConnect},
		{"网络异常", kerr.NetworkException, metrics.ReasonConnect},

		{"未知错误", errors.New("boom"), metrics.ReasonUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyErr(tt.err); got != tt.want {
				t.Fatalf("classifyErr(%v) = %q, 期望 %q", tt.err, got, tt.want)
			}
		})
	}
}

// 超时优先级：DeliveryTimeout 超时的错误链里同时有 ErrProduce 与
// context.DeadlineExceeded，必须归类为 timeout（先判 ctx 再判 kerr）。
func TestClassifyErrTimeoutWins(t *testing.T) {
	err := fmt.Errorf("%w: 送达超时: %w", ErrProduce, context.DeadlineExceeded)
	if got := classifyErr(err); got != metrics.ReasonTimeout {
		t.Fatalf("classifyErr = %q, 期望 timeout", got)
	}
}

// Close 之后的投递必须报 ErrClosed（而不是卡在缓冲里永远悬着）。
func TestProduceAfterClose(t *testing.T) {
	c, err := Open(Config{Brokers: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	_ = c.Close()

	if err := c.Produce(context.Background(), "t", []byte("v")); !errors.Is(err, ErrClosed) {
		t.Errorf("Produce after Close = %v, 期望 ErrClosed", err)
	}
}

// Status 在关闭后必须仍然安全可调（监控收尾时常见）。
func TestStatusAfterClose(t *testing.T) {
	c, err := Open(Config{Brokers: []string{"127.0.0.1:1"}, Name: "closed-status"})
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	_ = c.Close()

	st := c.Status()
	if !st.Closed || st.Instance != "closed-status" {
		t.Errorf("Status = %+v", st)
	}
}
