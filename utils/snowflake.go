// Snowflake 雪花 ID 生成器：纯数字 int64 输出，位宽可定制，
// 面向 DB 主键场景（BIGINT 直存、字符串主键用 NextString 十进制落库）。
//
// 位布局（从高位到低位）：时间戳毫秒 | 节点 | 序列，
// 三段位宽可按需切分，总位宽 ≤ 63（保持 int64 正数）：
//
//	默认 41+10+12 = 63 位 → 最长 19 位数字，69 年不溢出
//	41+5+7  = 53 位 → ≤ 16 位数字，可安全过 JS Number / 前端展示
//	更短位宽 → 更短数字，但时间/节点/吞吐余量相应变小
package utils

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"
)

// DefaultSnowflakeEpoch 默认纪元：2024-01-01 UTC。
// 相对 1970 纪元多出约 54 年余量，41 位毫秒可用到 2095 年。
var DefaultSnowflakeEpoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// 雪花 ID 默认位宽（经典 64 位切分去掉符号位）。
const (
	DefaultTimeBits = 41
	DefaultNodeBits = 10
	DefaultSeqBits  = 12
	// DefaultMaxRollbackWait 时钟回拨容忍窗口：窗口内自旋等待时钟追上，
	// 超窗直接报错（宁可拒绝生成，也不产出重复 ID）。
	DefaultMaxRollbackWait = 2 * time.Second
)

// SnowflakeConfig 生成器配置。零值字段取默认值。
type SnowflakeConfig struct {
	// TimeBits / NodeBits / SeqBits 三段位宽，总和必须 ≤ 63。
	// TimeBits ≥ 1；NodeBits / SeqBits 允许 0（单机 / 每毫秒 1 个）。
	//
	// 零值段按该段默认值补齐（41 / 10 / 12）：三段全零即默认切分，
	// 只给出其中几段时未给出的段取默认值。因此「NodeBits:5, SeqBits:7」
	// 得到的是 41+5+7=53 位（JS 安全整数方案），而不是把 TimeBits
	// 当成 0/1 —— 时间戳 1 位意味着 1ms 后就无法发号。
	// 需要精确控制（例如 SeqBits=0）请把三段全部显式写出。
	TimeBits int
	NodeBits int
	SeqBits  int

	// Node 节点 ID，多实例部署必须互不相同，范围 [0, 2^NodeBits)。
	Node int64

	// StartTime 纪元（时间戳基准），零值用 DefaultSnowflakeEpoch。
	// 必须晚于 1970 且早于当前时间。
	StartTime time.Time

	// MaxRollbackWait 时钟回拨等待上限，<=0 用 DefaultMaxRollbackWait。
	MaxRollbackWait time.Duration
}

// Snowflake 雪花 ID 生成器。并发安全；一个进程内每种节点身份建一个实例即可。
type Snowflake struct {
	cfg SnowflakeConfig

	timeShift uint // seq 在低位，向左依次是 node、time
	nodeShift uint
	maxSeq    int64
	maxNode   int64
	// maxTime 是时间戳字段能表示的最大毫秒数（2^TimeBits - 1）。
	// 超过它就必须拒发：否则 ms 会溢出到符号位，静默产出负数 ID。
	maxTime int64

	mu     sync.Mutex
	lastMS int64 // 已使用的最大毫秒（相对纪元）
	seq    int64 // 当前毫秒内序列
	now    func() time.Time
}

// NewSnowflake 构造生成器。配置非法（位宽越界、Node 溢出、纪元在未来）
// 返回错误。
func NewSnowflake(cfg SnowflakeConfig) (*Snowflake, error) {
	// 位宽补齐：三段全零 → 默认切分；否则为 0 的段按该段默认值补齐。
	//
	// 旧实现把「TimeBits 为 0 但其余非零」静默按最小值 1 处理，于是
	// {NodeBits: 5, SeqBits: 7}（本意是 41+5+7=53 位的 JS 安全整数方案）
	// 得到的是 1+5+7=13 位 —— 时间戳只有 1 位，1ms 之后就溢出拒发。
	// 配置笔误被静默接受成另一个语义，是这个字段最难排查的坑。
	if cfg.TimeBits == 0 && cfg.NodeBits == 0 && cfg.SeqBits == 0 {
		cfg.TimeBits, cfg.NodeBits, cfg.SeqBits = DefaultTimeBits, DefaultNodeBits, DefaultSeqBits
	} else {
		if cfg.TimeBits == 0 {
			cfg.TimeBits = DefaultTimeBits
		}
		if cfg.NodeBits == 0 {
			cfg.NodeBits = DefaultNodeBits
		}
		if cfg.SeqBits == 0 {
			cfg.SeqBits = DefaultSeqBits
		}
	}
	if cfg.TimeBits < 1 || cfg.NodeBits < 0 || cfg.SeqBits < 0 {
		return nil, fmt.Errorf("snowflake: invalid bit widths time=%d node=%d seq=%d",
			cfg.TimeBits, cfg.NodeBits, cfg.SeqBits)
	}
	if total := cfg.TimeBits + cfg.NodeBits + cfg.SeqBits; total > 63 {
		return nil, fmt.Errorf("snowflake: total bits %d exceed 63 (time=%d node=%d seq=%d)",
			total, cfg.TimeBits, cfg.NodeBits, cfg.SeqBits)
	}

	if cfg.MaxRollbackWait <= 0 {
		cfg.MaxRollbackWait = DefaultMaxRollbackWait
	}
	if cfg.StartTime.IsZero() {
		cfg.StartTime = DefaultSnowflakeEpoch
	}
	if cfg.StartTime.Before(DefaultSnowflakeEpoch.AddDate(-54, 0, 0)) {
		return nil, errors.New("snowflake: StartTime must not precede 1970")
	}

	// 时间戳字段的上界：超过它就拒发，避免溢出成负数 ID。
	var maxTime int64
	if cfg.TimeBits >= 63 {
		// 1<<63 会溢出成负数，直接取 int64 上界。
		maxTime = math.MaxInt64
	} else {
		maxTime = int64(1)<<cfg.TimeBits - 1
	}

	s := &Snowflake{
		cfg:       cfg,
		timeShift: uint(cfg.NodeBits + cfg.SeqBits),
		nodeShift: uint(cfg.SeqBits),
		// 用 int64(1) 而不是 1：后者是 int，在 32 位平台（GOARCH=386/arm）
		// 上 SeqBits/NodeBits ≥ 32 时移位会溢出。
		maxSeq:  int64(1)<<cfg.SeqBits - 1,
		maxNode: int64(1)<<cfg.NodeBits - 1,
		maxTime: maxTime,
		now:     time.Now,
	}
	if cfg.Node < 0 || cfg.Node > s.maxNode {
		return nil, fmt.Errorf("snowflake: node %d out of range [0, %d]", cfg.Node, s.maxNode)
	}
	if s.now().Before(cfg.StartTime) {
		return nil, errors.New("snowflake: StartTime is in the future")
	}
	return s, nil
}

// Next 生成下一个纯数字 ID（int64，可直接作 BIGINT 主键）。
// 时钟回拨超过 MaxRollbackWait 时返回错误。
func (s *Snowflake) Next() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ms, err := s.advance()
	if err != nil {
		return 0, err
	}
	return (ms << s.timeShift) | (s.cfg.Node << s.nodeShift) | s.seq, nil
}

// NextString 生成十进制字符串形式的 ID（无符号、无前缀，
// 适合字符串主键 / 对外暴露的订单号等场景）。
func (s *Snowflake) NextString() (string, error) {
	id, err := s.Next()
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(id, 10), nil
}

// advance 在持锁状态下推进到可分配的毫秒与序列。
// 处理两件事：时钟回拨（等待追上，超窗报错）、单毫秒序列耗尽（进位到下一毫秒）。
func (s *Snowflake) advance() (int64, error) {
	ms := s.currentMS()

	// 时钟回拨：等待真实时钟追上 lastMS，超过容忍窗口则拒绝。
	if ms < s.lastMS {
		behind := time.Duration(s.lastMS-ms) * time.Millisecond
		if behind > s.cfg.MaxRollbackWait {
			return 0, fmt.Errorf("snowflake: clock moved back %v (> %v), refuse to generate",
				behind, s.cfg.MaxRollbackWait)
		}
		// 循环内必须再设一道绝对 deadline：behind 只是「进入循环时」的
		// 落后量，若此后墙钟不再推进（容器时钟冻结、时钟源异常、注入的
		// 假时钟停在某一刻），仅靠 ms >= s.lastMS 会永远等下去 ——
		// 而且是**持锁**等待，整个进程的 ID 生成被永久阻塞。
		deadline := time.Now().Add(s.cfg.MaxRollbackWait)
		for {
			if time.Now().After(deadline) {
				return 0, fmt.Errorf("snowflake: clock stalled %v while catching up from rollback, refuse to generate",
					s.cfg.MaxRollbackWait)
			}
			time.Sleep(time.Millisecond)
			ms = s.currentMS()
			if ms >= s.lastMS {
				break
			}
		}
	}

	if ms == s.lastMS {
		// 同一毫秒内发号：序列递增；耗尽则推进到下一毫秒。
		s.seq = (s.seq + 1) & s.maxSeq
		if s.seq == 0 {
			// 同样是持锁等待，同样需要 deadline（理由同上）。
			deadline := time.Now().Add(time.Second)
			for ms <= s.lastMS {
				if time.Now().After(deadline) {
					return 0, errors.New("snowflake: clock stalled waiting for next millisecond, refuse to generate")
				}
				time.Sleep(50 * time.Microsecond)
				ms = s.currentMS()
			}
		}
	} else {
		s.seq = 0
	}

	// 时间戳位宽耗尽：继续发号会让 ms 溢出到符号位，静默产出负数 ID
	// （BIGINT 主键出现负值、NextString 出现 '-'）。与「回拨超窗拒发」
	// 保持一致的宁缺勿错策略。
	if ms > s.maxTime {
		return 0, fmt.Errorf("snowflake: timestamp bits exhausted (ms=%d > max=%d), refuse to generate", ms, s.maxTime)
	}

	s.lastMS = ms
	return ms, nil
}

func (s *Snowflake) currentMS() int64 {
	return s.now().Sub(s.cfg.StartTime).Milliseconds()
}

// ParsedID 是 Parse 的反解结果。
type ParsedID struct {
	Time time.Time // 生成时刻（含纪元偏移）
	Node int64
	Seq  int64
}

// Parse 反解 ID 的生成时间、节点与序列（与生成时使用的位宽配置匹配
// 才有意义；位宽不同的实例解析结果不可比）。
func (s *Snowflake) Parse(id int64) ParsedID {
	ms := id >> s.timeShift
	return ParsedID{
		Time: s.cfg.StartTime.Add(time.Duration(ms) * time.Millisecond),
		Node: (id >> s.nodeShift) & s.maxNode,
		Seq:  id & s.maxSeq,
	}
}
