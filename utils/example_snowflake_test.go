package utils_test

import (
	"fmt"
	"log"

	"github.com/zavierswong/go-infra/utils"
)

// ExampleNewSnowflake 默认配置：63 位纯数字，直接作 BIGINT 主键。
func ExampleNewSnowflake() {
	s, err := utils.NewSnowflake(utils.SnowflakeConfig{Node: 1}) // 多实例 Node 必须不同
	if err != nil {
		log.Fatal(err)
	}

	id, err := s.Next()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(id) // 纯数字 int64，如 72057594037928100

	// 字符串主键 / 对外订单号用十进制字符串。
	str, _ := s.NextString()
	fmt.Println(len(str) <= 19) // true
}

// ExampleNewSnowflake_customBits 定制 53 位：结果可安全过 JS Number。
func ExampleNewSnowflake_customBits() {
	s, err := utils.NewSnowflake(utils.SnowflakeConfig{
		TimeBits: 41, NodeBits: 5, SeqBits: 7, // 41+5+7 = 53
		Node: 3, // [0, 2^5)
	})
	if err != nil {
		log.Fatal(err)
	}

	id, err := s.Next()
	if err != nil {
		log.Fatal(err)
	}
	_ = id < 1<<53 // true：JS Number 安全整数

	// 反解出生成时间 / 节点 / 序列（排查问题时定位发号实例）。
	p := s.Parse(id)
	fmt.Println(p.Node) // 3
}

// ExampleSnowflake_Next 错误处理：时钟回拨超窗时拒绝生成（宁缺勿重）。
func ExampleSnowflake_Next() {
	s, _ := utils.NewSnowflake(utils.SnowflakeConfig{Node: 1})
	id, err := s.Next()
	if err != nil {
		// 仅当时钟回拨超过 MaxRollbackWait 时发生：
		// 拒绝发号，避免产出重复 ID。
		log.Fatal(err)
		return
	}
	_ = id
}
