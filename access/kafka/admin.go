package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/zavierswong/go-infra/metrics"
)

// EnsureTopic 幂等创建主题：不存在则按 spec 创建，已存在则原样返回 nil。
//
// 与 Config.Topics 的区别：这个面向运行期（如多租户动态建主题），
// 那个面向启动期声明。同样地，"同名但属性不同"不会被这里修正 ——
// Kafka 不支持修改已存在主题的副本数等属性，需要显式迁移。
func (c *Client) EnsureTopic(ctx context.Context, spec TopicSpec) error {
	start := time.Now()
	err := c.ensureOne(ctx, spec)
	c.observe(metrics.OpDeclare, start, err, spec.Topic)
	return err
}

// ensureOne 是 EnsureTopic 的主体（Open 时批量声明复用）。
func (c *Client) ensureOne(ctx context.Context, spec TopicSpec) error {
	if spec.Topic == "" {
		return fmt.Errorf("%w: TopicSpec.Topic 不能为空", ErrInvalidConfig)
	}

	listed, err := c.adm.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("%w: 查询主题列表失败: %v", ErrTopic, err)
	}
	if _, exists := listed[spec.Topic]; exists {
		return nil
	}
	_, err = c.adm.CreateTopic(ctx, spec.Partitions, spec.ReplicationFactor, configPtrs(spec.Configs), spec.Topic)
	if err != nil {
		return fmt.Errorf("%w: 创建主题 %s (partitions=%d rf=%d): %w",
			ErrTopic, spec.Topic, spec.Partitions, spec.ReplicationFactor, err)
	}
	c.log.Infof(ctx, "已创建主题 %s (partitions=%d rf=%d)",
		spec.Topic, spec.Partitions, spec.ReplicationFactor)
	return nil
}

// configPtrs 把主题配置转成 kadm 需要的指针 map。
func configPtrs(m map[string]string) map[string]*string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]*string, len(m))
	for k, v := range m {
		v := v
		out[k] = &v
	}
	return out
}

// DeleteTopic 删除主题。数据不可恢复，调用方必须清楚自己在做什么。
func (c *Client) DeleteTopic(ctx context.Context, topic string) error {
	start := time.Now()
	res, err := c.adm.DeleteTopic(ctx, topic)
	if err == nil {
		err = res.Err
	}
	if err != nil {
		err = fmt.Errorf("%w: 删除主题 %s: %w", ErrTopic, topic, err)
	}
	c.observe(metrics.OpDeclare, start, err, topic)
	return err
}

// TopicInfo 是主题的元信息摘要。
type TopicInfo struct {
	Topic      string
	Partitions int32
}

// Topics 列出当前可见的全部主题及其分区数，便于运维排查。
func (c *Client) Topics(ctx context.Context) ([]TopicInfo, error) {
	listed, err := c.adm.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: 查询主题列表失败: %v", ErrTopic, err)
	}
	out := make([]TopicInfo, 0, len(listed))
	for name, detail := range listed {
		out = append(out, TopicInfo{Topic: name, Partitions: int32(len(detail.Partitions))})
	}
	return out, nil
}
