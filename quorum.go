package main

import (
	"sync"
	"time"
)

// 数据条目结构
type DataItem struct {
	Key     string
	Value   string
	Version int64
}

// 节点结构
type Node struct {
	Id      int
	Data    map[string]DataItem
	Version map[string]int64 //记录每个key的最新版本
	mu      sync.Mutex
}

// 集群结构
type Cluster struct {
	Nodes []*Node
	N     int
	W     int
	R     int
}

// 写入数据：需满足写入W个副本成功
func (c *Cluster) Write(key string, value string) bool {
	version := time.Now().UnixNano()

	success := 0
	for _, node := range c.Nodes {
		node.mu.Lock()
		node.Data[key] = DataItem{
			Key:     key,
			Value:   value,
			Version: version,
		}
		node.Version[key] = version
		node.mu.Unlock()
		success++

		if success >= c.W {
			break //达到Quorum写入的最低副本数W
		}
	}

	return success >= c.W
}

// 读取数据：需满足查询R个副本，并返回最新数据
func (c *Cluster) Read(key string) (string, bool) {
	result := make([]DataItem, 0)

	for _, node := range c.Nodes {
		node.mu.Lock()
		if itme, exists := node.Data[key]; exists {
			result = append(result, itme)
		}
		node.mu.Unlock()

		if len(result) >= c.R {
			break //达到最小读取副本数R
		}
	}

	if len(result) == 0 {
		return "", false
	}

	//选取最大版本号的数据
	latest := result[0]
	for _, item := range result[1:] {
		if item.Version > latest.Version {
			latest = item
		}
	}
	return latest.Value, true
}
