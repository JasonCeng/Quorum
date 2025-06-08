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

// 同步写入到指定节点
func (n *Node) syncWrite(key string, item DataItem) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Data[key] = item
	n.Version[key] = item.Version
	return true
}

// 异步同步剩余节点
func (c *Cluster) asyncSync(key string, item DataItem, successChan chan<- int) {
	var wg sync.WaitGroup
	for _, node := range c.Nodes {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			if n.syncWrite(key, item) {
				successChan <- 1
			}
		}(node)
	}
	wg.Wait()
	close(successChan)
}

// 写入数据：需满足写入W个副本成功
func (c *Cluster) Write(key string, value string) bool {
	version := time.Now().UnixNano()
	item := DataItem{
		Key:     key,
		Value:   value,
		Version: version,
	}

	// 第一阶段：同步写入W个节点
	success := 0
	for _, node := range c.Nodes {
		node.mu.Lock()
		node.Data[key] = item
		node.Version[key] = version
		node.mu.Unlock()
		success++

		if success >= c.W {
			break //达到Quorum写入的最低副本数W
		}
	}

	// 第二阶段：异步写入剩余N-W个节点
	successChan := make(chan int, c.N-c.W)
	go c.asyncSync(key, item, successChan)

	// 可以立即返回，不用等待异步同步节点完成
	return true
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
