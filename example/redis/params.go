package main

import "github.com/itnxs/delaytimer"

// OrderTimeout 示例到期参数。
type OrderTimeout struct {
    OrderID string `json:"order_id"`
}

func (p *OrderTimeout) Event() delaytimer.Event {
    return "order_timeout"
}

// DemoTimeout 示例到期参数。
type DemoTimeout struct {
    Time int64 `json:"time"`
}

func (p *DemoTimeout) Event() delaytimer.Event {
    return "demo_timeout"
}
