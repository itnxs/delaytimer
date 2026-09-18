package main

import "gitlab.haochang.tv/gopkg/delaytimer"

// OrderTimeout 示例到期参数。
type OrderTimeout struct {
    OrderID string `json:"order_id"`
}

func (p *OrderTimeout) Event() delaytimer.Event {
    return "order_timeout"
}
