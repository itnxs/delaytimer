package delaytimer

import (
    "container/heap"
    "context"
    "sync"
    "time"
)

var _ Store = (*Memory)(nil)

// Memory 不跨进程内存储处理
type Memory struct {
    mu    sync.Mutex
    cond  *sync.Cond
    items memHeap
    byKey map[string]*memItem
    clock Clock
}

type memItem struct {
    task  Task
    index int
}

type memHeap []*memItem

func (h memHeap) Len() int { return len(h) }

func (h memHeap) Less(i, j int) bool { return h[i].task.At.Before(h[j].task.At) }

func (h memHeap) Swap(i, j int) {
    h[i], h[j] = h[j], h[i]
    h[i].index = i
    h[j].index = j
}

func (h *memHeap) Push(x any) {
    item := x.(*memItem)
    item.index = len(*h)
    *h = append(*h, item)
}

func (h *memHeap) Pop() any {
    old := *h
    n := len(old)
    item := old[n-1]
    old[n-1] = nil
    item.index = -1
    *h = old[:n-1]
    return item
}

// MemoryOption Memory 选项
type MemoryOption func(*Memory)

// WithMemoryClock 注入时钟，便于测试
func WithMemoryClock(c Clock) MemoryOption {
    return func(m *Memory) {
        if c != nil {
            m.clock = c
        }
    }
}

// NewMemory 新建内存后端
func NewMemory(opts ...MemoryOption) *Memory {
    m := &Memory{
        byKey: make(map[string]*memItem),
        clock: time.Now,
    }
    m.cond = sync.NewCond(&m.mu)
    for _, opt := range opts {
        opt(m)
    }
    return m
}

// Schedule 设置或覆盖任务
func (m *Memory) Schedule(_ context.Context, task Task) error {
    task = task.withKey()
    m.mu.Lock()
    defer m.mu.Unlock()

    if exist, ok := m.byKey[task.Key]; ok {
        exist.task = task
        heap.Fix(&m.items, exist.index)
        m.cond.Broadcast()
        return nil
    }

    m.pushLocked(task)
    return nil
}

// Cancel 取消尚未领取的任务
func (m *Memory) Cancel(_ context.Context, key string) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    item, ok := m.byKey[key]
    if !ok {
        return nil
    }

    heap.Remove(&m.items, item.index)
    delete(m.byKey, key)
    m.cond.Broadcast()
    return nil
}

// Claim 领取已到期任务，若无到期任务则等待 ctx 取消
func (m *Memory) Claim(ctx context.Context, n int) ([]Task, error) {
    if n < 1 {
        n = 1
    }

    stop := make(chan struct{})
    defer close(stop)
    go func() {
        select {
        case <-ctx.Done():
            m.cond.Broadcast()
        case <-stop:
        }
    }()

    m.mu.Lock()
    defer m.mu.Unlock()

    for {
        if err := ctx.Err(); err != nil {
            return nil, err
        }

        now := m.clock()
        var out []Task
        for len(out) < n && m.items.Len() > 0 {
            head := m.items[0]
            if head.task.At.After(now) {
                break
            }
            item := heap.Pop(&m.items).(*memItem)
            delete(m.byKey, item.task.Key)
            out = append(out, item.task)
        }
        if len(out) > 0 {
            return out, nil
        }

        wait := m.waitDuration(now)
        if wait <= 0 {
            m.cond.Wait()
            continue
        }

        timer := time.AfterFunc(wait, func() { m.cond.Broadcast() })
        m.cond.Wait()
        timer.Stop()
    }
}

func (m *Memory) waitDuration(now time.Time) time.Duration {
    if m.items.Len() == 0 {
        return 0
    }
    at := m.items[0].task.At
    if !at.After(now) {
        return 0
    }
    d := at.Sub(now)
    if d > time.Second {
        return time.Second
    }
    return d
}

// Ack 内存后端在 Claim 时已移除任务
func (m *Memory) Ack(context.Context, Task) error { return nil }

// Fail 重新入队；若同 Key 已被新 Schedule 覆盖则不改写
func (m *Memory) Fail(_ context.Context, task Task) error {
    task = task.withKey()
    m.mu.Lock()
    defer m.mu.Unlock()
    if _, exists := m.byKey[task.Key]; exists {
        return nil
    }
    task.At = m.clock().Add(failRequeueDelay)
    m.pushLocked(task)
    return nil
}

func (m *Memory) pushLocked(task Task) {
    item := &memItem{task: task}
    heap.Push(&m.items, item)
    m.byKey[task.Key] = item
    m.cond.Broadcast()
}
