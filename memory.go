package delaytimer

import (
    "container/heap"
    "context"
    "sync"
    "time"
)

const memoryWakeInterval = time.Second

var _ Store = (*Memory)(nil)

// Memory 进程内任务存储，不跨进程。应用服务，对外实现 Store，对内委托给日程聚合。
type Memory struct {
    clock  Clock
    agenda *memoryAgenda
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
        clock:  time.Now,
        agenda: newMemoryAgenda(),
    }
    for _, opt := range opts {
        opt(m)
    }
    return m
}

// Schedule 设置或覆盖任务
func (m *Memory) Schedule(_ context.Context, task Task) error {
    m.agenda.upsert(task)
    return nil
}

// Cancel 取消尚未领取的任务
func (m *Memory) Cancel(_ context.Context, key string) error {
    m.agenda.remove(key)
    return nil
}

// Claim 领取已到期任务，若无到期任务则等待 ctx 取消
func (m *Memory) Claim(ctx context.Context, n int) ([]Task, error) {
    if n < 1 {
        n = 1
    }
    return m.agenda.claim(ctx, n, m.clock)
}

// Ack 内存后端在 Claim 时已移除任务
func (m *Memory) Ack(context.Context, Task) error { return nil }

// Fail 重新入队；若同 Key 已被新 Schedule 覆盖则不改写
func (m *Memory) Fail(_ context.Context, task Task) error {
    m.agenda.requeueUnlessPresent(task, m.clock().Add(failRequeueDelay))
    return nil
}

// memoryAgenda 待办日程聚合：按到期时间维护已调度任务，并负责领取时的阻塞等待。
type memoryAgenda struct {
    mu    sync.Mutex
    cond  *sync.Cond
    queue *dueQueue
    index map[string]*scheduledTask
}

func newMemoryAgenda() *memoryAgenda {
    a := &memoryAgenda{
        queue: newDueQueue(),
        index: make(map[string]*scheduledTask),
    }
    a.cond = sync.NewCond(&a.mu)
    return a
}

func (a *memoryAgenda) upsert(task Task) {
    a.mu.Lock()
    defer a.mu.Unlock()

    if exist, ok := a.index[task.Key]; ok {
        exist.replace(task)
        a.queue.fix(exist)
        a.notify()
        return
    }
    a.insertLocked(task)
}

func (a *memoryAgenda) remove(key string) {
    a.mu.Lock()
    defer a.mu.Unlock()

    item, ok := a.index[key]
    if !ok {
        return
    }
    a.queue.remove(item)
    delete(a.index, key)
    a.notify()
}

func (a *memoryAgenda) requeueUnlessPresent(task Task, at time.Time) {
    a.mu.Lock()
    defer a.mu.Unlock()

    if a.contains(task.Key) {
        return
    }
    task.At = at
    a.insertLocked(task)
}

func (a *memoryAgenda) claim(ctx context.Context, n int, clock Clock) ([]Task, error) {
    stop := a.watch(ctx)
    defer stop()

    a.mu.Lock()
    defer a.mu.Unlock()

    for {
        if err := ctx.Err(); err != nil {
            return nil, err
        }
        if due := a.takeDue(clock(), n); len(due) > 0 {
            return due, nil
        }
        a.await(a.waitDuration(clock()))
    }
}

func (a *memoryAgenda) contains(key string) bool {
    _, ok := a.index[key]
    return ok
}

func (a *memoryAgenda) insertLocked(task Task) {
    item := newScheduledTask(task)
    a.queue.push(item)
    a.index[item.key()] = item
    a.notify()
}

func (a *memoryAgenda) takeDue(now time.Time, n int) []Task {
    var out []Task
    for len(out) < n {
        head := a.queue.peek()
        if head == nil || !head.due(now) {
            break
        }
        item := a.queue.pop()
        delete(a.index, item.key())
        out = append(out, item.snapshot())
    }
    return out
}

func (a *memoryAgenda) waitDuration(now time.Time) time.Duration {
    head := a.queue.peek()
    if head == nil || head.due(now) {
        return 0
    }
    d := head.dueAt().Sub(now)
    if d > memoryWakeInterval {
        return memoryWakeInterval
    }
    return d
}

func (a *memoryAgenda) watch(ctx context.Context) func() {
    stop := make(chan struct{})
    go func() {
        select {
        case <-ctx.Done():
            a.notify()
        case <-stop:
        }
    }()
    return func() { close(stop) }
}

func (a *memoryAgenda) await(wait time.Duration) {
    if wait <= 0 {
        a.cond.Wait()
        return
    }
    timer := time.AfterFunc(wait, a.notify)
    a.cond.Wait()
    timer.Stop()
}

func (a *memoryAgenda) notify() {
    a.cond.Broadcast()
}

// scheduledTask 日程中的任务实体，身份为 Task.Key。
type scheduledTask struct {
    task  Task
    index int
}

func newScheduledTask(task Task) *scheduledTask {
    return &scheduledTask{task: task, index: -1}
}

func (t *scheduledTask) key() string { return t.task.Key }

func (t *scheduledTask) dueAt() time.Time { return t.task.At }

func (t *scheduledTask) due(now time.Time) bool { return !t.task.At.After(now) }

func (t *scheduledTask) replace(task Task) { t.task = task }

func (t *scheduledTask) snapshot() Task { return t.task }

// dueQueue 按到期时间排序的优先队列。
type dueQueue struct {
    items []*scheduledTask
}

func newDueQueue() *dueQueue {
    return &dueQueue{}
}

func (q *dueQueue) Len() int { return len(q.items) }

func (q *dueQueue) Less(i, j int) bool {
    return q.items[i].dueAt().Before(q.items[j].dueAt())
}

func (q *dueQueue) Swap(i, j int) {
    q.items[i], q.items[j] = q.items[j], q.items[i]
    q.items[i].index = i
    q.items[j].index = j
}

func (q *dueQueue) Push(x any) {
    item := x.(*scheduledTask)
    item.index = len(q.items)
    q.items = append(q.items, item)
}

func (q *dueQueue) Pop() any {
    n := len(q.items)
    item := q.items[n-1]
    q.items[n-1] = nil
    item.index = -1
    q.items = q.items[:n-1]
    return item
}

func (q *dueQueue) peek() *scheduledTask {
    if len(q.items) == 0 {
        return nil
    }
    return q.items[0]
}

func (q *dueQueue) push(item *scheduledTask) {
    heap.Push(q, item)
}

func (q *dueQueue) pop() *scheduledTask {
    return heap.Pop(q).(*scheduledTask)
}

func (q *dueQueue) remove(item *scheduledTask) {
    heap.Remove(q, item.index)
}

func (q *dueQueue) fix(item *scheduledTask) {
    heap.Fix(q, item.index)
}
