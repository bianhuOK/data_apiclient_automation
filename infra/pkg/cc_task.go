package pkg

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/avast/retry-go"
)

// 管道阶段类型定义
type TaskStage func(context.Context, <-chan any) <-chan result

type ConcurrentTask struct {
	maxConcurrency int
	maxRetries     int
	taskTimeout    time.Duration
	funcTimeout    time.Duration
}

type CTaskOptionFunc func(*ConcurrentTask)

func NewConcurrentTask(opts ...CTaskOptionFunc) *ConcurrentTask {
	ct := &ConcurrentTask{
		maxConcurrency: runtime.NumCPU(),
		maxRetries:     3,
		taskTimeout:    10 * time.Minute,
		funcTimeout:    30 * time.Second,
	}
	for _, opt := range opts {
		opt(ct)
	}
	return ct
}

// WithMaxConcurrency 设置最大并发数
func WithMaxConcurrency(n int) CTaskOptionFunc {
	return func(ct *ConcurrentTask) {
		if n > 0 {
			ct.maxConcurrency = n
		}
	}
}

// WithMaxRetries 设置最大重试次数
func WithMaxRetries(n int) func(*ConcurrentTask) {
	return func(ct *ConcurrentTask) {
		if n > 0 {
			ct.maxRetries = n
		}
	}
}

// WithTaskTimeout 设置任务总超时时间
func WithTaskTimeout(d time.Duration) func(*ConcurrentTask) {
	return func(ct *ConcurrentTask) {
		if d > 0 {
			ct.taskTimeout = d
		}
	}
}

// WithTestFuncTimeout 设置单个测试函数超时时间
func WithTestFuncTimeout(d time.Duration) func(*ConcurrentTask) {
	return func(ct *ConcurrentTask) {
		if d > 0 {
			ct.funcTimeout = d
		}
	}
}

type result struct {
	id  any
	err error
}

type testFunc func(context.Context, any) error

// 核心运行逻辑（对外暴露）
func (ct *ConcurrentTask) Run(t *testing.T, ids []any, f testFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), ct.taskTimeout)
	defer cancel()

	retryQueue := ids
	err := retry.Do(
		func() error {
			if len(retryQueue) == 0 {
				return nil
			}
			// 构建处理管道
			stage1 := ct.idWalker(ctx, retryQueue)      // Fan-out: 生成ID流
			stage2 := ct.testFuncRunner(ctx, stage1, f) // 并行处理
			failed := ct.resultCollector(ctx, stage2)   // Fan-in: 收集结果

			if len(failed) == 0 {
				return nil
			}

			retryQueue = failed
			t.Logf("重试: 还有 %d 个任务失败", len(failed))
			return fmt.Errorf("还有 %d 个任务需要重试", len(failed))
		},
		retry.Attempts(uint(ct.maxRetries)),
		retry.DelayType(retry.FixedDelay),
		retry.OnRetry(func(n uint, err error) {
			t.Logf("第 %d 次重试", n+1)
		}),
	)
	if err != nil {
		t.Fatalf("重试 %d 次后仍然失败: %v", ct.maxRetries, err)
	}
}

// Stage 1: ID生成管道（Fan-out源）
func (ct *ConcurrentTask) idWalker(ctx context.Context, ids []any) <-chan any {
	out := make(chan any, ct.maxConcurrency*10)

	go func() {
		defer close(out)
		for _, id := range ids {
			select {
			case out <- id:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

// Stage 2: 并行测试执行（Fan-out处理）
func (ct *ConcurrentTask) testFuncRunner(ctx context.Context, in <-chan any, f testFunc) <-chan result {
	out := make(chan result, ct.maxConcurrency)
	var wg sync.WaitGroup

	// 启动worker池
	wg.Add(ct.maxConcurrency)
	for i := 0; i < ct.maxConcurrency; i++ {
		go func() {
			defer wg.Done()
			for id := range in {
				select {
				case out <- ct.executeTask(ctx, id, f):
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// 管道关闭协调
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// 单个任务执行封装
func (ct *ConcurrentTask) executeTask(ctx context.Context, id any, f testFunc) result {
	ctx, cancel := context.WithTimeout(ctx, ct.funcTimeout)
	defer cancel()

	resultCh := make(chan error, 1) // need to be buffered to avoid blocking
	go func() { resultCh <- f(ctx, id) }()

	select {
	case err := <-resultCh:
		return result{id, err}
	case <-ctx.Done():
		return result{id, ctx.Err()}
	}
}

// Stage 3: 结果收集（Fan-in汇聚）
func (ct *ConcurrentTask) resultCollector(ctx context.Context, in <-chan result) []any {
	var failed []any
	var mu sync.Mutex

	for {
		select {
		case res, ok := <-in:
			if !ok {
				return failed
			}
			if res.err != nil {
				mu.Lock()
				failed = append(failed, res.id)
				mu.Unlock()
			}
		case <-ctx.Done():
			return failed
		}
	}
}
