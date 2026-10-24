package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// requestKey is the context key for the request id. An empty struct type of
// our own cannot collide with a key from any other package.
type requestKey struct{}

// gen must send start, start+step, start+2*step, ... on the returned channel
// and close it as soon as ctx is done, even while it is blocked on a send.
func gen(ctx context.Context, start, step int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for v := start; ; v += step {
			select {
			case <-ctx.Done():
				return
			case out <- v:
			}
		}
	}()
	return out
}

// requestID must return the id stored in ctx under requestKey{}, and whether
// the context carried one at all.
func requestID(ctx context.Context) (string, bool) {
	// TODO: read the value
	id, ok := ctx.Value(requestKey{}).(string)
	return id, ok
}

// ---- scaffolding: scenario runner ----

func take(ch <-chan int, k int) []int {
	var got []int
	limit := time.After(2 * time.Second)
	for len(got) < k {
		select {
		case v, ok := <-ch:
			if !ok {
				return got
			}
			got = append(got, v)
		case <-limit:
			return got
		}
	}
	return got
}

// settle gives the generator 40 ms to react, then reports whether its goroutine
// is still alive and whether its channel was closed.
func settle(ch <-chan int, before int) (closed, leaked bool) {
	time.Sleep(40 * time.Millisecond)
	leaked = runtime.NumGoroutine() > before
	select {
	case _, ok := <-ch:
		closed = !ok
	default:
	}
	return closed, leaked
}

func report(got []int, ctx context.Context, closed, leaked bool) {
	line := "values:"
	for _, v := range got {
		line += " " + strconv.Itoa(v)
	}
	fmt.Println(line)
	fmt.Println("err:", ctx.Err())
	fmt.Println("closed:", closed)
	fmt.Println("leaked:", leaked)
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func runCancel(k, start, step int) {
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	ch := gen(ctx, start, step)
	got := take(ch, k)
	time.Sleep(10 * time.Millisecond) // let gen block on its next send
	cancel()
	closed, leaked := settle(ch, before)
	fmt.Println("cancel")
	report(got, ctx, closed, leaked)
}

func runTimeout(ms int) {
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ms)*time.Millisecond)
	defer cancel()
	ch := gen(ctx, 1, 1)
	got := take(ch, 2)
	<-ctx.Done()
	closed, leaked := settle(ch, before)
	fmt.Println("timeout")
	report(got, ctx, closed, leaked)
}

func runValue(id string, k, start, step int) {
	before := runtime.NumGoroutine()
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	if id != "-" {
		parent = context.WithValue(parent, requestKey{}, id)
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	rid, ok := requestID(ctx)
	ch := gen(ctx, start, step)
	got := take(ch, k)
	time.Sleep(10 * time.Millisecond) // let gen block on its next send
	cancelParent()
	closed, leaked := settle(ch, before)
	fmt.Println("value")
	if ok {
		fmt.Println("request:", rid)
	} else {
		fmt.Println("request: none")
	}
	report(got, ctx, closed, leaked)
}

func main() {
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) == 0 {
			continue
		}
		switch f[0] {
		case "cancel":
			runCancel(atoi(f[1]), atoi(f[2]), atoi(f[3]))
		case "timeout":
			runTimeout(atoi(f[1]))
		case "value":
			runValue(f[1], atoi(f[2]), atoi(f[3]), atoi(f[4]))
		}
	}
}
