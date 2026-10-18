package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/shubhamjain/aetherbolt/internal/function"
)

type fakeRunner struct {
	command Command
	result  CommandResult
	err     error
}

func (r *fakeRunner) Run(_ context.Context, command Command) (CommandResult, error) {
	r.command = command
	return r.result, r.err
}

func containerFunction() function.Function {
	return function.Function{Runtime: function.RuntimeContainer, Image: "aetherbolt-function-echo:dev", MemoryMB: 128, CPUMillis: 250}
}

func TestDockerExecutorBuildsConstrainedCommandAndParsesResponse(t *testing.T) {
	runner := &fakeRunner{result: CommandResult{Stdout: []byte(`{"result":{"ok":true},"handler_duration_ms":0}`)}}
	executor := NewDockerExecutor(runner)
	result, err := executor.Invoke(context.Background(), containerFunction(), []byte(`{"message":"hello"}`))
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !result.ColdStart || string(result.Result) != `{"ok":true}` {
		t.Fatalf("unexpected result: %#v", result)
	}
	arguments := strings.Join(runner.command.Args, " ")
	for _, expected := range []string{"--network none", "--read-only", "--user 65532:65532", "--cap-drop ALL", "no-new-privileges", "--pids-limit 64", "--memory 128m", "--cpus 0.250", "--pull never"} {
		if !strings.Contains(arguments, expected) {
			t.Errorf("missing constrained runtime argument %q in %q", expected, arguments)
		}
	}
}

func TestDockerExecutorClassifiesFailures(t *testing.T) {
	tests := []struct {
		name   string
		runner fakeRunner
		want   error
	}{
		{name: "function exit", runner: fakeRunner{result: CommandResult{ExitCode: 2}}, want: function.ErrFunctionFailed},
		{name: "daemon startup", runner: fakeRunner{result: CommandResult{Stderr: []byte("docker: Error response from daemon: image not found")}, err: errors.New("exit status 125")}, want: function.ErrRuntimeFailure},
		{name: "invalid envelope", runner: fakeRunner{result: CommandResult{Stdout: []byte("not-json")}}, want: function.ErrFunctionFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := NewDockerExecutor(&tt.runner)
			_, err := executor.Invoke(context.Background(), containerFunction(), []byte(`{}`))
			if !errors.Is(err, tt.want) {
				t.Fatalf("expected %v, got %v", tt.want, err)
			}
		})
	}
}

func TestDockerExecutorHonorsDeadline(t *testing.T) {
	runner := &deadlineRunner{}
	executor := NewDockerExecutor(runner)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	_, err := executor.Invoke(ctx, containerFunction(), []byte(`{}`))
	if !errors.Is(err, function.ErrTimedOut) {
		t.Fatalf("expected timeout, got %v", err)
	}
	if !runner.cleanedUp {
		t.Fatal("expected timeout cleanup to force-remove the named container")
	}
}

type deadlineRunner struct{ cleanedUp bool }

func (r *deadlineRunner) Run(ctx context.Context, command Command) (CommandResult, error) {
	if len(command.Args) > 0 && command.Args[0] == "rm" {
		r.cleanedUp = true
		return CommandResult{}, nil
	}
	<-ctx.Done()
	return CommandResult{}, ctx.Err()
}
