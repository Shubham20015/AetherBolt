package runtime

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/shubhamjain/aetherbolt/internal/function"
)

const maxRuntimeOutputBytes = 1 << 20

var imageReference = regexp.MustCompile(`^(aetherbolt-function-[a-z0-9-]+(?::[a-zA-Z0-9._-]+|@sha256:[a-f0-9]{64})|sha256:[a-f0-9]{64})$`)

type Command struct {
	Name  string
	Args  []string
	Stdin []byte
}
type CommandResult struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int
}
type CommandRunner interface {
	Run(context.Context, Command) (CommandResult, error)
}

type DockerExecutor struct{ runner CommandRunner }

func NewDockerExecutor(runner CommandRunner) *DockerExecutor { return &DockerExecutor{runner: runner} }
func NewOSCommandRunner() CommandRunner                      { return osCommandRunner{} }

func (d *DockerExecutor) Validate(fn function.Function) error {
	if fn.Image == "" || !imageReference.MatchString(fn.Image) {
		return fmt.Errorf("%w: image must use an aetherbolt-function-* local image reference", function.ErrInvalidInput)
	}
	return nil
}

func (d *DockerExecutor) ResolveArtifact(ctx context.Context, fn function.Function) (function.Function, error) {
	inspectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := d.runner.Run(inspectCtx, Command{Name: "docker", Args: []string{"image", "inspect", "--format", "{{.Id}}", fn.Image}})
	if err != nil || result.ExitCode != 0 {
		return function.Function{}, fmt.Errorf("%w: image is not available locally", function.ErrRuntimeFailure)
	}
	artifact := strings.TrimSpace(string(result.Stdout))
	if !regexp.MustCompile(`^sha256:[a-f0-9]{64}$`).MatchString(artifact) {
		return function.Function{}, fmt.Errorf("%w: image did not resolve to a content ID", function.ErrRuntimeFailure)
	}
	fn.Image = artifact
	return fn, nil
}

func (d *DockerExecutor) Invoke(ctx context.Context, fn function.Function, event json.RawMessage) (function.ExecutionResult, error) {
	started := time.Now()
	containerName := newContainerName()
	result, err := d.runner.Run(ctx, Command{Name: "docker", Args: dockerArgs(fn, containerName), Stdin: event})
	elapsed := time.Since(started)
	if ctx.Err() != nil {
		_, _ = d.runner.Run(context.Background(), Command{Name: "docker", Args: []string{"rm", "-f", containerName}})
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return function.ExecutionResult{}, fmt.Errorf("%w: deadline exceeded", function.ErrTimedOut)
	}
	if err != nil {
		if strings.Contains(string(result.Stderr), "Error response from daemon") || strings.Contains(err.Error(), "executable file not found") {
			return function.ExecutionResult{}, fmt.Errorf("%w: container startup: %v", function.ErrRuntimeFailure, err)
		}
		return function.ExecutionResult{}, fmt.Errorf("%w: container command: %v", function.ErrFunctionFailed, err)
	}
	if result.ExitCode != 0 {
		return function.ExecutionResult{}, fmt.Errorf("%w: container exited with code %d", function.ErrFunctionFailed, result.ExitCode)
	}
	var envelope struct {
		Result            json.RawMessage `json:"result"`
		HandlerDurationMS int64           `json:"handler_duration_ms"`
	}
	if err := json.Unmarshal(result.Stdout, &envelope); err != nil || !json.Valid(envelope.Result) || envelope.HandlerDurationMS < 0 {
		return function.ExecutionResult{}, fmt.Errorf("%w: invalid runtime response", function.ErrFunctionFailed)
	}
	handlerDuration := time.Duration(envelope.HandlerDurationMS) * time.Millisecond
	if handlerDuration > elapsed {
		handlerDuration = elapsed
	}
	return function.ExecutionResult{Result: envelope.Result, InitDuration: elapsed - handlerDuration, HandlerDuration: handlerDuration, ColdStart: true}, nil
}

func dockerArgs(fn function.Function, containerName string) []string {
	return []string{"run", "--name", containerName, "--rm", "--init", "--pull", "never", "--network", "none", "--read-only", "--tmpfs", "/tmp:rw,noexec,nosuid,size=64m", "--user", "65532:65532", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--pids-limit", "64", "--memory", strconv.Itoa(fn.MemoryMB) + "m", "--cpus", fmt.Sprintf("%.3f", float64(fn.CPUMillis)/1000), "-i", fn.Image}
}

func newContainerName() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return "aetherbolt-unknown"
	}
	return "aetherbolt-" + hex.EncodeToString(bytes)
}

type osCommandRunner struct{}

func (osCommandRunner) Run(ctx context.Context, command Command) (CommandResult, error) {
	cmd := exec.CommandContext(ctx, command.Name, command.Args...)
	cmd.Stdin = bytes.NewReader(command.Stdin)
	stdout, stderr := &limitedBuffer{limit: maxRuntimeOutputBytes}, &limitedBuffer{limit: maxRuntimeOutputBytes}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	err := cmd.Run()
	result := CommandResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result.ExitCode = exitErr.ExitCode()
	}
	if stdout.exceeded || stderr.exceeded {
		return result, fmt.Errorf("runtime output exceeded %d bytes", maxRuntimeOutputBytes)
	}
	return result, err
}

type limitedBuffer struct {
	bytes.Buffer
	limit    int
	exceeded bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	remaining := b.limit - b.Len()
	if remaining <= 0 {
		b.exceeded = true
		return len(p), nil
	}
	if len(p) > remaining {
		_, _ = b.Buffer.Write(p[:remaining])
		b.exceeded = true
		return len(p), nil
	}
	return b.Buffer.Write(p)
}
