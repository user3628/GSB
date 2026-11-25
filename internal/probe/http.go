package probe

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"deployverify/internal/model"
)

var sharedHTTPClient = &http.Client{
	Timeout: 0, // Per-probe context owns the independent timeout.
	Transport: &http.Transport{
		DisableKeepAlives: true,
		MaxIdleConns:      0,
	},
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

func ExecuteHTTP(parent context.Context, task Task) Result {
	method := task.Method
	if method == "" {
		method = http.MethodGet
	}
	timeout := task.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	roundDeadline, hasRoundDeadline := parent.Deadline()
	budgetCapped := hasRoundDeadline && time.Now().Add(timeout).After(roundDeadline)
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, method, task.URL, nil)
	if err != nil {
		return Result{Ref: task.Ref, Status: "failure", Code: model.CodeSpecInvalid, URL: task.URL, Detail: err.Error()}
	}
	request.Close = true
	started := time.Now()
	response, err := sharedHTTPClient.Do(request)
	latency := time.Since(started).Milliseconds()
	if err != nil {
		code := model.CodeProbeFailed
		status := "failure"
		if errors.Is(err, context.DeadlineExceeded) || isTimeoutError(err) {
			if budgetCapped {
				code = model.CodeProbeBudget
				status = "skipped"
			} else {
				code = model.CodeProbeTimeout
				status = "timeout"
			}
		} else if errors.Is(err, context.Canceled) {
			code = "E_PROBE_CANCELED"
			status = "skipped"
		}
		return Result{Ref: task.Ref, Status: status, Code: code, URL: task.URL, LatencyMS: latency, Detail: err.Error()}
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return Result{
			Ref: task.Ref, Status: "failure", Code: model.CodeProbeFailed, URL: task.URL,
			LatencyMS: latency, Detail: "HTTP " + response.Status + ": " + strings.TrimSpace(string(body)),
		}
	}
	return Result{Ref: task.Ref, Status: "success", Code: "OK", URL: task.URL, LatencyMS: latency, Detail: strings.TrimSpace(string(body))}
}

type timeoutInterface interface {
	Timeout() bool
}

func isTimeoutError(err error) bool {
	var timeoutErr timeoutInterface
	if errors.As(err, &timeoutErr) {
		return timeoutErr.Timeout()
	}
	return false
}
