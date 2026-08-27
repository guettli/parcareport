package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	qgrpc "buf.build/gen/go/parca-dev/parca/grpc/go/parca/query/v1alpha1/queryv1alpha1grpc"
	qv1 "buf.build/gen/go/parca-dev/parca/protocolbuffers/go/parca/query/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Client wraps the Parca QueryService.
//
// Parca multiplexes gRPC and its web UI on a single port and routes by
// Content-Type, so a plain JSON POST silently returns the UI's HTML instead of
// an error. We speak real gRPC, which is also what parca-agent uses to write.
type Client struct {
	conn *grpc.ClientConn
	q    qgrpc.QueryServiceClient
	// timeout bounds every metadata query (Labels, Values, ProfileTypes).
	// The merge queries take their deadline from the caller, which already
	// applies --timeout per group; these had no deadline of their own at all
	// and inherited only the process-wide one, so a stalled label lookup held
	// the whole run for ten minutes before failing as something else.
	timeout time.Duration
}

func Dial(addr string, insecureTransport bool, timeout time.Duration) (*Client, error) {
	var opts []grpc.DialOption
	if insecureTransport {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		return nil, fmt.Errorf("TLS transport not implemented yet; pass --insecure")
	}
	// Merged profiles over a long window can exceed the 4MiB default.
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256<<20)))

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &Client{conn: conn, q: qgrpc.NewQueryServiceClient(conn), timeout: timeout}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// meta bounds one metadata query. A non-positive timeout disables the bound,
// which is what the zero-value Client in tests wants.
func (c *Client) meta(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.timeout)
}

// metaErr annotates a metadata failure with what to do about it.
//
// These queries are cheap and usually instant, so a deadline here nearly
// always means the server is unwell rather than the window being too large --
// and it is retryable, which the bare gRPC text does not say.
func (c *Client) metaErr(what string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		return fmt.Errorf("%s: timed out after %s -- the server is slow or unreachable; "+
			"retry, or raise --timeout: %w", what, c.timeout, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}

// LabelValues lists the distinct values of a label in the window. This is what
// lets the tool discover clusters instead of being told about them.
func (c *Client) LabelValues(ctx context.Context, name string, start, end time.Time) ([]string, error) {
	qctx, cancel := c.meta(ctx)
	defer cancel()
	resp, err := c.q.Values(qctx, &qv1.ValuesRequest{
		LabelName: name,
		Start:     timestamppb.New(start),
		End:       timestamppb.New(end),
	})
	if err != nil {
		return nil, c.metaErr(fmt.Sprintf("values for label %q", name), err)
	}
	return resp.GetLabelValues(), nil
}

func (c *Client) LabelNames(ctx context.Context, start, end time.Time) ([]string, error) {
	qctx, cancel := c.meta(ctx)
	defer cancel()
	resp, err := c.q.Labels(qctx, &qv1.LabelsRequest{
		Start: timestamppb.New(start),
		End:   timestamppb.New(end),
	})
	if err != nil {
		return nil, c.metaErr("labels", err)
	}
	return resp.GetLabelNames(), nil
}

// ProfileTypeNames returns selector-ready type strings, e.g.
// "parca_agent:samples:count:cpu:nanoseconds:delta".
func (c *Client) ProfileTypeNames(ctx context.Context) ([]string, error) {
	qctx, cancel := c.meta(ctx)
	defer cancel()
	resp, err := c.q.ProfileTypes(qctx, &qv1.ProfileTypesRequest{})
	if err != nil {
		return nil, c.metaErr("profile types", err)
	}
	out := make([]string, 0, len(resp.GetTypes()))
	for _, t := range resp.GetTypes() {
		s := fmt.Sprintf("%s:%s:%s:%s:%s",
			t.GetName(), t.GetSampleType(), t.GetSampleUnit(), t.GetPeriodType(), t.GetPeriodUnit())
		if t.GetDelta() {
			s += ":delta"
		}
		out = append(out, s)
	}
	return out, nil
}

// MergePprof asks the server to merge every profile matching the selector over
// [start,end] and hand back a standard pprof. Doing the merge server-side is
// the whole reason this tool stays small: all local analysis is plain pprof.
func (c *Client) MergePprof(ctx context.Context, selector string, start, end time.Time) ([]byte, error) {
	resp, err := c.q.Query(ctx, &qv1.QueryRequest{
		Mode:       qv1.QueryRequest_MODE_MERGE,
		ReportType: qv1.QueryRequest_REPORT_TYPE_PPROF,
		Options: &qv1.QueryRequest_Merge{
			Merge: &qv1.MergeProfile{
				Query: selector,
				Start: timestamppb.New(start),
				End:   timestamppb.New(end),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("merge query %q: %w", selector, err)
	}
	return resp.GetPprof(), nil
}
