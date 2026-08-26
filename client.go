package main

import (
	"context"
	"fmt"
	"time"

	qgrpc "buf.build/gen/go/parca-dev/parca/grpc/go/parca/query/v1alpha1/queryv1alpha1grpc"
	qv1 "buf.build/gen/go/parca-dev/parca/protocolbuffers/go/parca/query/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
}

func Dial(addr string, insecureTransport bool) (*Client, error) {
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
	return &Client{conn: conn, q: qgrpc.NewQueryServiceClient(conn)}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// LabelValues lists the distinct values of a label in the window. This is what
// lets the tool discover clusters instead of being told about them.
func (c *Client) LabelValues(ctx context.Context, name string, start, end time.Time) ([]string, error) {
	resp, err := c.q.Values(ctx, &qv1.ValuesRequest{
		LabelName: name,
		Start:     timestamppb.New(start),
		End:       timestamppb.New(end),
	})
	if err != nil {
		return nil, fmt.Errorf("values for label %q: %w", name, err)
	}
	return resp.GetLabelValues(), nil
}

func (c *Client) LabelNames(ctx context.Context, start, end time.Time) ([]string, error) {
	resp, err := c.q.Labels(ctx, &qv1.LabelsRequest{
		Start: timestamppb.New(start),
		End:   timestamppb.New(end),
	})
	if err != nil {
		return nil, fmt.Errorf("labels: %w", err)
	}
	return resp.GetLabelNames(), nil
}

// ProfileTypeNames returns selector-ready type strings, e.g.
// "parca_agent:samples:count:cpu:nanoseconds:delta".
func (c *Client) ProfileTypeNames(ctx context.Context) ([]string, error) {
	resp, err := c.q.ProfileTypes(ctx, &qv1.ProfileTypesRequest{})
	if err != nil {
		return nil, fmt.Errorf("profile types: %w", err)
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
