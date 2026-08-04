// Package interceptors holds the shared gRPC server middleware: panic
// recovery, typed-error mapping, and per-client rate limiting. Controllers
// compose them; transport-status mapping lives here and nowhere else.
package interceptors

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/KFN002/perfect-go-service/pkg/apperrors"
	"github.com/KFN002/perfect-go-service/pkg/ratelimit"
)

// Recovery converts panics into codes.Internal instead of killing the server.
func Recovery(log *zap.Logger) grpc.UnaryServerInterceptor {
	//nolint:nonamedreturns // the deferred recover must overwrite the returns
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic recovered", zap.Any("panic", r), zap.String("method", info.FullMethod))
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

// StatusFromError converts a typed error into a gRPC status — the single
// place the taxonomy meets the transport. Both the interceptor chain (network
// gRPC) and the in-process gateway servers (which bypass interceptors) call
// this, so REST and gRPC can never disagree on status mapping.
func StatusFromError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok && apperrors.CodeOf(err) == apperrors.CodeInternal {
		return err // already a gRPC status
	}
	return status.Error(grpcCode(apperrors.CodeOf(err)), err.Error())
}

// MapErrors applies StatusFromError to every unary response.
func MapErrors() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			return nil, StatusFromError(err)
		}
		return resp, nil
	}
}

func grpcCode(c apperrors.Code) codes.Code {
	switch c {
	case apperrors.CodeInvalidInput, apperrors.CodeDivisionByZero:
		return codes.InvalidArgument
	case apperrors.CodeNotFound:
		return codes.NotFound
	case apperrors.CodeConflict:
		return codes.AlreadyExists
	case apperrors.CodeRateLimited, apperrors.CodeOverloaded:
		return codes.ResourceExhausted
	case apperrors.CodeUnavailable:
		return codes.Unavailable
	default:
		return codes.Internal
	}
}

// RateLimit throttles per client key (x-forwarded-for behind nginx, else the
// peer address) — the gRPC layer of the defense in depth.
func RateLimit(limiter *ratelimit.Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (any, error) {
		if !limiter.Allow(clientKey(ctx)) {
			return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded, retry later")
		}
		return handler(ctx, req)
	}
}

func clientKey(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if xff := md.Get("x-forwarded-for"); len(xff) > 0 && xff[0] != "" {
			return xff[0]
		}
	}
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return "unknown"
}

// Chain composes the standard interceptor stack in the right order.
func Chain(log *zap.Logger, limiter *ratelimit.Limiter) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(
		Recovery(log),
		RateLimit(limiter),
		MapErrors(),
	)
}
