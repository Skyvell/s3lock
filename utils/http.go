package utils

import (
	"context"
	"fmt"

	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func SetHttpHeaders(headers map[string]string) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Serialize.Insert(middleware.SerializeMiddlewareFunc("SetHttpHeadersMiddleware", func(
			ctx context.Context, in middleware.SerializeInput, next middleware.SerializeHandler,
		) (
			middleware.SerializeOutput, middleware.Metadata, error,
		) {
			req, ok := in.Request.(*smithyhttp.Request)
			if !ok {
				return middleware.SerializeOutput{}, middleware.Metadata{}, fmt.Errorf("unknown request type %T", in.Request)
			}

			for key, value := range headers {
				req.Header.Add(key, value)
			}

			return next.HandleSerialize(ctx, in)
		}), "OperationSerializer", middleware.Before)
	}
}
