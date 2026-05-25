package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssignerv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	s3ServiceName  = "s3"
	s3TimeFormat   = "20060102T150405Z"
	s3SignerSource = "ech0-s3-sink"
)

func signS3SinkRequest(ctx context.Context, request *http.Request, sink S3SinkConfig, payload []byte, now time.Time) error {
	payloadHash := sha256Hex(payload)
	request.Header.Set("X-Amz-Content-Sha256", payloadHash)
	request.Header.Set("X-Amz-Date", now.Format(s3TimeFormat))
	if strings.TrimSpace(sink.SessionToken) != "" {
		request.Header.Set("X-Amz-Security-Token", strings.TrimSpace(sink.SessionToken))
	}
	if strings.TrimSpace(sink.AccessKeyID) == "" || strings.TrimSpace(sink.SecretAccessKey) == "" {
		return nil
	}
	credentials := aws.Credentials{
		AccessKeyID:     strings.TrimSpace(sink.AccessKeyID),
		SecretAccessKey: strings.TrimSpace(sink.SecretAccessKey),
		SessionToken:    strings.TrimSpace(sink.SessionToken),
		Source:          s3SignerSource,
	}
	err := awssignerv4.NewSigner().SignHTTP(ctx, credentials, request, payloadHash, s3ServiceName, s3SinkRegion(sink), now)
	return wrapBroker("s3_sink_sign_failed", err, "sign s3 sink request")
}

func sha256Hex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
