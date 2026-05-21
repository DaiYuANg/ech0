package broker

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	s3SignatureAlgorithm = "AWS4-HMAC-SHA256"
	s3ServiceName        = "s3"
	s3RequestType        = "aws4_request"
	s3DateFormat         = "20060102"
	s3TimeFormat         = "20060102T150405Z"
)

func signS3SinkRequest(request *http.Request, sink S3SinkConfig, payload []byte, now time.Time) {
	payloadHash := sha256Hex(payload)
	request.Header.Set("X-Amz-Content-Sha256", payloadHash)
	request.Header.Set("X-Amz-Date", now.Format(s3TimeFormat))
	if strings.TrimSpace(sink.SessionToken) != "" {
		request.Header.Set("X-Amz-Security-Token", strings.TrimSpace(sink.SessionToken))
	}
	if strings.TrimSpace(sink.AccessKeyID) == "" || strings.TrimSpace(sink.SecretAccessKey) == "" {
		return
	}
	date := now.Format(s3DateFormat)
	credentialScope := s3CredentialScope(date, s3SinkRegion(sink))
	signedHeaders, canonicalHeaders := s3SignedHeaders(request)
	canonical := s3CanonicalRequest(request, canonicalHeaders, signedHeaders, payloadHash)
	stringToSign := s3StringToSign(now, credentialScope, canonical)
	signature := s3Signature(sink.SecretAccessKey, date, s3SinkRegion(sink), stringToSign)
	request.Header.Set("Authorization", s3AuthorizationHeader(sink.AccessKeyID, credentialScope, signedHeaders, signature))
}

func s3CredentialScope(date, region string) string {
	return strings.Join([]string{date, region, s3ServiceName, s3RequestType}, "/")
}

func s3CanonicalRequest(request *http.Request, canonicalHeaders, signedHeaders, payloadHash string) string {
	return strings.Join([]string{
		request.Method,
		request.URL.EscapedPath(),
		request.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
}

func s3StringToSign(now time.Time, credentialScope, canonicalRequest string) string {
	return strings.Join([]string{
		s3SignatureAlgorithm,
		now.Format(s3TimeFormat),
		credentialScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")
}

func s3SignedHeaders(request *http.Request) (string, string) {
	values := map[string]string{"host": request.URL.Host}
	for key, headerValues := range request.Header {
		name := strings.ToLower(strings.TrimSpace(key))
		if name == "" || len(headerValues) == 0 {
			continue
		}
		values[name] = strings.Join(trimHeaderValues(headerValues), ",")
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	lines := make([]string, 0, len(names))
	for _, name := range names {
		lines = append(lines, name+":"+values[name]+"\n")
	}
	return strings.Join(names, ";"), strings.Join(lines, "")
}

func trimHeaderValues(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, strings.Join(strings.Fields(value), " "))
	}
	return out
}

func s3Signature(secret, date, region, stringToSign string) string {
	dateKey := hmacSHA256([]byte("AWS4"+secret), date)
	regionKey := hmacSHA256(dateKey, region)
	serviceKey := hmacSHA256(regionKey, s3ServiceName)
	signingKey := hmacSHA256(serviceKey, s3RequestType)
	return hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
}

func s3AuthorizationHeader(accessKey, scope, signedHeaders, signature string) string {
	return s3SignatureAlgorithm +
		" Credential=" + strings.TrimSpace(accessKey) + "/" + scope +
		", SignedHeaders=" + signedHeaders +
		", Signature=" + signature
}

func hmacSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	if written, err := mac.Write([]byte(value)); err != nil || written != len(value) {
		return nil
	}
	return mac.Sum(nil)
}

func sha256Hex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
