package webhooks

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type vector struct {
	Secret         string `json:"secret"`
	Timestamp      int64  `json:"timestamp"`
	Now            int64  `json:"now"`
	ToleranceSecs  int64  `json:"tolerance_secs"`
	Header         string `json:"header"`
	Body           string `json:"body"`
}

func loadVector(t *testing.T) vector {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "test_vector.json"))
	if err != nil {
		t.Fatalf("read test vector: %v", err)
	}
	var v vector
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("parse test vector: %v", err)
	}
	return v
}

func TestVerifyCommittedVector(t *testing.T) {
	v := loadVector(t)
	if !VerifyWebhookSignature([]byte(v.Body), v.Header, v.Secret, v.Now) {
		t.Fatal("committed vector must verify")
	}
}

func TestRejectsTamperedBody(t *testing.T) {
	v := loadVector(t)
	if VerifyWebhookSignature([]byte(v.Body+" "), v.Header, v.Secret, v.Now) {
		t.Fatal("tampered body must not verify")
	}
}

func TestRejectsWrongSecret(t *testing.T) {
	v := loadVector(t)
	if VerifyWebhookSignature([]byte(v.Body), v.Header, "wrong-secret", v.Now) {
		t.Fatal("wrong secret must not verify")
	}
}

func TestRejectsOutsideTolerance(t *testing.T) {
	v := loadVector(t)
	if VerifyWebhookSignature([]byte(v.Body), v.Header, v.Secret, v.Timestamp+v.ToleranceSecs+1) {
		t.Fatal("stale delivery must not verify")
	}
}

func TestRejectsMalformedHeader(t *testing.T) {
	v := loadVector(t)
	if VerifyWebhookSignature([]byte(v.Body), "v1=abc", v.Secret, v.Now) {
		t.Fatal("header without timestamp must not verify")
	}
}
