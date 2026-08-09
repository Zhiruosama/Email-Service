package payload

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"testing"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

func TestProtectorFingerprintIsDeterministicAndKeyed(t *testing.T) {
	encryptionKey := bytes.Repeat([]byte{1}, KeySize)
	first, err := New("key-1", encryptionKey, bytes.Repeat([]byte{2}, KeySize))
	if err != nil {
		t.Fatalf("new first protector: %v", err)
	}
	second, err := New("key-1", encryptionKey, bytes.Repeat([]byte{3}, KeySize))
	if err != nil {
		t.Fatalf("new second protector: %v", err)
	}
	payload := []byte(`{"recipient":"secret@example.com"}`)
	if first.Fingerprint(payload) != first.Fingerprint(payload) {
		t.Fatal("same payload did not produce the same fingerprint")
	}
	if first.Fingerprint(payload) == second.Fingerprint(payload) {
		t.Fatal("different HMAC keys produced the same fingerprint")
	}
}

func TestProtectorSealUsesRandomizedAuthenticatedEncryption(t *testing.T) {
	key := bytes.Repeat([]byte{7}, KeySize)
	protector, err := New("key-2026-08", key, bytes.Repeat([]byte{8}, KeySize))
	if err != nil {
		t.Fatalf("new protector: %v", err)
	}
	plaintext := []byte(`{"code":"123456"}`)
	first, err := protector.Seal(context.Background(), "tenant/message", plaintext)
	if err != nil {
		t.Fatalf("seal first: %v", err)
	}
	second, err := protector.Seal(context.Background(), "tenant/message", plaintext)
	if err != nil {
		t.Fatalf("seal second: %v", err)
	}
	if bytes.Equal(first.Ciphertext, second.Ciphertext) {
		t.Fatal("two encryptions reused the same ciphertext")
	}

	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	nonceSize := aead.NonceSize()
	decrypted, err := aead.Open(
		nil,
		first.Ciphertext[:nonceSize],
		first.Ciphertext[nonceSize:],
		[]byte("tenant/message"),
	)
	if err != nil {
		t.Fatalf("decrypt sealed payload: %v", err)
	}
	if !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("decrypted payload = %q, want %q", decrypted, plaintext)
	}
	opened, err := protector.Open(context.Background(), "tenant/message", first)
	if err != nil {
		t.Fatalf("open protected payload: %v", err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatalf("opened payload = %q, want %q", opened, plaintext)
	}
	first.Ciphertext[len(first.Ciphertext)-1] ^= 0xff
	if _, err := protector.Open(context.Background(), "tenant/message", first); !errors.Is(err, ports.ErrPayloadAuthentication) {
		t.Fatalf("tampered payload error = %v, want ErrPayloadAuthentication", err)
	}
	if _, err := aead.Open(nil, first.Ciphertext[:nonceSize], first.Ciphertext[nonceSize:], []byte("other")); err == nil {
		t.Fatal("ciphertext accepted with different associated data")
	}
}

func TestProtectorOpenRejectsUnavailableKey(t *testing.T) {
	protector, err := New("current-key", bytes.Repeat([]byte{1}, KeySize), bytes.Repeat([]byte{2}, KeySize))
	if err != nil {
		t.Fatalf("new protector: %v", err)
	}
	_, err = protector.Open(context.Background(), "tenant/message", ports.ProtectedPayload{
		KeyID:      "old-key",
		Ciphertext: make([]byte, 29),
	})
	if !errors.Is(err, ports.ErrPayloadKeyUnavailable) {
		t.Fatalf("unavailable key error = %v, want ErrPayloadKeyUnavailable", err)
	}
}

func TestProtectorRejectsInvalidInputs(t *testing.T) {
	if _, err := New("key", make([]byte, 31), make([]byte, 32)); !errors.Is(err, ErrInvalidKeyMaterial) {
		t.Fatalf("key error = %v, want ErrInvalidKeyMaterial", err)
	}
	protector, err := New("key", make([]byte, 32), make([]byte, 32))
	if err != nil {
		t.Fatalf("new protector: %v", err)
	}
	if _, err := protector.Seal(context.Background(), "", []byte("x")); !errors.Is(err, ports.ErrPayloadProtection) {
		t.Fatalf("seal error = %v, want ErrPayloadProtection", err)
	}
}
