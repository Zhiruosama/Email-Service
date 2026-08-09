// Package payload protects sensitive submission data at the persistence edge.
package payload

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Zhiruosama/Email-Service/internal/application/ports"
)

const KeySize = 32

var ErrInvalidKeyMaterial = errors.New("invalid payload key material")

type Protector struct {
	keyID          string
	aead           cipher.AEAD
	fingerprintKey [KeySize]byte
	random         io.Reader
}

var _ ports.PayloadProtector = (*Protector)(nil)

func New(keyID string, encryptionKey, fingerprintKey []byte) (*Protector, error) {
	return newWithRandom(keyID, encryptionKey, fingerprintKey, rand.Reader)
}

func newWithRandom(
	keyID string,
	encryptionKey, fingerprintKey []byte,
	random io.Reader,
) (*Protector, error) {
	if strings.TrimSpace(keyID) == "" || len(keyID) > 128 {
		return nil, fmt.Errorf("%w: key id must contain 1..128 bytes", ErrInvalidKeyMaterial)
	}
	if len(encryptionKey) != KeySize || len(fingerprintKey) != KeySize {
		return nil, fmt.Errorf("%w: encryption and fingerprint keys must be 32 bytes", ErrInvalidKeyMaterial)
	}
	if random == nil {
		return nil, fmt.Errorf("%w: random source is required", ErrInvalidKeyMaterial)
	}
	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return nil, fmt.Errorf("%w: initialize AES", ErrInvalidKeyMaterial)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: initialize GCM", ErrInvalidKeyMaterial)
	}
	protector := &Protector{keyID: keyID, aead: aead, random: random}
	copy(protector.fingerprintKey[:], fingerprintKey)
	return protector, nil
}

func (p *Protector) Fingerprint(plaintext []byte) [32]byte {
	mac := hmac.New(sha256.New, p.fingerprintKey[:])
	_, _ = mac.Write([]byte("mail-service/submission-fingerprint/v1\x00"))
	_, _ = mac.Write(plaintext)
	var result [32]byte
	copy(result[:], mac.Sum(nil))
	return result
}

func (p *Protector) Seal(
	ctx context.Context,
	associatedData string,
	plaintext []byte,
) (ports.ProtectedPayload, error) {
	if err := ctx.Err(); err != nil {
		return ports.ProtectedPayload{}, err
	}
	if associatedData == "" || len(plaintext) == 0 {
		return ports.ProtectedPayload{}, fmt.Errorf("%w: associated data and plaintext are required", ports.ErrPayloadProtection)
	}
	nonce := make([]byte, p.aead.NonceSize())
	if _, err := io.ReadFull(p.random, nonce); err != nil {
		return ports.ProtectedPayload{}, fmt.Errorf("%w: generate nonce", ports.ErrPayloadProtection)
	}
	ciphertext := make([]byte, len(nonce))
	copy(ciphertext, nonce)
	ciphertext = p.aead.Seal(ciphertext, nonce, plaintext, []byte(associatedData))
	result := ports.ProtectedPayload{KeyID: p.keyID, Ciphertext: ciphertext}
	if err := result.Validate(); err != nil {
		return ports.ProtectedPayload{}, err
	}
	return result, nil
}
