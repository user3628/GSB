package authquota

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
)

const idempotencyPayloadVersion byte = 1

func idempotencyKey(secret []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte("openplatform/idempotency/aes-gcm/v1\x00"))
	h.Write(secret)
	var key [32]byte
	copy(key[:], h.Sum(nil))
	return key
}

// sealIdempotencyPayload uses a deterministic nonce derived from the
// plaintext. The ciphertext is stored only in the local idempotency cache; it
// keeps first-response replay byte-identical across runs without introducing a
// nondeterministic random source.
func sealIdempotencyPayload(secret, plaintext []byte) ([]byte, error) {
	key := idempotencyKey(secret)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha256.New, key[:])
	mac.Write(plaintext)
	nonce := mac.Sum(nil)[:gcm.NonceSize()]
	sealed := gcm.Seal(nil, nonce, plaintext, []byte("openplatform-idempotency-v1"))
	payload := make([]byte, 1+len(nonce)+len(sealed))
	payload[0] = idempotencyPayloadVersion
	copy(payload[1:], nonce)
	copy(payload[1+len(nonce):], sealed)
	return payload, nil
}

func openIdempotencyPayload(secret, payload []byte) ([]byte, error) {
	key := idempotencyKey(secret)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(payload) < 1+gcm.NonceSize()+gcm.Overhead() || payload[0] != idempotencyPayloadVersion {
		return nil, fmt.Errorf("unsupported idempotency payload encoding")
	}
	nonce := payload[1 : 1+gcm.NonceSize()]
	return gcm.Open(nil, nonce, payload[1+gcm.NonceSize():], []byte("openplatform-idempotency-v1"))
}
