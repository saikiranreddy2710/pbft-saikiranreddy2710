package utils

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"fmt"
)

// Generate RSA Key pair
func GenerateKeys() (*rsa.PrivateKey, *rsa.PublicKey, error) {
	// Generate a 2048-bit RSA key pair
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	return privateKey, &privateKey.PublicKey, nil
}

// Sign a message using RSA private key
func SignMessage(privateKey *rsa.PrivateKey, message []byte) ([]byte, error) {
	// Hash the message using SHA-256
	hash := sha256.Sum256(message)
	// Sign the hashed message using the private key
	return rsa.SignPKCS1v15(rand.Reader, privateKey, 0, hash[:])
}

// Verify a signature using RSA public key
func VerifySignature(publicKey *rsa.PublicKey, message, signature []byte) error {
	// Hash the message using SHA-256
	hash := sha256.Sum256(message)
	// Verify the signature using the public key
	err := rsa.VerifyPKCS1v15(publicKey, 0, hash[:], signature)
	if err != nil {
		return fmt.Errorf("signature verification failed: %v", err)
	}
	return nil
}

// Print public key (for debugging purposes)
func PrintPublicKey(publicKey *rsa.PublicKey) {
	fmt.Printf("Public Key: %+v\n", publicKey)
}

// Load public key (can be extended for actual key loading if needed)
func LoadPublicKey() *rsa.PublicKey {
	// In real scenarios, the public key would be read from a file or a secure source
	// Placeholder: return a dummy public key for testing
	return &rsa.PublicKey{
		// Placeholder modulus and exponent
		// In practice, you should load actual values here
		N: nil,   // Replace with the actual public key modulus
		E: 65537, // Common public exponent
	}
}

// Load private key (can be extended for actual key loading if needed)
func LoadPrivateKey() *rsa.PrivateKey {
	// In real scenarios, the private key would be loaded from a secure source
	// Placeholder: return a dummy private key for testing
	return &rsa.PrivateKey{
		// Placeholder private key fields
		PublicKey: rsa.PublicKey{
			N: nil,   // Replace with actual public key modulus
			E: 65537, // Common public exponent
		},
		D: nil, // Replace with actual private exponent
	}
}
