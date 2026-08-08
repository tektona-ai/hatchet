package token

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/tink-crypto/tink-go/jwt"

	"github.com/hatchet-dev/hatchet/pkg/encryption"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/cache"
)

type JWTManager interface {
	GenerateTenantToken(ctx context.Context, tenantId uuid.UUID, name string, internal bool, expires *time.Time) (*Token, error)
	ValidateTenantToken(ctx context.Context, token string) (uuid.UUID, uuid.UUID, error)
}

type TokenOpts struct {
	Issuer               string
	Audience             string
	ServerURL            string
	GRPCBroadcastAddress string
}

// verifySignatureCacheDuration is how long a token's *signature* verification
// is reused. It is deliberately short and independent of the token's own
// lifetime: the point is to collapse the repeated verification of the same
// token across a burst of requests, not to remember tokens.
const verifySignatureCacheDuration = 5 * time.Second

// verifiedToken is what survives in the signature cache. It holds only claims
// that are fixed for the life of the token, never anything revocable — see
// ValidateTenantToken for why that distinction is the whole design.
type verifiedToken struct {
	tenantId uuid.UUID
	tokenId  uuid.UUID
}

type jwtManagerImpl struct {
	encryption encryption.EncryptionService
	opts       *TokenOpts
	tokenRepo  v1.APITokenRepository
	verifier   jwt.Verifier

	// verifyCache memoizes signature verification, keyed by a digest of the
	// token rather than the token itself so that a heap dump of the cache does
	// not hand out usable credentials.
	verifyCache cache.Cacheable
}

func NewJWTManager(encryptionSvc encryption.EncryptionService, tokenRepo v1.APITokenRepository, opts *TokenOpts) (JWTManager, error) {
	verifier, err := jwt.NewVerifier(encryptionSvc.GetPublicJWTHandle())

	if err != nil {
		return nil, fmt.Errorf("failed to create JWT Verifier: %v", err)
	}

	return &jwtManagerImpl{
		encryption:  encryptionSvc,
		opts:        opts,
		tokenRepo:   tokenRepo,
		verifier:    verifier,
		verifyCache: cache.New(verifySignatureCacheDuration),
	}, nil
}

type Token struct {
	TokenId   uuid.UUID
	ExpiresAt time.Time
	Token     string
}

func (j *jwtManagerImpl) createToken(ctx context.Context, tenantId uuid.UUID, name string, id *uuid.UUID, expires *time.Time) (*Token, error) {
	// Retrieve the JWT Signer primitive from privateKeysetHandle.
	signer, err := jwt.NewSigner(j.encryption.GetPrivateJWTHandle())

	if err != nil {
		return nil, fmt.Errorf("failed to create JWT Signer: %v", err)
	}

	tokenId, expiresAt, opts := j.getJWTOptionsForTenant(tenantId, id, expires)

	rawJWT, err := jwt.NewRawJWT(opts)

	if err != nil {
		return nil, fmt.Errorf("failed to create raw JWT: %v", err)
	}

	token, err := signer.SignAndEncode(rawJWT)

	if err != nil {
		return nil, fmt.Errorf("failed to sign and encode JWT: %v", err)
	}

	return &Token{
		TokenId:   tokenId,
		ExpiresAt: expiresAt,
		Token:     token,
	}, nil
}

func (j *jwtManagerImpl) GenerateTenantToken(ctx context.Context, tenantId uuid.UUID, name string, internal bool, expires *time.Time) (*Token, error) {
	token, err := j.createToken(ctx, tenantId, name, nil, expires)
	if err != nil {
		return nil, err
	}

	// write the token to the database
	_, err = j.tokenRepo.CreateAPIToken(ctx, &v1.CreateAPITokenOpts{
		ID:        token.TokenId,
		ExpiresAt: token.ExpiresAt,
		TenantId:  &tenantId,
		Name:      &name,
		Internal:  internal,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to write token to database: %v", err)
	}

	return token, nil
}

// ValidateTenantToken verifies a token's signature and confirms it is still
// live, returning the tenant and token IDs.
//
// The two halves are cached differently on purpose. Signature verification is
// an ECDSA operation over claims that cannot change once signed, so its result
// is memoized for a few seconds — under load the same token arrives on every
// request and re-verifying it was measured at roughly 8% of engine CPU. The
// revocation and expiry checks below are NOT part of that memo: they run on
// every call, so revoking a token takes effect as quickly as it always did
// (bounded by the API token repository's own cache, not by this one).
//
// Getting that split wrong would be a security bug rather than a slow path, so
// nothing revocable may be added to verifiedToken.
func (j *jwtManagerImpl) ValidateTenantToken(ctx context.Context, token string) (tenantId uuid.UUID, tokenUUID uuid.UUID, err error) {
	verified, err := j.verifySignature(token)

	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}

	// Read the token from the database and make sure it's not revoked. This is
	// intentionally outside the signature cache.
	dbToken, err := j.tokenRepo.GetAPITokenById(ctx, verified.tokenId)

	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("failed to read token from database: %v", err)
	}

	if dbToken.Revoked {
		return uuid.Nil, uuid.Nil, fmt.Errorf("token has been revoked")
	}

	if expiresAt := dbToken.ExpiresAt.Time; expiresAt.Before(time.Now().UTC()) {
		return uuid.Nil, uuid.Nil, fmt.Errorf("token has expired")
	}

	return verified.tenantId, dbToken.ID, nil
}

// verifySignature checks the token's signature and claims, memoizing the
// result. A failure is never cached: only a token that verified is remembered,
// so a bad token cannot poison the cache and a transient error does not stick.
func (j *jwtManagerImpl) verifySignature(token string) (*verifiedToken, error) {
	// Key on a digest rather than the token so the cache never holds a usable
	// credential in plaintext.
	sum := sha256.Sum256([]byte(token))
	key := hex.EncodeToString(sum[:])

	if v, ok := j.verifyCache.Get(key); ok {
		if verified, ok := v.(*verifiedToken); ok {
			return verified, nil
		}
	}

	verified, err := j.verifyAndDecode(token)

	if err != nil {
		return nil, err
	}

	j.verifyCache.Set(key, verified)

	return verified, nil
}

// verifyAndDecode performs the uncached signature verification and claim
// checks. It returns only claims fixed at signing time.
func (j *jwtManagerImpl) verifyAndDecode(token string) (*verifiedToken, error) {
	tenantId, tokenUUID, err := j.doVerifyAndDecode(token)

	if err != nil {
		return nil, err
	}

	return &verifiedToken{tenantId: tenantId, tokenId: tokenUUID}, nil
}

func (j *jwtManagerImpl) doVerifyAndDecode(token string) (tenantId uuid.UUID, tokenUUID uuid.UUID, err error) {
	// Verify the signed token.
	audience := j.opts.Audience

	validator, err := jwt.NewValidator(&jwt.ValidatorOpts{
		ExpectedAudience:      &audience,
		ExpectedIssuer:        &j.opts.Issuer,
		FixedNow:              time.Now(),
		ExpectIssuedInThePast: true,
	})

	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("failed to create JWT Validator: %v", err)
	}

	verifiedJwt, err := j.verifier.VerifyAndDecode(token, validator)

	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("failed to verify and decode JWT: %v", err)
	}

	// Read the token from the database and make sure it's not revoked
	if hasTokenId := verifiedJwt.HasStringClaim("token_id"); !hasTokenId {
		return uuid.Nil, uuid.Nil, fmt.Errorf("token does not have token_id claim")
	}

	tokenId, err := verifiedJwt.StringClaim("token_id")

	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("failed to read token_id claim: %v", err)
	}

	tokenIdUuid, err := uuid.Parse(tokenId)

	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("failed to parse token_id claim: %v", err)
	}

	// ensure the current server url matches the token, if present
	if hasServerURL := verifiedJwt.HasStringClaim("server_url"); hasServerURL {
		serverURL, err := verifiedJwt.StringClaim("server_url")

		if err != nil {
			return uuid.Nil, uuid.Nil, fmt.Errorf("failed to read server_url claim: %v", err)
		}

		if serverURL != j.opts.ServerURL {
			return uuid.Nil, uuid.Nil, fmt.Errorf("server_url claim does not match")
		}
	}

	// The database read that used to happen here — revocation and expiry — has
	// moved to ValidateTenantToken, which is not cached. Nothing below this
	// point may depend on mutable state.

	// ensure the subject of the token matches the tenantId
	if hasSubject := verifiedJwt.HasSubject(); !hasSubject {
		return uuid.Nil, uuid.Nil, fmt.Errorf("token does not have subject claim")
	}

	subject, err := verifiedJwt.Subject()

	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("failed to read subject claim: %v", err)
	}

	parsedSubject, err := uuid.Parse(subject)

	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("failed to parse subject claim: %v", err)
	}

	return parsedSubject, tokenIdUuid, nil
}

func (j *jwtManagerImpl) getJWTOptionsForTenant(tenantId uuid.UUID, id *uuid.UUID, expires *time.Time) (tokenId uuid.UUID, expiresAt time.Time, opts *jwt.RawJWTOptions) {

	if expires != nil {
		expiresAt = *expires
	} else {
		expiresAt = time.Now().Add(90 * 24 * time.Hour)
	}

	iAt := time.Now()
	audience := j.opts.Audience
	subject := tenantId
	issuer := j.opts.Issuer
	if id == nil {
		tokenId = uuid.New()
	} else {
		tokenId = *id
	}

	subjectString := subject.String()
	opts = &jwt.RawJWTOptions{
		IssuedAt:  &iAt,
		Audience:  &audience,
		Subject:   &subjectString,
		ExpiresAt: &expiresAt,
		Issuer:    &issuer,
		CustomClaims: map[string]interface{}{
			"token_id":               tokenId.String(),
			"server_url":             j.opts.ServerURL,
			"grpc_broadcast_address": j.opts.GRPCBroadcastAddress,
		},
	}

	return
}
