# Rule: Credential Security

## Never log credentials
Database passwords, API keys, JWT secrets, and encryption keys must never appear in log
output, error messages, or HTTP responses. Use `***` or `[REDACTED]` in logs.

## Fernet encryption
All database passwords are encrypted with Fernet (`cryptography` library) using
`CREDENTIAL_ENCRYPTION_KEY` before being stored in the `instances` table.
The encryption/decryption happens in `app/security/credential_reference.py`.

**Never** store a plaintext password. **Never** return the encrypted value from the API
(the `password` field is excluded from all response schemas).

## Credential vault
The `CredentialResolver` supports an external vault via `CREDENTIAL_VAULT_BASE_URL`. When
configured, encrypted passwords are stored as vault references and the actual secret is
fetched from the vault at collection time. The vault response is never logged.

## JWT secrets
`JWT_SECRET_KEY` must be set to a random 32+ byte value in production. The default value
in the codebase must never reach a production deployment.

## Admin default password
`ADMIN_PASSWORD=admin` is the default. It must be changed immediately on first deployment.
The system does not enforce this automatically — it is an operational responsibility.

## API key masking
AI API keys stored in `ai_api_keys` are truncated to show only the last 4 characters in
list/detail API responses. The full key is only used server-side for LLM calls.
