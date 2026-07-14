"""Credential resolution for Amazon Ads LWA OAuth.

Refresh tokens live in AWS Secrets Manager, never in config or env vars. config/profiles.yaml
only names *which* secret to use (`secret_name`) -- same indirection principle as the
reference pipeline's env-var-name-per-store convention, but backed by Secrets Manager
(KMS-encrypted, access-controlled, auditable via CloudTrail) rather than plain env vars,
since these are OAuth refresh tokens for 26 independently-authorized accounts, not static
per-store API keys.

Access tokens (short-lived, ~1hr per LWA) are cached in-process per secret_name so a warm
Lambda container doesn't re-exchange a refresh token on every invocation -- only when the
cached token is missing or within a minute of expiring.
"""

import json
import os
import time

import boto3
import requests

from common.logging_config import get_logger, log_fields

logger = get_logger(__name__)

_TOKEN_ENDPOINT = "https://api.amazon.com/auth/o2/token"
_TOKEN_EXPIRY_SAFETY_MARGIN_SECONDS = 60

# secret_name -> (access_token, expires_at_epoch_seconds)
_access_token_cache: dict[str, tuple[str, float]] = {}

_secrets_client = None


def _client():
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = boto3.client("secretsmanager")
    return _secrets_client


def get_refresh_token(secret_name: str) -> str:
    """Fetch one authorizing account's refresh token from Secrets Manager.

    Secret is a JSON object shaped {"refresh_token": "Atzr|..."}.
    """
    response = _client().get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret["refresh_token"]


def get_access_token(secret_name: str) -> str:
    """Return a cached access token if still valid, else exchange the refresh token for a
    new one via LWA's token endpoint.

    Never logs a token value or the raw refresh token -- only secret_name (a Secrets
    Manager identifier, not a credential itself) and non-sensitive response metadata. On
    exchange failure, the underlying HTTPError is deliberately not re-raised as-is (its
    request object can carry the refresh token in form-encoded data) -- same secret-hygiene
    discipline as the reference pipeline's Teams notifier scrubbing a webhook URL out of its
    own error logs.
    """
    cached = _access_token_cache.get(secret_name)
    now = time.time()
    if cached and cached[1] - _TOKEN_EXPIRY_SAFETY_MARGIN_SECONDS > now:
        return cached[0]

    refresh_token = get_refresh_token(secret_name)
    client_id = os.environ["ADS_LWA_CLIENT_ID"]
    client_secret = os.environ["ADS_LWA_CLIENT_SECRET"]

    response = requests.post(
        _TOKEN_ENDPOINT,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=10,
    )
    if not response.ok:
        logger.error(
            "LWA token exchange failed",
            extra=log_fields(secret_name=secret_name, status_code=response.status_code),
        )
        raise RuntimeError(f"LWA token exchange failed with status {response.status_code}") from None

    payload = response.json()
    access_token = payload["access_token"]
    expires_at = now + payload["expires_in"]
    _access_token_cache[secret_name] = (access_token, expires_at)
    logger.info(
        "refreshed LWA access token",
        extra=log_fields(secret_name=secret_name, expires_in=payload["expires_in"]),
    )
    return access_token
