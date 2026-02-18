"""
LexSolv AI — Async wrapper for the Anthropic Claude API.

Provides a thin async interface for narrative generation with:
- Rate-limit handling via exponential backoff (3 attempts)
- Token usage logging for cost tracking
- Graceful error handling when API key is missing

WARNING: All input text must be scrubbed via PrivacyVault.scrub()
before passing to this client. See Rule 10 in dev-rulebook.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-20250514"
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 2.0


@dataclass
class GenerateResult:
    """Result from a Claude API generation call."""
    text: str
    input_tokens: int
    output_tokens: int
    model: str


class ClaudeClient:
    """
    Minimal async wrapper for the Anthropic Claude API.

    WARNING: All input text must be scrubbed via PrivacyVault.scrub()
    before passing to this client. See Rule 10 in dev-rulebook.
    """

    def __init__(self, api_key: Optional[str] = None) -> None:
        self._api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        self._client = None

    def _ensure_client(self):
        """Lazily initialise the Anthropic async client."""
        if self._client is not None:
            return
        if not self._api_key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not set. "
                "Set it in the environment or pass api_key to ClaudeClient."
            )
        import anthropic
        self._client = anthropic.AsyncAnthropic(api_key=self._api_key)

    async def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 2000,
    ) -> GenerateResult:
        """
        Send a prompt to Claude and return the generated text with token usage.

        Parameters
        ----------
        system_prompt : str
            System-level instructions for Claude.
        user_prompt : str
            The user-facing prompt (scrubbed of PII).
        max_tokens : int
            Maximum tokens in the response.

        Returns
        -------
        GenerateResult
            Contains generated text and token usage metadata.

        Raises
        ------
        RuntimeError
            If the API key is missing or all retries are exhausted.
        """
        import asyncio
        import anthropic

        self._ensure_client()

        last_error: Optional[Exception] = None
        backoff = INITIAL_BACKOFF_SECONDS

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = await self._client.messages.create(
                    model=MODEL,
                    max_tokens=max_tokens,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_prompt}],
                )

                text = response.content[0].text if response.content else ""
                input_tokens = response.usage.input_tokens
                output_tokens = response.usage.output_tokens

                logger.info(
                    "Claude generation complete — model=%s input_tokens=%d output_tokens=%d",
                    MODEL, input_tokens, output_tokens,
                )

                return GenerateResult(
                    text=text,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    model=MODEL,
                )

            except anthropic.RateLimitError as exc:
                last_error = exc
                logger.warning(
                    "Rate limited (attempt %d/%d), retrying in %.1fs",
                    attempt, MAX_RETRIES, backoff,
                )
                await asyncio.sleep(backoff)
                backoff *= 2

            except anthropic.APIStatusError as exc:
                last_error = exc
                if attempt < MAX_RETRIES and exc.status_code >= 500:
                    logger.warning(
                        "API error %d (attempt %d/%d), retrying in %.1fs",
                        exc.status_code, attempt, MAX_RETRIES, backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff *= 2
                else:
                    raise RuntimeError(
                        f"Claude API error (status {exc.status_code}): {exc.message}"
                    ) from exc

        raise RuntimeError(
            f"Claude API call failed after {MAX_RETRIES} attempts: {last_error}"
        )
