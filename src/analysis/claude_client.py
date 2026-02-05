"""Thin wrapper around the Anthropic Python SDK for Claude API calls."""

import logging
import time
from typing import Optional

import anthropic

from src.config import ANTHROPIC_API_KEY, CLAUDE_MODEL

logger = logging.getLogger(__name__)

SYSTEM_PROMPT_SCIENTIST = """You are a senior molecular diagnostics scientist specializing in TB \
detection using qPCR-based point-of-care devices. You have deep expertise in:

- qPCR interpretation: Ct values, amplification curve quality (sigmoid vs \
sloping), limit of detection (LOD), and what these mean for assay performance
- TB-specific assays: IS6110, IS1081 (M. tuberculosis detection), \
rpoB (rifampicin resistance), Human (internal control)
- Polymerase comparison: DsBio HS (hot-start) vs fTaq and their behavior \
with different sample matrices
- Sample types: tongue swabs, sputum, liquid controls, and how matrix \
effects (inhibitors) impact PCR performance
- Thermocycling optimization: preheating, touchdown sequences, annealing \
temperature effects
- Dual-channel qPCR: FAM (target detection) and ROX (internal control) \
fluorophore channels

Key domain knowledge:
- Lower Ct = earlier amplification = more target DNA = better detection
- Sigmoid curves indicate clean amplification; sloping curves suggest inhibition
- A Ct difference >2 between conditions is generally meaningful
- "0" or "-" in Ct data means no amplification detected
- Human control (ROX channel) should always amplify in clinical samples
- Good LOD for TB detection: reliable detection at <100 copies per reaction"""


class ClaudeClient:
    """Wrapper around the Anthropic API with retry logic and token budgeting."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        max_retries: int = 3,
    ):
        self._client = anthropic.Anthropic(api_key=api_key or ANTHROPIC_API_KEY)
        self._model = model or CLAUDE_MODEL
        self._max_retries = max_retries

    def send_message(
        self,
        user_prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: int = 8192,
        temperature: float = 0.3,
    ) -> str:
        """Send a message to Claude and return the response text.

        Args:
            user_prompt: The user message content.
            system_prompt: Optional system prompt. Defaults to scientist prompt.
            max_tokens: Maximum tokens in the response.
            temperature: Sampling temperature.

        Returns:
            The assistant's response text.
        """
        if system_prompt is None:
            system_prompt = SYSTEM_PROMPT_SCIENTIST

        for attempt in range(self._max_retries):
            try:
                message = self._client.messages.create(
                    model=self._model,
                    max_tokens=max_tokens,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_prompt}],
                    temperature=temperature,
                )
                return message.content[0].text

            except anthropic.RateLimitError:
                wait_time = 2 ** (attempt + 1)
                logger.warning(f"Rate limited, waiting {wait_time}s (attempt {attempt + 1})")
                time.sleep(wait_time)
            except anthropic.APIStatusError as e:
                if e.status_code >= 500:
                    wait_time = 2 ** (attempt + 1)
                    logger.warning(f"Server error {e.status_code}, retrying in {wait_time}s")
                    time.sleep(wait_time)
                else:
                    raise

        raise RuntimeError(f"Failed after {self._max_retries} retries")

    def send_message_with_system(
        self,
        user_prompt: str,
        system_prompt: str,
        max_tokens: int = 8192,
    ) -> str:
        """Send a message with a custom system prompt."""
        return self.send_message(
            user_prompt=user_prompt,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
        )
