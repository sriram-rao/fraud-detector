"""LLM-based fraud checker using AI analysis."""
from typing import List, Dict, Any, Optional
import json
from checker.model_based_checker import ModelBasedChecker
from checker.fraud_checker import Transaction


class LLMChecker(ModelBasedChecker):
    """Uses LLM to detect fraud patterns in transactions."""

    def __init__(self, name: str = "LLMChecker", api_key: Optional[str] = None):
        super().__init__(name)
        self.api_key = api_key

    def initialize(self, historical_transactions: Optional[List[Transaction]] = None,
                   config: Optional[Dict[str, Any]] = None) -> None:
        super().initialize(historical_transactions, config)
        if config:
            self.api_key = config.get('api_key', self.api_key)

    def predict(self, transactions: List[Transaction]) -> List[tuple[List[Transaction], str, float]]:
        """Send transactions to LLM for fraud detection."""
        if not self.api_key:
            return []

        if not transactions:
            return []

        txn_data = self._format_transactions(transactions)

        try:
            import anthropic
            client = anthropic.Anthropic(api_key=self.api_key)

            response = client.messages.create(
                model="claude-sonnet-4-5",
                max_tokens=2000,
                messages=[{
                    "role": "user",
                    "content": f"""Analyze these transactions for fraud patterns. Return ONLY a JSON array of suspicious transaction indices (0-based).

Transactions:
{txn_data}

Look for: velocity spikes, unusual amounts, late-night high values, merchant repetition, geographic shifts.

Return format: {{"suspicious_indices": [1, 5, 7], "reason": "explanation"}}"""
                }]
            )

            result = self._parse_llm_response(response.content[0].text, transactions)
            return result

        except Exception as e:
            print(f"LLM checker error: {e}")
            return []

    def _format_transactions(self, transactions: List[Transaction]) -> str:
        """Format transactions for LLM."""
        lines = []
        for i, txn in enumerate(transactions):
            lines.append(
                f"{i}: user={txn.user_id}, time={txn.timestamp}, "
                f"merchant={txn.merchant_name}, amount=${txn.amount:.2f}"
            )
        return "\n".join(lines)

    def _parse_llm_response(self, response_text: str,
                           transactions: List[Transaction]) -> List[tuple[List[Transaction], str, float]]:
        """Parse LLM response into predictions."""
        try:
            start = response_text.find('{')
            end = response_text.rfind('}') + 1
            if start == -1 or end == 0:
                return []

            json_str = response_text[start:end]
            result = json.loads(json_str)

            indices = result.get('suspicious_indices', [])
            reason = result.get('reason', 'LLM detected suspicious pattern')

            if not indices:
                return []

            flagged_txns = [transactions[i] for i in indices if 0 <= i < len(transactions)]

            if flagged_txns:
                return [(flagged_txns, f"LLM: {reason}", 0.70)]

            return []

        except Exception as e:
            print(f"Failed to parse LLM response: {e}")
            return []
