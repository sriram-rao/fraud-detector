## Fraud Detector

Transaction monitoring subsystem for detecting fraudulent patterns in financial transactions.

## Architecture

**Pipeline:** CSV → DuckDB → ExecutionEngine → Checkers → Fraud Flags

**Core abstraction:** `FraudChecker` interface with pluggable detection strategies
- Rule-based: Static thresholds and conditions
- Window-based: Temporal patterns using SQL window functions
- Model-based: ML/AI-powered detection (LLM integration)

**Key design choices:**
- **Batch processing** - Load entire CSV upfront, not real-time streaming
- **Database for compute** - Use DuckDB for heavy operations (aggregations, window functions)
- **Composable rules** - Checkers are independent, can be mixed and matched

## Usage

```bash
python main.py transactions.csv output.txt
```