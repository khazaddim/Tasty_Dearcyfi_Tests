## Why

The repository has a documented Tastytrade SDK demo roadmap, but no runnable, verified examples that establish how the SDK behaves in the environments the application will support. A small, well-documented script knowledge base is needed now so later DearCyFi and application work can rely on tested SDK behavior rather than assumptions or outdated synchronous examples.

## What Changes

- Add independently runnable, asynchronous Python demos covering OAuth connection, account information, market data, historical data, options, and dry-run order previews.
- Establish shared safety, configuration, output-redaction, timeout, and resource-cleanup conventions for every demo.
- Turn the existing roadmap into a maintained index with exact PowerShell run commands, supported-environment requirements, upstream references, expected output, and failure guidance.
- Add lightweight automated coverage for behavior that can be verified without Tastytrade credentials, including configuration validation, redaction, formatting, and bounded streaming/data-collection logic.
- Keep all account and market-data examples read-only and enforce `dry_run=True` for the order-preview demo; live order submission, replacement, and cancellation remain out of scope.

## Capabilities

### New Capabilities

- `tastyware-demo-scripts`: Provides a documented, safe, and independently runnable Tastytrade SDK script knowledge base that future repository components can treat as verified reference behavior.

### Modified Capabilities

None.

## Impact

- Adds Python demo scripts, shared demo helpers, and focused tests under the existing `Tastyware_Demo_Scripts` area.
- Updates the demo-script README/index and repository ignore rules for private local exports as needed.
- Introduces a pinned and documented Tastytrade SDK/Python compatibility baseline and uses only the SDK's current asynchronous OAuth API.
- Requires users to provide credentials through environment variables; no credentials, tokens, account numbers, or private account exports are committed or printed in full.
