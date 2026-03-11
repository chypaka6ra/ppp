# Capital.com Level1 Quotes Handler

Python module for real-time Level1 market data from Capital.com API with both **REST API polling** and **WebSocket streaming** support.

## Features

- **Level1 Data**: Bid, Ask, Last Price, Percentage Change
- **WebSocket Streaming** ⚡ (RECOMMENDED): Real-time data via `wss://` connection
- **REST API Polling**: Fallback option, polls every 1 second
- **Async/Await**: Full asynchronous support with asyncio
- **Session Management**: Automatic authentication with token refresh
- **Batch Processing**: Up to 50 symbols per REST request, up to 40 concurrent subscriptions via WebSocket
- **Error Handling**: Custom exceptions for authentication and connection errors
- **Multiple Interfaces**: Context manager, async iterator, callback-based, polling
- **Type Hints**: Full Python type annotations
- **Automatic Reconnection**: WebSocket auto-reconnects on disconnection

## Installation

Copy `capitalcom.py` to your `lib/brokers/` directory.

### Dependencies

```bash
pip install aiohttp websockets
```

## WebSocket vs REST Polling

| Aspect | WebSocket | REST Polling |
|--------|-----------|--------------|
| **Update Frequency** | Real-time (instant) | Every 1 second |
| **Latency** | ~10-100ms | ~1000ms |
| **Network Efficiency** | Very efficient | Less efficient (polling) |
| **Max Instruments** | 40 concurrent | 50 per request |
| **Server Load** | Lower | Higher |
| **Recommended Use** | ✅ Production & Real-time | Fallback/Simple cases |

**Recommendation**: Use WebSocket for real-time market data and trading applications.

## Quick Start

### WebSocket Streaming (Recommended) ⚡

```python
import asyncio
from lib.brokers.capitalcom import CapitalComLevel1WebSocket

async def main():
    async with CapitalComLevel1WebSocket(
        api_key='YOUR_API_KEY',
        identifier='your_email@example.com',
        password='your_password'
    ) as ws:
        def on_quote(quote):
            print(f"{quote.symbol}: {quote.bid}/{quote.ask}")

        await ws.subscribe(['CS.D', 'AAPL.US'], callback=on_quote)
        await ws._receive_loop()  # Runs continuously

asyncio.run(main())
```

### Simple Single Quote (Auto-detects Best Method)

```python
import asyncio
from lib.brokers.capitalcom import get_level1_quote

async def main():
    quote = await get_level1_quote(
        api_key='YOUR_API_KEY',
        identifier='your_email@example.com',
        password='your_password',
        symbol='CS.D',
        use_websocket=True  # Recommended for speed
    )
    print(f"{quote.symbol}: {quote.bid}/{quote.ask}")

asyncio.run(main())
```

### Context Manager

```python
from lib.brokers.capitalcom import CapitalComLevel1Quotes

async def main():
    async with CapitalComLevel1Quotes(
        api_key='YOUR_API_KEY',
        identifier='your_email@example.com',
        password='your_password'
    ) as quotes:
        await quotes.subscribe(['CS.D', 'AAPL.US'])
        market_data = await quotes._fetch_market_data()
        for quote in market_data:
            print(f"{quote.symbol}: {quote.bid}/{quote.ask}")

asyncio.run(main())
```

### Async Iterator

```python
async with CapitalComLevel1Quotes(...) as quotes:
    await quotes.subscribe(['CS.D'])
    async for quote in quotes:
        print(f"{quote.symbol}: {quote.last_price}")
```

### Continuous Polling

```python
async def main():
    quotes = CapitalComLevel1Quotes(
        api_key='YOUR_API_KEY',
        identifier='your_email@example.com',
        password='your_password',
        poll_interval=1.0
    )

    await quotes.connect()
    await quotes.subscribe(['CS.D', 'AAPL.US'])
    await quotes.start_polling()  # Runs continuously

asyncio.run(main())
```

### With Callback

```python
def on_quote(quote):
    print(f"{quote.symbol}: {quote.bid}/{quote.ask}")

async with CapitalComLevel1Quotes(...) as quotes:
    await quotes.subscribe(['CS.D'], callback=on_quote)
    market_data = await quotes._fetch_market_data()
```

## API Reference

### Class: `CapitalComLevel1Quotes`

#### Constructor

```python
CapitalComLevel1Quotes(
    api_key: str,
    identifier: str,
    password: str,
    poll_interval: float = 1.0,
    last_price_mode: str = 'mid'
)
```

**Parameters:**
- `api_key`: Capital.com API key
- `identifier`: Email/username for authentication
- `password`: Account password
- `poll_interval`: Data polling interval in seconds (default: 1.0)
- `last_price_mode`: How to calculate last price:
  - `'bid'`: Use bid price as last price
  - `'ask'`: Use ask price as last price
  - `'mid'`: Use mid-price (bid + ask) / 2 [default]

#### Methods

##### `async connect() -> None`
Establish connection and authenticate with Capital.com API.

##### `async disconnect() -> None`
Close connection and stop polling.

##### `async subscribe(symbols: list[str], callback: Optional[Callable] = None) -> None`
Subscribe to symbols for Level1 data.

**Parameters:**
- `symbols`: List of epic symbols (e.g., `['CS.D', 'AAPL.US']`)
- `callback`: Optional callback function called for each quote

##### `async unsubscribe(symbols: list[str]) -> None`
Unsubscribe from symbols.

##### `async ensure_session() -> None`
Ensure valid authentication session.

Raises:
- `CapitalComAuthenticationError`: If authentication fails
- `CapitalComConnectionError`: If connection fails

##### `async start_polling() -> None`
Start continuous polling of subscribed symbols.

##### `set_error_handler(callback: Callable[[Exception], None]) -> None`
Set callback for handling errors during polling.

##### `async _fetch_market_data() -> list[Level1Quote]`
Fetch Level1 market data for subscribed symbols.

Returns: List of `Level1Quote` objects

### Class: `CapitalComLevel1WebSocket` ⚡

WebSocket streaming handler for real-time Level1 market data.

**Recommended for production use** - significantly lower latency and network overhead.

#### Constructor

```python
CapitalComLevel1WebSocket(
    api_key: str,
    identifier: str,
    password: str,
    last_price_mode: str = 'mid'
)
```

**Parameters:**
- `api_key`: Capital.com API key
- `identifier`: Email/username
- `password`: Password
- `last_price_mode`: How to calculate last price (`'bid'`, `'ask'`, `'mid'`)

#### Methods

##### `async connect() -> None`
Establish WebSocket connection and authenticate.

##### `async disconnect() -> None`
Close WebSocket and cleanup resources.

##### `async subscribe(symbols: list[str], callback: Optional[Callable] = None) -> None`
Subscribe to symbols via WebSocket.

**Parameters:**
- `symbols`: List of epic symbols (max 40 concurrent)
- `callback`: Optional callback function for each quote

**Raises:**
- `CapitalComWebSocketError`: If subscription fails or max limit exceeded

##### `async unsubscribe(symbols: list[str]) -> None`
Unsubscribe from symbols.

##### `async ensure_session() -> None`
Ensure valid authentication session.

##### `set_error_handler(callback: Callable[[Exception], None]) -> None`
Set callback for error handling during streaming.

##### `async _receive_loop() -> None`
Start continuous receiving of WebSocket messages.

Automatically reconnects on disconnection.

### Class: `Level1Quote`

Data class representing Level1 market data.

**Attributes:**
- `epic: str` - Instrument identifier
- `symbol: str` - Instrument symbol/name
- `bid: float` - Current bid price
- `ask: float` - Current ask price
- `last_price: float` - Calculated last price
- `percentage_change: float` - Percentage change
- `timestamp: datetime` - Data timestamp

**Methods:**
- `to_dict() -> Dict[str, Any]` - Convert to dictionary representation

### Exceptions

#### `CapitalComAuthenticationError`
Raised when authentication fails or session is invalid.

#### `CapitalComConnectionError`
Raised when network connection fails or request times out.

#### `CapitalComWebSocketError`
Raised when WebSocket operation fails (subscription, connection, etc).

### Convenience Functions

#### `get_level1_quote(api_key, identifier, password, symbol, use_websocket=False)`
Get a single Level1 quote.

```python
quote = await get_level1_quote(
    api_key='KEY',
    identifier='email@example.com',
    password='pwd',
    symbol='CS.D',
    use_websocket=True  # Recommended
)
```

#### `stream_level1_quotes_websocket(api_key, identifier, password, symbols, callback)`
Stream Level1 quotes via WebSocket with callback.

```python
async def on_quote(quote):
    print(f"{quote.symbol}: {quote.bid}/{quote.ask}")

await stream_level1_quotes_websocket(
    api_key='KEY',
    identifier='email@example.com',
    password='pwd',
    symbols=['CS.D', 'AAPL.US'],
    callback=on_quote
)
```

## Configuration

### Last Price Mode

Choose how to calculate the last price from bid/ask:

```python
quotes = CapitalComLevel1Quotes(
    ...,
    last_price_mode='mid'  # 'bid', 'ask', or 'mid'
)
```

### Poll Interval

Set the polling frequency (in seconds):

```python
quotes = CapitalComLevel1Quotes(
    ...,
    poll_interval=0.5  # Poll twice per second
)
```

### Session Timeout

The module automatically refreshes sessions every 9 minutes (Capital.com session validity: 10 minutes).

## Performance Considerations

### Symbol Batching

The Capital.com API supports up to 50 symbols per request. Larger symbol sets are automatically batched:

```python
# This will be split into 2 requests (50 + 30)
await quotes.subscribe([...80 symbols...])
```

### Polling Interval

- **1.0 second** (default): Good balance for most use cases
- **0.5 seconds**: More real-time data, higher API usage
- **2.0+ seconds**: Lower latency for non-critical data

### Concurrent Operations

Run polling and other operations concurrently:

```python
async def main():
    quotes = CapitalComLevel1Quotes(...)
    await quotes.connect()

    polling_task = asyncio.create_task(quotes.start_polling())

    # Do other work while polling
    await asyncio.sleep(60)

    # Stop polling
    await quotes.disconnect()
    await polling_task

asyncio.run(main())
```

## Error Handling

### Handle Authentication Errors

```python
from lib.brokers.capitalcom import CapitalComAuthenticationError

try:
    async with CapitalComLevel1Quotes(...) as quotes:
        await quotes.subscribe(['CS.D'])
except CapitalComAuthenticationError as e:
    print(f"Invalid credentials: {e}")
```

### Handle Connection Errors

```python
from lib.brokers.capitalcom import CapitalComConnectionError

try:
    quote = await quotes._fetch_market_data()
except CapitalComConnectionError as e:
    print(f"Network error: {e}")
```

### Set Global Error Handler

```python
def on_error(error):
    print(f"Error: {type(error).__name__}: {error}")

quotes = CapitalComLevel1Quotes(...)
quotes.set_error_handler(on_error)
await quotes.start_polling()
```

## Logging

Enable debug logging to see API calls and session management:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('lib.brokers.capitalcom')
```

## API Endpoints Reference

The module uses these Capital.com API endpoints:

- **Authentication**: `POST /api/v1/session`
- **Market Data**: `GET /api/v1/markets?epics={symbols}`

## Rate Limiting

Capital.com API rate limits:
- Typically 1000 requests per 10 seconds
- The module polls every 1 second by default
- With 50 symbols max per request, safely within limits

## Limitations

### REST API (Polling)
- Maximum 50 symbols per single API request (module handles batching)
- Polling interval: 1 second (configurable)
- Not real-time (latency ~1000ms)

### WebSocket (Streaming)
- Maximum 40 concurrent subscriptions
- Session timeout: 10 minutes (module auto-refreshes at 9 minutes)
- Must ping every 10 minutes to keep connection alive (auto-managed)
- Real-time update frequency: 10-100ms latency

### Both Methods
- Only Level1 data (no order book depth)
- Rate limit: ~10 requests per second
- Supported markets depend on Capital.com account permissions

## Security Notes

- Never hardcode credentials in production code
- Use environment variables or secure configuration
- Keep API keys and passwords private
- Recommend using IP whitelisting in Capital.com settings

## Contributing

See the main repository guidelines for contributions.

## License

Same as parent project.
