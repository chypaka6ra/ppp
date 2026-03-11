"""
Capital.com Level1 Quotes - Usage Examples

Demonstrates various ways to use the CapitalComLevel1Quotes (polling)
and CapitalComLevel1WebSocket (streaming) classes.
"""

import asyncio
from lib.brokers.capitalcom import (
    CapitalComLevel1Quotes,
    CapitalComLevel1WebSocket,
    get_level1_quote,
    stream_level1_quotes_websocket,
    CapitalComAuthenticationError,
    CapitalComConnectionError,
    CapitalComWebSocketError
)


# Example 1: Simple single quote fetch
async def example_single_quote():
    """Fetch a single Level1 quote"""
    try:
        quote = await get_level1_quote(
            api_key='your_api_key',
            identifier='your_email@example.com',
            password='your_password',
            symbol='CS.D'  # Capital.com stock symbol
        )

        print(f"Symbol: {quote.symbol}")
        print(f"Bid: {quote.bid}")
        print(f"Ask: {quote.ask}")
        print(f"Last Price: {quote.last_price}")
        print(f"Change: {quote.percentage_change}%")

    except CapitalComAuthenticationError as e:
        print(f"Authentication failed: {e}")
    except CapitalComConnectionError as e:
        print(f"Connection error: {e}")


# Example 2: Context manager with callback
async def example_with_callback():
    """Subscribe to multiple symbols with callback"""

    def on_quote(quote):
        """Handle each quote"""
        print(f"{quote.symbol}: {quote.bid:.4f}/{quote.ask:.4f} ({quote.percentage_change:+.2f}%)")

    try:
        async with CapitalComLevel1Quotes(
            api_key='your_api_key',
            identifier='your_email@example.com',
            password='your_password',
            last_price_mode='mid'  # Options: 'bid', 'ask', 'mid'
        ) as quotes:
            # Subscribe to multiple symbols
            await quotes.subscribe(
                ['CS.D', 'AAPL.US', 'MSFT.US'],
                callback=on_quote
            )

            # Polling loop - runs for 30 seconds
            start_time = asyncio.get_event_loop().time()
            while asyncio.get_event_loop().time() - start_time < 30:
                await quotes._fetch_market_data()
                await asyncio.sleep(1)

    except (CapitalComAuthenticationError, CapitalComConnectionError) as e:
        print(f"Error: {e}")


# Example 3: Async iterator pattern
async def example_async_iterator():
    """Use async iterator to get quotes"""
    try:
        async with CapitalComLevel1Quotes(
            api_key='your_api_key',
            identifier='your_email@example.com',
            password='your_password'
        ) as quotes:
            await quotes.subscribe(['CS.D'])

            # Get 10 quotes
            for i in range(10):
                quote = await quotes.__anext__()
                print(f"Quote {i+1}: {quote.symbol} @ {quote.last_price}")

    except (CapitalComAuthenticationError, CapitalComConnectionError) as e:
        print(f"Error: {e}")


# Example 4: Continuous polling with error handling
async def example_continuous_polling():
    """Continuous polling with error handler"""

    def on_error(error):
        """Handle errors"""
        print(f"Error occurred: {type(error).__name__}: {error}")

    try:
        quotes = CapitalComLevel1Quotes(
            api_key='your_api_key',
            identifier='your_email@example.com',
            password='your_password',
            poll_interval=1.0
        )

        await quotes.connect()
        quotes.set_error_handler(on_error)

        await quotes.subscribe(['CS.D', 'AAPL.US'])

        # Start polling
        polling_task = asyncio.create_task(quotes.start_polling())

        # Let it run for 60 seconds
        try:
            await asyncio.wait_for(polling_task, timeout=60)
        except asyncio.TimeoutError:
            pass
        finally:
            await quotes.disconnect()

    except Exception as e:
        print(f"Setup error: {e}")


# Example 5: Multiple concurrent subscriptions
async def example_multiple_subscriptions():
    """Handle multiple symbol subscriptions in batches"""
    try:
        async with CapitalComLevel1Quotes(
            api_key='your_api_key',
            identifier='your_email@example.com',
            password='your_password'
        ) as quotes:
            # Capital.com can handle up to 50 symbols per request
            symbols = ['CS.D', 'AAPL.US', 'MSFT.US', 'GOOGL.US', 'AMZN.US']

            await quotes.subscribe(symbols)

            # Fetch market data once
            market_data = await quotes._fetch_market_data()

            for quote in market_data:
                print(f"{quote.symbol}: Bid={quote.bid:.4f} Ask={quote.ask:.4f}")

    except Exception as e:
        print(f"Error: {e}")


# Example 6: Raw market data parsing
async def example_custom_processing():
    """Custom processing of market data"""
    try:
        async with CapitalComLevel1Quotes(
            api_key='your_api_key',
            identifier='your_email@example.com',
            password='your_password'
        ) as quotes:
            await quotes.subscribe(['CS.D', 'AAPL.US'])

            quotes_data = await quotes._fetch_market_data()

            # Process quotes in custom way
            for quote in quotes_data:
                quote_dict = quote.to_dict()
                print(f"Quote JSON: {quote_dict}")

                # Calculate spread
                spread = quote.ask - quote.bid
                spread_pips = spread * 10000  # For forex pairs

                print(f"Spread: {spread} ({spread_pips:.0f} pips)")

    except Exception as e:
        print(f"Error: {e}")


# WebSocket Examples (Recommended for Real-time Data)

# Example 7: WebSocket - Simple streaming
async def example_websocket_streaming():
    """Stream quotes via WebSocket (most efficient)"""
    try:
        async with CapitalComLevel1WebSocket(
            api_key='your_api_key',
            identifier='your_email@example.com',
            password='your_password'
        ) as ws:
            def on_quote(quote):
                print(f"{quote.symbol}: {quote.bid:.4f}/{quote.ask:.4f}")

            await ws.subscribe(['CS.D', 'AAPL.US'], callback=on_quote)

            # Keep receiving for 60 seconds
            try:
                await asyncio.wait_for(ws._receive_loop(), timeout=60)
            except asyncio.TimeoutError:
                pass

    except CapitalComWebSocketError as e:
        print(f"WebSocket error: {e}")
    except (CapitalComAuthenticationError, CapitalComConnectionError) as e:
        print(f"Error: {e}")


# Example 8: WebSocket - Async iterator
async def example_websocket_async_iterator():
    """Use async iterator with WebSocket"""
    try:
        async with CapitalComLevel1WebSocket(
            api_key='your_api_key',
            identifier='your_email@example.com',
            password='your_password'
        ) as ws:
            await ws.subscribe(['CS.D'])

            # Get 10 quotes via iterator
            for i in range(10):
                try:
                    quote = await asyncio.wait_for(
                        ws.__anext__(),
                        timeout=5.0
                    )
                    print(f"Quote {i+1}: {quote.symbol} @ {quote.last_price}")
                except asyncio.TimeoutError:
                    print(f"Timeout waiting for quote {i+1}")
                    break

    except (CapitalComAuthenticationError, CapitalComConnectionError) as e:
        print(f"Error: {e}")


# Example 9: WebSocket - Convenience function
async def example_websocket_convenience():
    """Use convenience function for WebSocket"""
    try:
        # Get single quote via WebSocket
        quote = await get_level1_quote(
            api_key='your_api_key',
            identifier='your_email@example.com',
            password='your_password',
            symbol='CS.D',
            use_websocket=True  # Use WebSocket instead of polling
        )

        print(f"Symbol: {quote.symbol}")
        print(f"Bid: {quote.bid}")
        print(f"Ask: {quote.ask}")
        print(f"Change: {quote.percentage_change}%")

    except (CapitalComAuthenticationError, CapitalComConnectionError) as e:
        print(f"Error: {e}")


# Example 10: WebSocket - Stream with error handling
async def example_websocket_with_errors():
    """WebSocket streaming with comprehensive error handling"""

    def on_error(error):
        print(f"Error occurred: {type(error).__name__}: {error}")

    def on_quote(quote):
        print(f"{quote.symbol}: {quote.bid}/{quote.ask}")

    try:
        async with CapitalComLevel1WebSocket(
            api_key='your_api_key',
            identifier='your_email@example.com',
            password='your_password'
        ) as ws:
            ws.set_error_handler(on_error)

            # Subscribe (max 40 instruments)
            symbols = ['CS.D', 'AAPL.US', 'GOOGL.US']
            await ws.subscribe(symbols, callback=on_quote)

            # Stream for 120 seconds
            try:
                await asyncio.wait_for(ws._receive_loop(), timeout=120)
            except asyncio.TimeoutError:
                pass

    except CapitalComWebSocketError as e:
        print(f"WebSocket error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


# Example 11: WebSocket - Convenience streaming function
async def example_websocket_convenience_stream():
    """Use convenience function for WebSocket streaming"""

    def on_quote(quote):
        print(f"{quote.symbol}: Bid={quote.bid:.4f} Ask={quote.ask:.4f}")

    try:
        await stream_level1_quotes_websocket(
            api_key='your_api_key',
            identifier='your_email@example.com',
            password='your_password',
            symbols=['CS.D', 'AAPL.US'],
            callback=on_quote
        )
    except Exception as e:
        print(f"Error: {e}")


# Example 12: REST vs WebSocket comparison
async def example_rest_vs_websocket():
    """Compare REST polling vs WebSocket streaming"""

    print("REST Polling (1 second interval):")
    async with CapitalComLevel1Quotes(
        api_key='your_api_key',
        identifier='your_email@example.com',
        password='your_password'
    ) as quotes:
        await quotes.subscribe(['CS.D'])
        start = asyncio.get_event_loop().time()

        for _ in range(5):
            data = await quotes._fetch_market_data()
            elapsed = asyncio.get_event_loop().time() - start
            if data:
                print(f"  {elapsed:.2f}s: {data[0].symbol} @ {data[0].last_price}")

    print("\nWebSocket Streaming (real-time):")
    quotes_received = []

    def on_quote(quote):
        quotes_received.append(quote)

    async with CapitalComLevel1WebSocket(
        api_key='your_api_key',
        identifier='your_email@example.com',
        password='your_password'
    ) as ws:
        await ws.subscribe(['CS.D'], callback=on_quote)

        try:
            await asyncio.wait_for(ws._receive_loop(), timeout=5)
        except asyncio.TimeoutError:
            pass

    print(f"  Received {len(quotes_received)} quotes in 5 seconds")


if __name__ == '__main__':
    print("Capital.com Level1 Quotes Examples")
    print("=" * 70)
    print("\nBefore running these examples:")
    print("1. Replace 'your_api_key' with your Capital.com API key")
    print("2. Replace 'your_email@example.com' with your Capital.com email")
    print("3. Replace 'your_password' with your password")
    print("\nChoose an example to run by uncommenting it below.\n")

    print("REST API Examples (Polling):")
    print("  - example_single_quote()")
    print("  - example_with_callback()")
    print("  - example_async_iterator()")
    print("  - example_continuous_polling()")
    print("  - example_multiple_subscriptions()")
    print("  - example_custom_processing()")

    print("\nWebSocket Examples (RECOMMENDED for real-time):")
    print("  - example_websocket_streaming()")
    print("  - example_websocket_async_iterator()")
    print("  - example_websocket_convenience()")
    print("  - example_websocket_with_errors()")
    print("  - example_websocket_convenience_stream()")
    print("  - example_rest_vs_websocket()")

    print("\nUncomment one of these to run:")
    # REST Examples
    # asyncio.run(example_single_quote())
    # asyncio.run(example_with_callback())
    # asyncio.run(example_async_iterator())
    # asyncio.run(example_continuous_polling())
    # asyncio.run(example_multiple_subscriptions())
    # asyncio.run(example_custom_processing())

    # WebSocket Examples
    # asyncio.run(example_websocket_streaming())
    # asyncio.run(example_websocket_async_iterator())
    # asyncio.run(example_websocket_convenience())
    # asyncio.run(example_websocket_with_errors())
    # asyncio.run(example_websocket_convenience_stream())
    # asyncio.run(example_rest_vs_websocket())
