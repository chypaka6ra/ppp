"""
Capital.com Level1 Quotes - Usage Examples

Demonstrates various ways to use the CapitalComLevel1Quotes class.
"""

import asyncio
from lib.brokers.capitalcom import (
    CapitalComLevel1Quotes,
    get_level1_quote,
    CapitalComAuthenticationError,
    CapitalComConnectionError
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


if __name__ == '__main__':
    print("Capital.com Level1 Quotes Examples")
    print("=" * 50)
    print("\nBefore running these examples:")
    print("1. Replace 'your_api_key' with your Capital.com API key")
    print("2. Replace 'your_email@example.com' with your Capital.com email")
    print("3. Replace 'your_password' with your password")
    print("\nChoose an example to run by uncommenting it below.\n")

    # Uncomment one of these to run:
    # asyncio.run(example_single_quote())
    # asyncio.run(example_with_callback())
    # asyncio.run(example_async_iterator())
    # asyncio.run(example_continuous_polling())
    # asyncio.run(example_multiple_subscriptions())
    # asyncio.run(example_custom_processing())
