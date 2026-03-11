"""
Capital.com Level1 Quotes Handler

Provides real-time Level1 market data (bid/ask/last price/change)
from Capital.com API with automatic session management.
"""

import asyncio
import aiohttp
import json
from typing import Optional, Callable, Dict, Any, Set
from dataclasses import dataclass
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class Level1Quote:
    """Level1 market data structure"""
    epic: str  # Instrument identifier
    symbol: str  # Instrument symbol
    bid: float  # Current bid price
    ask: float  # Current ask (offer) price
    last_price: float  # Mid price or last trade
    percentage_change: float  # Percentage change
    timestamp: datetime  # Data timestamp

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'epic': self.epic,
            'symbol': self.symbol,
            'bid': self.bid,
            'ask': self.ask,
            'last_price': self.last_price,
            'percentage_change': self.percentage_change,
            'timestamp': self.timestamp.isoformat()
        }


class CapitalComAuthenticationError(Exception):
    """Raised when authentication fails"""
    pass


class CapitalComConnectionError(Exception):
    """Raised when connection fails"""
    pass


class CapitalComLevel1Quotes:
    """
    Asynchronous handler for Capital.com Level1 quotes.

    Manages session authentication and polls market data from Capital.com API.

    Example:
        async with CapitalComLevel1Quotes(
            api_key='your_api_key',
            identifier='your_email@example.com',
            password='your_password'
        ) as quotes:
            await quotes.subscribe(['CS.D', 'AAPL.US'])
            async for quote in quotes:
                print(f"{quote.symbol}: {quote.bid}/{quote.ask}")
    """

    BASE_URL = 'https://api-capital.backend-capital.com/api/v1'
    SESSION_TIMEOUT = 600  # 10 minutes - session validity period
    POLL_INTERVAL = 1.0  # Poll every 1 second
    MAX_SYMBOLS_PER_REQUEST = 50

    def __init__(
        self,
        api_key: str,
        identifier: str,
        password: str,
        poll_interval: float = POLL_INTERVAL,
        last_price_mode: str = 'mid'
    ):
        """
        Initialize Capital.com Level1 quotes handler.

        Args:
            api_key: Capital.com API key
            identifier: Email/username for authentication
            password: Account password
            poll_interval: Data polling interval in seconds (default: 1.0)
            last_price_mode: How to calculate last price ('bid', 'ask', 'mid')
        """
        self.api_key = api_key
        self.identifier = identifier
        self.password = password
        self.poll_interval = poll_interval
        self.last_price_mode = last_price_mode

        self.security_token: Optional[str] = None
        self.cst: Optional[str] = None
        self.last_session_check: Optional[float] = None

        self._session: Optional[aiohttp.ClientSession] = None
        self._subscribed_symbols: Set[str] = set()
        self._should_poll = False
        self._callback: Optional[Callable[[Level1Quote], None]] = None
        self._on_error: Optional[Callable[[Exception], None]] = None

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()

    async def __aiter__(self):
        """Async iterator support"""
        return self

    async def __anext__(self) -> Level1Quote:
        """Fetch next Level1 quote"""
        while True:
            await self.ensure_session()
            quotes = await self._fetch_market_data()

            if quotes:
                return quotes[0]

            await asyncio.sleep(self.poll_interval)

    async def connect(self) -> None:
        """Establish connection and authenticate"""
        self._session = aiohttp.ClientSession()
        await self.ensure_session()
        logger.info('Connected to Capital.com API')

    async def disconnect(self) -> None:
        """Close connection"""
        self._should_poll = False
        if self._session:
            await self._session.close()
        logger.info('Disconnected from Capital.com API')

    async def subscribe(self, symbols: list[str], callback: Optional[Callable[[Level1Quote], None]] = None) -> None:
        """
        Subscribe to symbols for Level1 data.

        Args:
            symbols: List of epic symbols (e.g., ['CS.D', 'AAPL.US'])
            callback: Optional callback function for each quote
        """
        self._subscribed_symbols.update(symbols)
        self._callback = callback
        self._should_poll = True
        logger.info(f'Subscribed to {len(symbols)} symbols')

    async def unsubscribe(self, symbols: list[str]) -> None:
        """Unsubscribe from symbols"""
        self._subscribed_symbols.difference_update(symbols)
        logger.info(f'Unsubscribed from {len(symbols)} symbols')

    async def ensure_session(self) -> None:
        """
        Ensure valid authentication session.

        Checks if session is still valid (within SESSION_TIMEOUT).
        If not, performs new authentication.

        Raises:
            CapitalComAuthenticationError: If authentication fails
            CapitalComConnectionError: If connection fails
        """
        import time

        now = time.time()

        # Skip if session is still valid
        if (self.last_session_check is not None and
            now - self.last_session_check < self.SESSION_TIMEOUT - 60):
            return

        if not self._session:
            raise CapitalComConnectionError('Session not initialized')

        try:
            headers = {
                'Content-Type': 'application/json',
                'X-CAP-API-KEY': self.api_key
            }

            body = {
                'identifier': self.identifier,
                'password': self.password,
                'encryptedPassword': False
            }

            async with self._session.post(
                f'{self.BASE_URL}/session',
                headers=headers,
                json=body,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status != 200:
                    raise CapitalComAuthenticationError(
                        f'Authentication failed: {response.status}'
                    )

                data = await response.json()

                # Extract security tokens from response headers
                response_headers = data.get('headers', [])

                security_token = None
                cst = None

                for header_info in response_headers:
                    header_name = header_info.get('header', '').upper()
                    if header_name == 'X-SECURITY-TOKEN':
                        security_token = header_info.get('value')
                    elif header_name == 'CST':
                        cst = header_info.get('value')

                if not security_token or not cst:
                    raise CapitalComAuthenticationError(
                        'Missing security tokens in response'
                    )

                self.security_token = security_token
                self.cst = cst
                self.last_session_check = now

                logger.info('Session authenticated successfully')

        except asyncio.TimeoutError:
            raise CapitalComConnectionError('Authentication request timeout')
        except aiohttp.ClientError as e:
            raise CapitalComConnectionError(f'Connection error: {e}')

    def _calculate_last_price(self, bid: float, ask: float) -> float:
        """Calculate last price based on configured mode"""
        if self.last_price_mode == 'bid':
            return bid
        elif self.last_price_mode == 'ask':
            return ask
        else:  # 'mid'
            return (bid + ask) / 2

    async def _fetch_market_data(self) -> list[Level1Quote]:
        """
        Fetch Level1 market data for subscribed symbols.

        Returns:
            List of Level1Quote objects
        """
        if not self._subscribed_symbols:
            return []

        if not self._session:
            raise CapitalComConnectionError('Session not initialized')

        try:
            quotes = []

            # Process symbols in batches
            symbols_list = list(self._subscribed_symbols)
            for i in range(0, len(symbols_list), self.MAX_SYMBOLS_PER_REQUEST):
                batch = symbols_list[i:i + self.MAX_SYMBOLS_PER_REQUEST]
                epics_param = ','.join(batch)

                headers = {
                    'X-SECURITY-TOKEN': self.security_token,
                    'CST': self.cst
                }

                async with self._session.get(
                    f'{self.BASE_URL}/markets',
                    params={'epics': epics_param},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 401:
                        # Force re-authentication
                        self.last_session_check = None
                        raise CapitalComAuthenticationError('Session expired')

                    if response.status != 200:
                        logger.warning(f'API returned status {response.status}')
                        continue

                    data = await response.json()
                    market_details = data.get('marketDetails', [])

                    for market in market_details:
                        try:
                            quote = self._parse_market_data(market)
                            quotes.append(quote)

                            # Call callback if set
                            if self._callback:
                                self._callback(quote)

                        except Exception as e:
                            logger.error(f'Error parsing market data: {e}')
                            continue

            return quotes

        except asyncio.TimeoutError:
            raise CapitalComConnectionError('Market data request timeout')
        except aiohttp.ClientError as e:
            raise CapitalComConnectionError(f'Connection error: {e}')
        except json.JSONDecodeError as e:
            raise CapitalComConnectionError(f'JSON decode error: {e}')

    def _parse_market_data(self, market: Dict[str, Any]) -> Level1Quote:
        """
        Parse market data from API response.

        Args:
            market: Market data dictionary from API

        Returns:
            Level1Quote object
        """
        snapshot = market.get('snapshot', {})
        instrument = market.get('instrument', {})

        bid = snapshot.get('bid', 0.0)
        ask = snapshot.get('offer', 0.0)
        percentage_change = snapshot.get('percentageChange', 0.0)

        epic = instrument.get('epic', '')
        symbol = market.get('displayName', epic)

        quote = Level1Quote(
            epic=epic,
            symbol=symbol,
            bid=float(bid),
            ask=float(ask),
            last_price=self._calculate_last_price(float(bid), float(ask)),
            percentage_change=float(percentage_change),
            timestamp=datetime.utcnow()
        )

        return quote

    async def start_polling(self) -> None:
        """Start continuous polling of subscribed symbols"""
        self._should_poll = True

        while self._should_poll:
            try:
                await self.ensure_session()
                await self._fetch_market_data()
                await asyncio.sleep(self.poll_interval)

            except CapitalComAuthenticationError as e:
                logger.error(f'Authentication error: {e}')
                self._should_poll = False

                if self._on_error:
                    self._on_error(e)
                break

            except CapitalComConnectionError as e:
                logger.warning(f'Connection error: {e}, retrying...')
                await asyncio.sleep(self.poll_interval * 2)

            except Exception as e:
                logger.error(f'Unexpected error: {e}')
                await asyncio.sleep(self.poll_interval)

    def set_error_handler(self, callback: Callable[[Exception], None]) -> None:
        """Set error callback handler"""
        self._on_error = callback


# Convenience function for simple use case
async def get_level1_quote(
    api_key: str,
    identifier: str,
    password: str,
    symbol: str
) -> Level1Quote:
    """
    Convenience function to get a single Level1 quote.

    Args:
        api_key: Capital.com API key
        identifier: Email/username
        password: Password
        symbol: Epic symbol (e.g., 'CS.D')

    Returns:
        Level1Quote object
    """
    async with CapitalComLevel1Quotes(api_key, identifier, password) as quotes:
        await quotes.subscribe([symbol])
        return await quotes.__anext__()
