"""
Tests for Capital.com Level1 Quotes module

Run with: python -m pytest test/capitalcom_test.py -v
"""

import pytest
import asyncio
import json
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import aiohttp
import websockets

from lib.brokers.capitalcom import (
    CapitalComLevel1Quotes,
    CapitalComLevel1WebSocket,
    Level1Quote,
    CapitalComAuthenticationError,
    CapitalComConnectionError,
    CapitalComWebSocketError
)


class TestLevel1Quote:
    """Tests for Level1Quote data class"""

    def test_level1_quote_creation(self):
        """Test creating a Level1Quote"""
        quote = Level1Quote(
            epic='CS.D',
            symbol='Capital.com',
            bid=5.10,
            ask=5.15,
            last_price=5.125,
            percentage_change=2.5,
            timestamp=datetime.utcnow()
        )

        assert quote.epic == 'CS.D'
        assert quote.symbol == 'Capital.com'
        assert quote.bid == 5.10
        assert quote.ask == 5.15
        assert quote.last_price == 5.125
        assert quote.percentage_change == 2.5

    def test_level1_quote_to_dict(self):
        """Test converting quote to dictionary"""
        now = datetime.utcnow()
        quote = Level1Quote(
            epic='CS.D',
            symbol='Capital.com',
            bid=5.10,
            ask=5.15,
            last_price=5.125,
            percentage_change=2.5,
            timestamp=now
        )

        quote_dict = quote.to_dict()

        assert quote_dict['epic'] == 'CS.D'
        assert quote_dict['bid'] == 5.10
        assert quote_dict['ask'] == 5.15
        assert 'timestamp' in quote_dict


class TestCapitalComLevel1Quotes:
    """Tests for CapitalComLevel1Quotes class"""

    @pytest.fixture
    def quotes_instance(self):
        """Create a CapitalComLevel1Quotes instance"""
        return CapitalComLevel1Quotes(
            api_key='test_key',
            identifier='test@example.com',
            password='test_password'
        )

    def test_initialization(self, quotes_instance):
        """Test instance initialization"""
        assert quotes_instance.api_key == 'test_key'
        assert quotes_instance.identifier == 'test@example.com'
        assert quotes_instance.password == 'test_password'
        assert quotes_instance.poll_interval == 1.0
        assert quotes_instance.last_price_mode == 'mid'
        assert quotes_instance.security_token is None
        assert quotes_instance.cst is None

    def test_initialization_with_options(self):
        """Test initialization with custom options"""
        quotes = CapitalComLevel1Quotes(
            api_key='test_key',
            identifier='test@example.com',
            password='test_password',
            poll_interval=0.5,
            last_price_mode='bid'
        )

        assert quotes.poll_interval == 0.5
        assert quotes.last_price_mode == 'bid'

    def test_calculate_last_price_bid(self, quotes_instance):
        """Test last price calculation in bid mode"""
        quotes_instance.last_price_mode = 'bid'
        last_price = quotes_instance._calculate_last_price(5.10, 5.15)
        assert last_price == 5.10

    def test_calculate_last_price_ask(self, quotes_instance):
        """Test last price calculation in ask mode"""
        quotes_instance.last_price_mode = 'ask'
        last_price = quotes_instance._calculate_last_price(5.10, 5.15)
        assert last_price == 5.15

    def test_calculate_last_price_mid(self, quotes_instance):
        """Test last price calculation in mid mode"""
        quotes_instance.last_price_mode = 'mid'
        last_price = quotes_instance._calculate_last_price(5.10, 5.15)
        assert last_price == 5.125

    def test_parse_market_data(self, quotes_instance):
        """Test parsing market data from API response"""
        market_data = {
            'snapshot': {
                'bid': 5.10,
                'offer': 5.15,
                'percentageChange': 2.5
            },
            'instrument': {
                'epic': 'CS.D'
            },
            'displayName': 'Capital.com'
        }

        quote = quotes_instance._parse_market_data(market_data)

        assert quote.epic == 'CS.D'
        assert quote.symbol == 'Capital.com'
        assert quote.bid == 5.10
        assert quote.ask == 5.15
        assert quote.percentage_change == 2.5
        assert quote.last_price == 5.125

    @pytest.mark.asyncio
    async def test_subscribe(self, quotes_instance):
        """Test subscribing to symbols"""
        def on_quote(quote):
            pass

        await quotes_instance.subscribe(['CS.D', 'AAPL.US'], callback=on_quote)

        assert 'CS.D' in quotes_instance._subscribed_symbols
        assert 'AAPL.US' in quotes_instance._subscribed_symbols
        assert quotes_instance._callback == on_quote
        assert quotes_instance._should_poll is True

    @pytest.mark.asyncio
    async def test_unsubscribe(self, quotes_instance):
        """Test unsubscribing from symbols"""
        await quotes_instance.subscribe(['CS.D', 'AAPL.US'])
        await quotes_instance.unsubscribe(['CS.D'])

        assert 'CS.D' not in quotes_instance._subscribed_symbols
        assert 'AAPL.US' in quotes_instance._subscribed_symbols

    def test_set_error_handler(self, quotes_instance):
        """Test setting error handler"""
        def on_error(error):
            pass

        quotes_instance.set_error_handler(on_error)
        assert quotes_instance._on_error == on_error

    @pytest.mark.asyncio
    async def test_ensure_session_not_initialized(self, quotes_instance):
        """Test ensure_session when session not initialized"""
        with pytest.raises(CapitalComConnectionError):
            await quotes_instance.ensure_session()

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_ensure_session_authentication(self, mock_session_class, quotes_instance):
        """Test session authentication"""
        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            'headers': [
                {'header': 'X-SECURITY-TOKEN', 'value': 'test_token'},
                {'header': 'CST', 'value': 'test_cst'}
            ]
        })

        mock_session.post = AsyncMock(return_value=mock_response)
        quotes_instance._session = mock_session

        await quotes_instance.ensure_session()

        assert quotes_instance.security_token == 'test_token'
        assert quotes_instance.cst == 'test_cst'

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_ensure_session_missing_tokens(self, mock_session_class, quotes_instance):
        """Test authentication failure when tokens are missing"""
        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={'headers': []})

        mock_session.post = AsyncMock(return_value=mock_response)
        quotes_instance._session = mock_session

        with pytest.raises(CapitalComAuthenticationError):
            await quotes_instance.ensure_session()

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_ensure_session_auth_failure(self, mock_session_class, quotes_instance):
        """Test authentication failure"""
        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 401

        mock_session.post = AsyncMock(return_value=mock_response)
        quotes_instance._session = mock_session

        with pytest.raises(CapitalComAuthenticationError):
            await quotes_instance.ensure_session()

    @pytest.mark.asyncio
    async def test_fetch_market_data_no_symbols(self, quotes_instance):
        """Test fetching market data with no subscribed symbols"""
        market_data = await quotes_instance._fetch_market_data()
        assert market_data == []

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_fetch_market_data(self, mock_session_class, quotes_instance):
        """Test fetching market data"""
        quotes_instance.security_token = 'test_token'
        quotes_instance.cst = 'test_cst'
        quotes_instance._subscribed_symbols = {'CS.D', 'AAPL.US'}

        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            'marketDetails': [
                {
                    'snapshot': {'bid': 5.10, 'offer': 5.15, 'percentageChange': 2.5},
                    'instrument': {'epic': 'CS.D'},
                    'displayName': 'Capital.com'
                }
            ]
        })

        mock_session.get = AsyncMock(return_value=mock_response)
        quotes_instance._session = mock_session

        market_data = await quotes_instance._fetch_market_data()

        assert len(market_data) == 1
        assert market_data[0].epic == 'CS.D'
        assert market_data[0].bid == 5.10

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_context_manager(self, mock_session_class, quotes_instance):
        """Test context manager interface"""
        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            'headers': [
                {'header': 'X-SECURITY-TOKEN', 'value': 'test_token'},
                {'header': 'CST', 'value': 'test_cst'}
            ]
        })

        mock_session.post = AsyncMock(return_value=mock_response)
        mock_session.close = AsyncMock()

        with patch('aiohttp.ClientSession', return_value=mock_session):
            async with CapitalComLevel1Quotes(
                api_key='test_key',
                identifier='test@example.com',
                password='test_password'
            ) as quotes:
                assert quotes._session is not None

            # Verify close was called
            mock_session.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling_in_fetch(self, quotes_instance):
        """Test error handling during market data fetch"""
        quotes_instance.security_token = 'test_token'
        quotes_instance.cst = 'test_cst'
        quotes_instance._subscribed_symbols = {'CS.D'}

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(side_effect=aiohttp.ClientError('Connection failed'))

        quotes_instance._session = mock_session

        with pytest.raises(CapitalComConnectionError):
            await quotes_instance._fetch_market_data()

    def test_base_url_constant(self):
        """Test BASE_URL constant"""
        assert CapitalComLevel1Quotes.BASE_URL == 'https://api-capital.backend-capital.com/api/v1'

    def test_session_timeout_constant(self):
        """Test SESSION_TIMEOUT constant"""
        assert CapitalComLevel1Quotes.SESSION_TIMEOUT == 600

    def test_max_symbols_per_request(self):
        """Test MAX_SYMBOLS_PER_REQUEST constant"""
        assert CapitalComLevel1Quotes.MAX_SYMBOLS_PER_REQUEST == 50


class TestCapitalComLevel1WebSocket:
    """Tests for CapitalComLevel1WebSocket class"""

    @pytest.fixture
    def ws_instance(self):
        """Create a WebSocket instance"""
        return CapitalComLevel1WebSocket(
            api_key='test_key',
            identifier='test@example.com',
            password='test_password'
        )

    def test_websocket_initialization(self, ws_instance):
        """Test WebSocket instance initialization"""
        assert ws_instance.api_key == 'test_key'
        assert ws_instance.identifier == 'test@example.com'
        assert ws_instance.password == 'test_password'
        assert ws_instance.last_price_mode == 'mid'
        assert ws_instance.security_token is None
        assert ws_instance.cst is None
        assert len(ws_instance._subscribed_symbols) == 0

    def test_websocket_constants(self):
        """Test WebSocket constants"""
        assert CapitalComLevel1WebSocket.WS_URL == 'wss://api-streaming-capital.backend-capital.com/connect'
        assert CapitalComLevel1WebSocket.MAX_SUBSCRIPTIONS == 40
        assert CapitalComLevel1WebSocket.PING_INTERVAL == 600

    def test_parse_streaming_message_valid(self, ws_instance):
        """Test parsing valid streaming message"""
        message = json.dumps({
            'epic': 'CS.D',
            'symbol': 'Capital.com',
            'price': {'bid': 5.10, 'ask': 5.15},
            'percentageChange': 2.5
        })

        quote = ws_instance._parse_streaming_message(message)

        assert quote is not None
        assert quote.epic == 'CS.D'
        assert quote.bid == 5.10
        assert quote.ask == 5.15
        assert quote.last_price == 5.125

    def test_parse_streaming_message_invalid(self, ws_instance):
        """Test parsing invalid streaming message"""
        message = json.dumps({'status': 'connected'})
        quote = ws_instance._parse_streaming_message(message)
        assert quote is None

    def test_parse_streaming_message_malformed(self, ws_instance):
        """Test parsing malformed JSON"""
        message = 'not valid json'
        quote = ws_instance._parse_streaming_message(message)
        assert quote is None

    @pytest.mark.asyncio
    async def test_websocket_subscribe_exceeds_limit(self, ws_instance):
        """Test subscription exceeds maximum limit"""
        symbols = [f'SYM{i}.US' for i in range(50)]  # 50 symbols

        with pytest.raises(CapitalComWebSocketError):
            await ws_instance.subscribe(symbols)

    @pytest.mark.asyncio
    async def test_websocket_subscribe(self, ws_instance):
        """Test WebSocket subscription"""
        def on_quote(quote):
            pass

        await ws_instance.subscribe(['CS.D', 'AAPL.US'], callback=on_quote)

        assert 'CS.D' in ws_instance._subscribed_symbols
        assert 'AAPL.US' in ws_instance._subscribed_symbols
        assert ws_instance._callback == on_quote
        assert ws_instance._should_stream is True

    @pytest.mark.asyncio
    async def test_websocket_unsubscribe(self, ws_instance):
        """Test WebSocket unsubscription"""
        await ws_instance.subscribe(['CS.D', 'AAPL.US'])
        await ws_instance.unsubscribe(['CS.D'])

        assert 'CS.D' not in ws_instance._subscribed_symbols
        assert 'AAPL.US' in ws_instance._subscribed_symbols

    def test_websocket_error_handler(self, ws_instance):
        """Test setting error handler"""
        def on_error(error):
            pass

        ws_instance.set_error_handler(on_error)
        assert ws_instance._on_error == on_error

    def test_calculate_last_price_bid(self, ws_instance):
        """Test last price calculation in bid mode"""
        ws_instance.last_price_mode = 'bid'
        last_price = ws_instance._calculate_last_price(5.10, 5.15)
        assert last_price == 5.10

    def test_calculate_last_price_ask(self, ws_instance):
        """Test last price calculation in ask mode"""
        ws_instance.last_price_mode = 'ask'
        last_price = ws_instance._calculate_last_price(5.10, 5.15)
        assert last_price == 5.15

    def test_calculate_last_price_mid(self, ws_instance):
        """Test last price calculation in mid mode"""
        ws_instance.last_price_mode = 'mid'
        last_price = ws_instance._calculate_last_price(5.10, 5.15)
        assert last_price == 5.125

    @pytest.mark.asyncio
    async def test_websocket_ensure_session_not_initialized(self, ws_instance):
        """Test ensure_session when HTTP session not initialized"""
        with pytest.raises(CapitalComConnectionError):
            await ws_instance.ensure_session()

    @pytest.mark.asyncio
    async def test_websocket_ensure_session_authentication(self, ws_instance):
        """Test WebSocket session authentication"""
        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            'headers': [
                {'header': 'X-SECURITY-TOKEN', 'value': 'ws_token'},
                {'header': 'CST', 'value': 'ws_cst'}
            ]
        })

        mock_session.post = AsyncMock(return_value=mock_response)
        ws_instance._http_session = mock_session

        await ws_instance.ensure_session()

        assert ws_instance.security_token == 'ws_token'
        assert ws_instance.cst == 'ws_cst'

    @pytest.mark.asyncio
    async def test_websocket_send_subscription(self, ws_instance):
        """Test sending subscription message"""
        ws_instance.security_token = 'test_token'
        ws_instance.cst = 'test_cst'

        mock_ws = AsyncMock()
        mock_ws.closed = False
        mock_ws.send = AsyncMock()

        ws_instance._ws = mock_ws

        await ws_instance._send_subscription(['CS.D'])

        mock_ws.send.assert_called_once()
        call_args = mock_ws.send.call_args[0][0]
        message = json.loads(call_args)

        assert message['destination'] == 'marketData.subscribe'
        assert message['epics'] == ['CS.D']
        assert message['securityToken'] == 'test_token'
        assert message['cst'] == 'test_cst'

    @pytest.mark.asyncio
    async def test_websocket_send_unsubscription(self, ws_instance):
        """Test sending unsubscription message"""
        ws_instance.security_token = 'test_token'
        ws_instance.cst = 'test_cst'

        mock_ws = AsyncMock()
        mock_ws.closed = False
        mock_ws.send = AsyncMock()

        ws_instance._ws = mock_ws

        await ws_instance._send_unsubscription(['CS.D'])

        mock_ws.send.assert_called_once()
        call_args = mock_ws.send.call_args[0][0]
        message = json.loads(call_args)

        assert message['destination'] == 'marketData.unsubscribe'
        assert message['epics'] == ['CS.D']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
