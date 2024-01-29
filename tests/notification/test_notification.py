import pytest
from unittest.mock import patch, AsyncMock
from src.notification import send_telegram_message

@pytest.mark.asyncio
@patch('src.notification.Bot')
async def test_send_telegram_message(mock_bot_class):
    # Create a mock for the send_message method
    mock_bot_instance = mock_bot_class.return_value
    mock_bot_instance.send_message = AsyncMock()

    # Test parameters
    token = 'test_token'
    group_chat_id = 'test_chat_id'
    message = 'Hello, World!'

    # Call the async function
    await send_telegram_message(token, group_chat_id, message)

    # Assertions
    mock_bot_instance.send_message.assert_called_once_with(
        chat_id=group_chat_id, 
        text=message,
        parse_mode='HTML'
    )
