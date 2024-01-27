"""contains notification logic"""
import logging
import os
from telegram import Bot
from telegram.constants import ParseMode

logger = logging.getLogger(os.path.basename(__file__))

async def send_telegram_message(
    token: str,
    group_chat_id: str,
    message: str
) -> None:
    """
    Sends a message to a telegram group chat.

    Args:
        token (str): Token from Telegram BotFather.
        group_chat_id (str): Group chat id.
        message (str): Message to send.
    """
    logger.info("running send_telegram_message func")
    bot = Bot(token=token)
    logger.debug("sending message")
    await bot.send_message(
        chat_id=group_chat_id, 
        text=message,
        parse_mode=ParseMode.HTML
    )
    logging.info("message has been sent")
