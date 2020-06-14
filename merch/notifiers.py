from pathlib import Path

from telegram_interactions.bot import TelegramBot


def send_message_from_file(
    token: str,
    chat_id: str,
    message_file_path: Path
) -> None:
    tg_bot = TelegramBot(token)

    with open(message_file_path) as f:
        message_text = f.read().replace(r'_', r'\_')

    tg_bot.send_message(
        chat_id=chat_id,
        message_text=message_text
    )
