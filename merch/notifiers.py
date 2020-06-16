from telegram_interactions.bot import TelegramBot


def send_bad_data_error(
    token: str,
    chat_id: str,
    **context
) -> None:
    tg_bot = TelegramBot(token)

    task_instance = context['ti']
    message_text = task_instance.xcom_pull(key='bad_data_error_message')

    if message_text is None:
        return

    message_text = message_text.replace(r'_', r'\_')

    tg_bot.send_message(
        chat_id=chat_id,
        message_text=message_text
    )
