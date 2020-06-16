from .notifiers import send_bad_data_error


def on_failure_callback(token_id, chat_id, context):
    send_bad_data_error(token_id, chat_id, **context)