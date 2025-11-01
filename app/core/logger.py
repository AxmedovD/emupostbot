from logging import INFO, Logger, getLogger, Formatter
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOG_DIR = BASE_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# Log format
LOG_FORMAT: str = "%(asctime)s | %(levelname)-8s | %(process)d | %(name)s:%(lineno)d | %(message)s"
DATE_FORMAT: str = '%Y-%m-%d %H:%M:%S'


def setup_logger(
        name: str,
        log_file: str = None,
        level: int = INFO,
        backup_count: int = 30
) -> Logger:
    """
    Setup logger with file and optional console handlers

    Args:
        name: Logger name
        log_file: Log file name (without path)
        level: Logging level
        backup_count: Number of backup log files to keep

    Returns:
        Configured Logger instance
    """
    _logger = getLogger(name)
    _logger.setLevel(level)

    # Eski handlerlarni tozalash (duplicate loglarni oldini olish)
    if _logger.handlers:
        _logger.handlers.clear()

    # Formatter yaratish
    formatter = Formatter(LOG_FORMAT, DATE_FORMAT)

    # File handler - agar log_file berilgan bo'lsa
    if log_file:
        file_handler = TimedRotatingFileHandler(
            filename=LOG_DIR / log_file,
            when='midnight',  # Har kecha yarim tunda rotate
            interval=1,  # Har 1 kun
            backupCount=backup_count,  # Necha kunlik saqlanadi
            encoding='UTF-8',
            utc=False  # Server vaqtidan foydalanish
        )
        file_handler.suffix = '%Y-%m-%d'  # Fayl nomi formati: app.log.2025-10-31
        file_handler.setFormatter(formatter)
        _logger.addHandler(file_handler)

    # Parent loggerga propagate qilmaslik (duplicate loglarni oldini olish)
    _logger.propagate = False

    return _logger


# Alohida loggerlar - har bir komponent uchun
logger: Logger = setup_logger(name='app', log_file='app.log')
telegram_logger: Logger = setup_logger(name='telegram', log_file='telegram.log')
webhook_logger: Logger = setup_logger(name='webhook', log_file='webhook.log')
api_logger: Logger = setup_logger(name='api', log_file='api.log')
db_logger: Logger = setup_logger(name='db', log_file='db.log')
bot_logger: Logger = setup_logger(name='bot', log_file='bot.log')
service_logger: Logger = setup_logger(name='service', log_file='service.log')
