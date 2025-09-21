# tests/unit/utils/test_logging_config.py

import logging
from src.logging_config import create_fallback_logger, configure_logger


def test_create_fallback_logger_outputs_json(capsys):
    logger = create_fallback_logger(logging.DEBUG)
    logger.info("test message")

    captured = capsys.readouterr()
    assert "test message" in captured.out


def test_configure_logger_with_dict(log_config):
    logger = configure_logger(logging_config=log_config)
    assert isinstance(logger, logging.Logger)
    logger.debug("hello world")


def test_configure_logger_fallback(monkeypatch):
    # ensure no handlers so it uses fallback
    logging.getLogger().handlers.clear()
    logger = configure_logger()
    logger.warning("fallback warning")
    assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)
