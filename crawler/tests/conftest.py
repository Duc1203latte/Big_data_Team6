"""Cấu hình chung cho pytest."""


def pytest_configure(config):
    # Test cần broker Kafka thật (localhost:9092). Không có broker thì test tự bỏ qua (skip).
    # Chạy riêng: pytest -m kafka ; bỏ hẳn: pytest -m "not kafka"
    config.addinivalue_line("markers", "kafka: needs a running Kafka broker on localhost:9092")
