from common.speed_io import evaluate_anomaly_rules, fmt_dt, load_anomaly_config


def test_historical_deviation_rule():
    cfg = load_anomaly_config()
    is_anom, score, reasons = evaluate_anomaly_rules(
        price_current=7_000_000,
        price_original=20_000_000,
        discount_pct=65.0,
        baseline_median=20_000_000.0,
        baseline_mad=200_000.0,
        other_platform_min_price=None,
        config=cfg,
    )
    assert is_anom == 1
    assert "HISTORICAL_DEVIATION" in reasons
    assert score > 0


def test_cross_platform_cheaper_rule():
    cfg = load_anomaly_config()
    is_anom, score, reasons = evaluate_anomaly_rules(
        price_current=12_000_000,
        price_original=12_000_000,
        discount_pct=0.0,
        baseline_median=12_000_000.0,
        baseline_mad=100_000.0,
        other_platform_min_price=18_000_000,
        config=cfg,
    )
    assert is_anom == 1
    assert "CROSS_PLATFORM_CHEAPER" in reasons


def test_fake_promotion_rule():
    cfg = load_anomaly_config()
    is_anom, score, reasons = evaluate_anomaly_rules(
        price_current=15_000_000,
        price_original=32_000_000,
        discount_pct=53.12,
        baseline_median=15_000_000.0,
        baseline_mad=150_000.0,
        other_platform_min_price=15_200_000,
        config=cfg,
    )
    assert is_anom == 1
    assert "FAKE_PROMOTION" in reasons


def test_normal_price_not_flagged():
    cfg = load_anomaly_config()
    is_anom, score, reasons = evaluate_anomaly_rules(
        price_current=14_900_000,
        price_original=15_500_000,
        discount_pct=3.87,
        baseline_median=15_000_000.0,
        baseline_mad=200_000.0,
        other_platform_min_price=15_000_000,
        config=cfg,
    )
    assert is_anom == 0
    assert score == 0.0
    assert reasons == []


def test_fmt_dt_converts_timezone():
    assert fmt_dt("2026-11-15T07:00:00+07:00") == "2026-11-15 00:00:00.000"
    assert fmt_dt("2026-11-15T00:00:00Z") == "2026-11-15 00:00:00.000"