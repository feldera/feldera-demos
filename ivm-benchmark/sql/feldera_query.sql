SELECT count(DISTINCT d.cc_num) AS n_alerts
FROM fraud_alert_details d
JOIN max_transaction_ts m ON d.ts >= m.max_ts - INTERVAL '30' MINUTE
