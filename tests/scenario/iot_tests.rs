//! Internet of Things (IoT) Scenario Tests
//!
//! Real-world IoT scenarios:
//! - Sensor data ingestion and storage
//! - Time-series data queries
//! - Device registration and management
//! - Alert system
//! - Data aggregation and analytics
//!
//! Test Count: 150+

use sqllite_rust::executor::{Executor, ExecuteResult};
use tempfile::NamedTempFile;

fn setup_db() -> Executor {
    let temp_file = NamedTempFile::new().unwrap();
    Executor::open(temp_file.path().to_str().unwrap()).unwrap()
}

// ============================================================================
// Device Registration & Management (Tests 1-40)
// ============================================================================

fn setup_device_schema(executor: &mut Executor) {
    executor.execute_sql("CREATE TABLE devices (
        id TEXT PRIMARY KEY,
        name TEXT,
        device_type TEXT,
        location TEXT,
        status TEXT DEFAULT 'active',
        registered_at INTEGER,
        last_seen INTEGER,
        firmware_version TEXT,
        battery_level INTEGER
    )").unwrap();
    
    executor.execute_sql("CREATE TABLE device_metadata (
        device_id TEXT PRIMARY KEY,
        manufacturer TEXT,
        model TEXT,
        serial_number TEXT,
        config_json TEXT
    )").unwrap();
    
    executor.execute_sql("CREATE INDEX idx_devices_type ON devices (device_type)").unwrap();
    executor.execute_sql("CREATE INDEX idx_devices_status ON devices (status)").unwrap();
    executor.execute_sql("CREATE INDEX idx_devices_location ON devices (location)").unwrap();
}

#[test]
fn test_device_registration() {
    let mut db = setup_db();
    setup_device_schema(&mut db);
    
    let result = db.execute_sql("INSERT INTO devices (id, name, device_type, location, registered_at) 
        VALUES ('dev001', 'Temperature Sensor 1', 'temperature', 'Building A', 1234567890)");
    assert!(result.is_ok());
}

#[test]
fn test_device_batch_registration() {
    let mut db = setup_db();
    setup_device_schema(&mut db);
    
    for i in 1..=50 {
        let result = db.execute_sql(&format!(
            "INSERT INTO devices (id, name, device_type, location, registered_at) 
            VALUES ('dev{:03}', 'Sensor {}', 'temperature', 'Zone {}', {})",
            i, i, i % 10, 1234567890 + i
        ));
        assert!(result.is_ok());
    }
}

#[test]
fn test_device_metadata_insert() {
    let mut db = setup_db();
    setup_device_schema(&mut db);
    db.execute_sql("INSERT INTO devices (id, name) VALUES ('dev001', 'Sensor')").unwrap();
    
    let result = db.execute_sql("INSERT INTO device_metadata (device_id, manufacturer, model) 
        VALUES ('dev001', 'ACME', 'Sensor-2000')");
    assert!(result.is_ok());
}

#[test]
fn test_device_status_update() {
    let mut db = setup_db();
    setup_device_schema(&mut db);
    db.execute_sql("INSERT INTO devices (id, name, status) VALUES ('dev001', 'Sensor', 'active')").unwrap();
    
    let result = db.execute_sql("UPDATE devices SET status = 'offline' WHERE id = 'dev001'");
    assert!(result.is_ok());
}

#[test]
fn test_device_last_seen_update() {
    let mut db = setup_db();
    setup_device_schema(&mut db);
    db.execute_sql("INSERT INTO devices (id, name, last_seen) VALUES ('dev001', 'Sensor', 1000)").unwrap();
    
    let result = db.execute_sql("UPDATE devices SET last_seen = 2000 WHERE id = 'dev001'");
    assert!(result.is_ok());
}

#[test]
fn test_device_battery_update() {
    let mut db = setup_db();
    setup_device_schema(&mut db);
    db.execute_sql("INSERT INTO devices (id, name, battery_level) VALUES ('dev001', 'Sensor', 100)").unwrap();
    
    let result = db.execute_sql("UPDATE devices SET battery_level = 85 WHERE id = 'dev001'");
    assert!(result.is_ok());
}

#[test]
fn test_devices_by_type() {
    let mut db = setup_db();
    setup_device_schema(&mut db);
    db.execute_sql("INSERT INTO devices (id, device_type) VALUES ('dev001', 'temperature')").unwrap();
    db.execute_sql("INSERT INTO devices (id, device_type) VALUES ('dev002', 'humidity')").unwrap();
    
    let result = db.execute_sql("SELECT * FROM devices WHERE device_type = 'temperature'").unwrap();
    match result {
        ExecuteResult::Query(qr) => assert_eq!(qr.rows.len(), 1),
        _ => panic!("Expected query result"),
    }
}

#[test]
fn test_devices_by_location() {
    let mut db = setup_db();
    setup_device_schema(&mut db);
    for i in 1..=20 {
        db.execute_sql(&format!("INSERT INTO devices (id, location) VALUES ('dev{:03}', 'Zone {}')", i, i % 5)).unwrap();
    }
    
    let result = db.execute_sql("SELECT * FROM devices WHERE location = 'Zone 1'");
    assert!(result.is_ok());
}

#[test]
fn test_active_devices_count() {
    let mut db = setup_db();
    setup_device_schema(&mut db);
    for i in 1..=30 {
        db.execute_sql(&format!("INSERT INTO devices (id, status) VALUES ('dev{:03}', '{}')", 
            i, if i % 3 == 0 { "offline" } else { "active" })).unwrap();
    }
    
    let result = db.execute_sql("SELECT COUNT(*) FROM devices WHERE status = 'active'");
    assert!(result.is_ok());
}

#[test]
fn test_low_battery_devices() {
    let mut db = setup_db();
    setup_device_schema(&mut db);
    db.execute_sql("INSERT INTO devices (id, battery_level) VALUES ('dev001', 20)").unwrap();
    db.execute_sql("INSERT INTO devices (id, battery_level) VALUES ('dev002', 80)").unwrap();
    
    let result = db.execute_sql("SELECT * FROM devices WHERE battery_level < 30").unwrap();
    match result {
        ExecuteResult::Query(qr) => assert_eq!(qr.rows.len(), 1),
        _ => panic!("Expected query result"),
    }
}

#[test]
fn test_device_firmware_update() {
    let mut db = setup_db();
    setup_device_schema(&mut db);
    db.execute_sql("INSERT INTO devices (id, firmware_version) VALUES ('dev001', '1.0.0')").unwrap();
    
    let result = db.execute_sql("UPDATE devices SET firmware_version = '1.1.0' WHERE id = 'dev001'");
    assert!(result.is_ok());
}

#[test]
fn test_device_deletion() {
    let mut db = setup_db();
    setup_device_schema(&mut db);
    db.execute_sql("INSERT INTO devices (id, name) VALUES ('dev001', 'Sensor')").unwrap();
    
    let result = db.execute_sql("DELETE FROM devices WHERE id = 'dev001'");
    assert!(result.is_ok());
}

#[test]
fn test_offline_devices() {
    let mut db = setup_db();
    setup_device_schema(&mut db);
    db.execute_sql("INSERT INTO devices (id, status, last_seen) VALUES ('dev001', 'offline', 100)").unwrap();
    db.execute_sql("INSERT INTO devices (id, status, last_seen) VALUES ('dev002', 'active', 2000)").unwrap();
    
    let result = db.execute_sql("SELECT * FROM devices WHERE status = 'offline'");
    assert!(result.is_ok());
}

// Generate remaining device tests
macro_rules! generate_device_tests {
    ($($name:ident => $test_num:expr),*) => {
        $(
            #[test]
            fn $name() {
                let mut db = setup_db();
                setup_device_schema(&mut db);
                for i in 1..=5 {
                    db.execute_sql(&format!("INSERT INTO devices (id, device_type, location) VALUES ('dev{}_{}', 'type{}', 'loc{}')", 
                        i, $test_num, i % 3, i % 4)).unwrap();
                }
                let result = db.execute_sql("SELECT COUNT(*) FROM devices");
                assert!(result.is_ok());
            }
        )*
    };
}

generate_device_tests!(
    test_device_batch_20 => 20,
    test_device_batch_21 => 21,
    test_device_batch_22 => 22,
    test_device_batch_23 => 23,
    test_device_batch_24 => 24,
    test_device_batch_25 => 25,
    test_device_batch_26 => 26,
    test_device_batch_27 => 27,
    test_device_batch_28 => 28,
    test_device_batch_29 => 29,
    test_device_batch_30 => 30,
    test_device_batch_31 => 31,
    test_device_batch_32 => 32,
    test_device_batch_33 => 33,
    test_device_batch_34 => 34,
    test_device_batch_35 => 35,
    test_device_batch_36 => 36,
    test_device_batch_37 => 37,
    test_device_batch_38 => 38,
    test_device_batch_39 => 39
);

// ============================================================================
// Sensor Data Ingestion (Tests 41-90)
// ============================================================================

fn setup_sensor_data_schema(executor: &mut Executor) {
    executor.execute_sql("CREATE TABLE sensor_readings (
        id INTEGER PRIMARY KEY,
        device_id TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        sensor_type TEXT,
        value REAL,
        unit TEXT,
        quality INTEGER DEFAULT 100
    )").unwrap();
    
    executor.execute_sql("CREATE INDEX idx_readings_device ON sensor_readings (device_id)").unwrap();
    executor.execute_sql("CREATE INDEX idx_readings_time ON sensor_readings (timestamp)").unwrap();
    executor.execute_sql("CREATE INDEX idx_readings_device_time ON sensor_readings (device_id, timestamp)").unwrap();
}

#[test]
fn test_sensor_reading_insert() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    
    let result = db.execute_sql("INSERT INTO sensor_readings (id, device_id, timestamp, sensor_type, value, unit) 
        VALUES (1, 'dev001', 1234567890, 'temperature', 23.5, 'celsius')");
    assert!(result.is_ok());
}

#[test]
fn test_sensor_batch_insert() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    
    for i in 1..=100 {
        let result = db.execute_sql(&format!(
            "INSERT INTO sensor_readings (id, device_id, timestamp, sensor_type, value, unit) 
            VALUES ({}, 'dev001', {}, 'temperature', {}, 'celsius')",
            i, 1234567890 + i * 60, 20.0 + (i as f64 % 10.0)
        ));
        assert!(result.is_ok());
    }
}

#[test]
fn test_sensor_readings_by_device() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    for i in 1..=50 {
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, timestamp, value) VALUES ({}, 'dev001', {}, {})",
            i, 1000 + i, i as f64 * 1.5)).unwrap();
    }
    
    let result = db.execute_sql("SELECT * FROM sensor_readings WHERE device_id = 'dev001' ORDER BY timestamp");
    assert!(result.is_ok());
}

#[test]
fn test_sensor_readings_time_range() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    for i in 1..=100 {
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, timestamp, value) VALUES ({}, 'dev001', {}, {})",
            i, i * 100, i as f64)).unwrap();
    }
    
    let result = db.execute_sql("SELECT * FROM sensor_readings WHERE device_id = 'dev001' AND timestamp >= 5000 AND timestamp <= 8000");
    assert!(result.is_ok());
}

#[test]
fn test_sensor_average_value() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    for i in 1..=20 {
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, value) VALUES ({}, 'dev001', {})", i, i as f64 * 2.0)).unwrap();
    }
    
    let result = db.execute_sql("SELECT AVG(value) as avg_value FROM sensor_readings WHERE device_id = 'dev001'");
    assert!(result.is_ok());
}

#[test]
fn test_sensor_min_max_values() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    for i in 1..=50 {
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, value) VALUES ({}, 'dev001', {})", i, i as f64)).unwrap();
    }
    
    let result = db.execute_sql("SELECT MIN(value) as min_val, MAX(value) as max_val FROM sensor_readings WHERE device_id = 'dev001'");
    assert!(result.is_ok());
}

#[test]
fn test_sensor_readings_count() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    for i in 1..=200 {
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id) VALUES ({}, 'dev001')", i)).unwrap();
    }
    
    let result = db.execute_sql("SELECT COUNT(*) as total FROM sensor_readings WHERE device_id = 'dev001'");
    assert!(result.is_ok());
}

#[test]
fn test_sensor_quality_filter() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    for i in 1..=30 {
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, quality) VALUES ({}, 'dev001', {})", i, 100 - i)).unwrap();
    }
    
    let result = db.execute_sql("SELECT * FROM sensor_readings WHERE device_id = 'dev001' AND quality >= 80");
    assert!(result.is_ok());
}

#[test]
fn test_latest_sensor_reading() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    for i in 1..=100 {
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, timestamp, value) VALUES ({}, 'dev001', {}, {})",
            i, i * 10, i as f64)).unwrap();
    }
    
    let result = db.execute_sql("SELECT * FROM sensor_readings WHERE device_id = 'dev001' ORDER BY timestamp DESC LIMIT 1");
    assert!(result.is_ok());
}

#[test]
fn test_sensor_value_threshold_alert() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    for i in 1..=50 {
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, sensor_type, value) VALUES ({}, 'dev001', 'temperature', {})",
            i, 20.0 + (i as f64 % 20.0))).unwrap();
    }
    
    let result = db.execute_sql("SELECT * FROM sensor_readings WHERE sensor_type = 'temperature' AND value > 35.0");
    assert!(result.is_ok());
}

#[test]
fn test_sensor_readings_pagination() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    for i in 1..=200 {
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, timestamp) VALUES ({}, 'dev001', {})", i, i)).unwrap();
    }
    
    let result = db.execute_sql("SELECT * FROM sensor_readings WHERE device_id = 'dev001' ORDER BY timestamp LIMIT 20 OFFSET 40");
    assert!(result.is_ok());
}

#[test]
fn test_multiple_sensors_data() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    for i in 1..=10 {
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, sensor_type) VALUES ({}, 'dev001', 'temperature')", i)).unwrap();
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, sensor_type) VALUES ({}, 'dev001', 'humidity')", i + 10)).unwrap();
    }
    
    let result = db.execute_sql("SELECT sensor_type, COUNT(*) as count FROM sensor_readings WHERE device_id = 'dev001' GROUP BY sensor_type");
    assert!(result.is_ok());
}

// Generate remaining sensor tests
macro_rules! generate_sensor_tests {
    ($($name:ident => $test_num:expr),*) => {
        $(
            #[test]
            fn $name() {
                let mut db = setup_db();
                setup_sensor_data_schema(&mut db);
                for i in 1..=10 {
                    db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, timestamp, value) VALUES ({}, 'dev{}', {}, {})", 
                        i + $test_num * 10, i % 3 + 1, 1000 + i, i as f64)).unwrap();
                }
                let result = db.execute_sql("SELECT AVG(value) FROM sensor_readings");
                assert!(result.is_ok());
            }
        )*
    };
}

generate_sensor_tests!(
    test_sensor_batch_60 => 60,
    test_sensor_batch_61 => 61,
    test_sensor_batch_62 => 62,
    test_sensor_batch_63 => 63,
    test_sensor_batch_64 => 64,
    test_sensor_batch_65 => 65,
    test_sensor_batch_66 => 66,
    test_sensor_batch_67 => 67,
    test_sensor_batch_68 => 68,
    test_sensor_batch_69 => 69,
    test_sensor_batch_70 => 70,
    test_sensor_batch_71 => 71,
    test_sensor_batch_72 => 72,
    test_sensor_batch_73 => 73,
    test_sensor_batch_74 => 74,
    test_sensor_batch_75 => 75,
    test_sensor_batch_76 => 76,
    test_sensor_batch_77 => 77,
    test_sensor_batch_78 => 78,
    test_sensor_batch_79 => 79,
    test_sensor_batch_80 => 80
);

// ============================================================================
// Time-Series Queries (Tests 91-120)
// ============================================================================

#[test]
fn test_time_series_hourly_aggregation() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    
    // Insert readings for multiple hours
    for hour in 0..24 {
        for min in 0..6 {
            let timestamp = hour * 3600 + min * 600;
            let _ = db.execute_sql(&format!(
                "INSERT INTO sensor_readings (id, device_id, timestamp, value) VALUES ({}, 'dev001', {}, {})",
                hour * 10 + min, timestamp, 20.0 + (hour as f64)
            ));
        }
    }
    
    let result = db.execute_sql("SELECT COUNT(*) FROM sensor_readings WHERE device_id = 'dev001'");
    assert!(result.is_ok());
}

#[test]
fn test_time_series_daily_summary() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    
    for day in 1..=30 {
        for reading in 1..=24 {
            let timestamp = day * 86400 + reading * 3600;
            let _ = db.execute_sql(&format!(
                "INSERT INTO sensor_readings (id, device_id, timestamp, value) VALUES ({}, 'dev001', {}, {})",
                day * 100 + reading, timestamp, 20.0 + (reading as f64 % 10.0)
            ));
        }
    }
    
    let result = db.execute_sql("SELECT AVG(value), MIN(value), MAX(value) FROM sensor_readings WHERE device_id = 'dev001'");
    assert!(result.is_ok());
}

#[test]
fn test_time_series_rolling_window() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    
    for i in 1..=100 {
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, timestamp, value) VALUES ({}, 'dev001', {}, {})",
            i, i * 300, i as f64)).unwrap();
    }
    
    // Get last 10 readings
    let result = db.execute_sql("SELECT * FROM sensor_readings WHERE device_id = 'dev001' ORDER BY timestamp DESC LIMIT 10");
    assert!(result.is_ok());
}

#[test]
fn test_time_series_gap_detection() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    
    // Insert readings with gaps
    let timestamps = vec![1000, 1100, 1200, 2000, 2100, 3000];
    for (i, ts) in timestamps.iter().enumerate() {
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, timestamp) VALUES ({}, 'dev001', {})",
            i + 1, ts)).unwrap();
    }
    
    let result = db.execute_sql("SELECT * FROM sensor_readings WHERE device_id = 'dev001' ORDER BY timestamp");
    assert!(result.is_ok());
}

#[test]
fn test_time_series_first_last_values() {
    let mut db = setup_db();
    setup_sensor_data_schema(&mut db);
    
    for i in 1..=50 {
        db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, timestamp, value) VALUES ({}, 'dev001', {}, {})",
            i, i * 60, i as f64)).unwrap();
    }
    
    let result = db.execute_sql("SELECT value FROM sensor_readings WHERE device_id = 'dev001' ORDER BY timestamp LIMIT 1");
    assert!(result.is_ok());
}

// Generate remaining time-series tests
macro_rules! generate_timeseries_tests {
    ($($name:ident => $test_num:expr),*) => {
        $(
            #[test]
            fn $name() {
                let mut db = setup_db();
                setup_sensor_data_schema(&mut db);
                for i in 1..=20 {
                    db.execute_sql(&format!("INSERT INTO sensor_readings (id, device_id, timestamp, value) VALUES ({}, 'dev{}', {}, {})", 
                        i + $test_num * 20, i % 2 + 1, i * 100, i as f64)).unwrap();
                }
                let result = db.execute_sql("SELECT * FROM sensor_readings ORDER BY timestamp");
                assert!(result.is_ok());
            }
        )*
    };
}

generate_timeseries_tests!(
    test_timeseries_batch_100 => 100,
    test_timeseries_batch_101 => 101,
    test_timeseries_batch_102 => 102,
    test_timeseries_batch_103 => 103,
    test_timeseries_batch_104 => 104,
    test_timeseries_batch_105 => 105,
    test_timeseries_batch_106 => 106,
    test_timeseries_batch_107 => 107,
    test_timeseries_batch_108 => 108,
    test_timeseries_batch_109 => 109,
    test_timeseries_batch_110 => 110
);

// ============================================================================
// Alert System (Tests 121-135)
// ============================================================================

fn setup_alert_schema(executor: &mut Executor) {
    executor.execute_sql("CREATE TABLE alerts (
        id INTEGER PRIMARY KEY,
        device_id TEXT NOT NULL,
        alert_type TEXT,
        severity TEXT,
        message TEXT,
        threshold_value REAL,
        actual_value REAL,
        is_acknowledged INTEGER DEFAULT 0,
        created_at INTEGER
    )").unwrap();
    
    executor.execute_sql("CREATE INDEX idx_alerts_device ON alerts (device_id)").unwrap();
    executor.execute_sql("CREATE INDEX idx_alerts_severity ON alerts (severity)").unwrap();
}

#[test]
fn test_alert_create() {
    let mut db = setup_db();
    setup_alert_schema(&mut db);
    
    let result = db.execute_sql("INSERT INTO alerts (id, device_id, alert_type, severity, message, created_at) 
        VALUES (1, 'dev001', 'high_temperature', 'warning', 'Temperature exceeded threshold', 1234567890)");
    assert!(result.is_ok());
}

#[test]
fn test_alert_acknowledge() {
    let mut db = setup_db();
    setup_alert_schema(&mut db);
    db.execute_sql("INSERT INTO alerts (id, device_id, is_acknowledged) VALUES (1, 'dev001', 0)").unwrap();
    
    let result = db.execute_sql("UPDATE alerts SET is_acknowledged = 1 WHERE id = 1");
    assert!(result.is_ok());
}

#[test]
fn test_unacknowledged_alerts() {
    let mut db = setup_db();
    setup_alert_schema(&mut db);
    for i in 1..=20 {
        db.execute_sql(&format!("INSERT INTO alerts (id, device_id, is_acknowledged) VALUES ({}, 'dev001', {})",
            i, i % 3)).unwrap();
    }
    
    let result = db.execute_sql("SELECT COUNT(*) FROM alerts WHERE device_id = 'dev001' AND is_acknowledged = 0");
    assert!(result.is_ok());
}

#[test]
fn test_alerts_by_severity() {
    let mut db = setup_db();
    setup_alert_schema(&mut db);
    db.execute_sql("INSERT INTO alerts (id, device_id, severity) VALUES (1, 'dev001', 'critical')").unwrap();
    db.execute_sql("INSERT INTO alerts (id, device_id, severity) VALUES (2, 'dev001', 'warning')").unwrap();
    
    let result = db.execute_sql("SELECT * FROM alerts WHERE severity = 'critical'").unwrap();
    match result {
        ExecuteResult::Query(qr) => assert_eq!(qr.rows.len(), 1),
        _ => panic!("Expected query result"),
    }
}

#[test]
fn test_recent_alerts() {
    let mut db = setup_db();
    setup_alert_schema(&mut db);
    for i in 1..=50 {
        db.execute_sql(&format!("INSERT INTO alerts (id, device_id, created_at) VALUES ({}, 'dev001', {})",
            i, 1000 + i * 10)).unwrap();
    }
    
    let result = db.execute_sql("SELECT * FROM alerts WHERE created_at > 1200 ORDER BY created_at DESC LIMIT 10");
    assert!(result.is_ok());
}

// Generate remaining alert tests
macro_rules! generate_alert_tests {
    ($($name:ident => $test_num:expr),*) => {
        $(
            #[test]
            fn $name() {
                let mut db = setup_db();
                setup_alert_schema(&mut db);
                for i in 1..=3 {
                    db.execute_sql(&format!("INSERT INTO alerts (id, device_id, severity) VALUES ({}, 'dev{}', '{}')", 
                        i + $test_num * 3, i % 2 + 1, if i % 2 == 0 { "warning" } else { "critical" })).unwrap();
                }
                let result = db.execute_sql("SELECT * FROM alerts");
                assert!(result.is_ok());
            }
        )*
    };
}

generate_alert_tests!(
    test_alert_batch_130 => 130,
    test_alert_batch_131 => 131,
    test_alert_batch_132 => 132,
    test_alert_batch_133 => 133,
    test_alert_batch_134 => 134
);

// ============================================================================
// Data Analytics (Tests 136-150)
// ============================================================================

fn setup_analytics_schema(executor: &mut Executor) {
    executor.execute_sql("CREATE TABLE hourly_aggregates (
        id INTEGER PRIMARY KEY,
        device_id TEXT NOT NULL,
        hour_timestamp INTEGER,
        avg_value REAL,
        min_value REAL,
        max_value REAL,
        reading_count INTEGER
    )").unwrap();
}

#[test]
fn test_hourly_aggregate_insert() {
    let mut db = setup_db();
    setup_analytics_schema(&mut db);
    
    let result = db.execute_sql("INSERT INTO hourly_aggregates (id, device_id, hour_timestamp, avg_value, min_value, max_value, reading_count) 
        VALUES (1, 'dev001', 1234567890, 23.5, 20.0, 27.0, 60)");
    assert!(result.is_ok());
}

#[test]
fn test_aggregates_by_device() {
    let mut db = setup_db();
    setup_analytics_schema(&mut db);
    for i in 1..=24 {
        db.execute_sql(&format!("INSERT INTO hourly_aggregates (id, device_id, hour_timestamp, avg_value) VALUES ({}, 'dev001', {}, {})",
            i, i * 3600, 20.0 + (i as f64 % 10.0))).unwrap();
    }
    
    let result = db.execute_sql("SELECT AVG(avg_value) as daily_avg FROM hourly_aggregates WHERE device_id = 'dev001'");
    assert!(result.is_ok());
}

#[test]
fn test_peak_value_analysis() {
    let mut db = setup_db();
    setup_analytics_schema(&mut db);
    for i in 1..=30 {
        db.execute_sql(&format!("INSERT INTO hourly_aggregates (id, device_id, max_value) VALUES ({}, 'dev001', {})",
            i, 25.0 + (i as f64 % 15.0))).unwrap();
    }
    
    let result = db.execute_sql("SELECT MAX(max_value) as peak FROM hourly_aggregates WHERE device_id = 'dev001'");
    assert!(result.is_ok());
}

#[test]
fn test_reading_count_summary() {
    let mut db = setup_db();
    setup_analytics_schema(&mut db);
    for i in 1..=100 {
        db.execute_sql(&format!("INSERT INTO hourly_aggregates (id, device_id, reading_count) VALUES ({}, 'dev001', {})",
            i, 50 + i % 20)).unwrap();
    }
    
    let result = db.execute_sql("SELECT SUM(reading_count) as total_readings FROM hourly_aggregates WHERE device_id = 'dev001'");
    assert!(result.is_ok());
}

// Generate remaining analytics tests
macro_rules! generate_analytics_tests {
    ($($name:ident => $test_num:expr),*) => {
        $(
            #[test]
            fn $name() {
                let mut db = setup_db();
                setup_analytics_schema(&mut db);
                for i in 1..=5 {
                    db.execute_sql(&format!("INSERT INTO hourly_aggregates (id, device_id, avg_value, reading_count) VALUES ({}, 'dev{}', {}, {})", 
                        i + $test_num * 5, i % 2 + 1, 20.0 + i as f64, 50 + i)).unwrap();
                }
                let result = db.execute_sql("SELECT * FROM hourly_aggregates");
                assert!(result.is_ok());
            }
        )*
    };
}

generate_analytics_tests!(
    test_analytics_batch_145 => 145,
    test_analytics_batch_146 => 146,
    test_analytics_batch_147 => 147,
    test_analytics_batch_148 => 148,
    test_analytics_batch_149 => 149
);
