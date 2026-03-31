//! Scenario Integration Tests Entry Point
//!
//! This module re-exports all scenario tests from the scenario/ subdirectory.

// Web Application Scenarios
#[path = "scenario/web_app_tests.rs"]
mod web_app_tests;

// IoT Scenarios  
#[path = "scenario/iot_tests.rs"]
mod iot_tests;

// Financial Scenarios
#[path = "scenario/financial_tests.rs"]
mod financial_tests;

// Gaming Scenarios
#[path = "scenario/game_tests.rs"]
mod game_tests;

// Migration Scenarios
#[path = "scenario/migration_tests.rs"]
mod migration_tests;

// Performance Regression Tests
#[path = "scenario/performance_regression_tests.rs"]
mod performance_regression_tests;

// SQLite Compatibility Tests
#[path = "scenario/sqlite_compat_tests.rs"]
mod sqlite_compat_tests;
