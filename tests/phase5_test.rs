//! Phase 5 Feature Tests
//! 
//! Tests for:
//! - P5-2: Triggers
//! - P5-3: Views with CHECK OPTION
//! - P5-4: Window Functions
//! - P5-5: Recursive CTEs
//! - P5-6: Full Text Search (FTS5)
//! - P5-7: R-Tree Spatial Index
//! - P5-8: JSON Support

use sqllite_rust::sql::{Parser, Statement};
use sqllite_rust::sql::ast::*;
use sqllite_rust::json::{JsonValue, JsonFunctions};
use sqllite_rust::fts::{Fts5Table, tokenize, TokenizerType};
use sqllite_rust::rtree::{RtreeIndex, BoundingBox};
use sqllite_rust::window::WindowEvaluator;
use sqllite_rust::trigger::{TriggerManager, TriggerMetadata};

// ============ P5-2: Trigger Tests ============
#[test]
fn test_trigger_parse() {
    let sql = "CREATE TRIGGER update_timestamp AFTER UPDATE ON users BEGIN UPDATE users SET updated_at = 'now'; END";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::CreateTrigger(t) => {
            assert_eq!(t.name, "update_timestamp");
            assert_eq!(t.table, "users");
            assert!(matches!(t.timing, TriggerTiming::After));
            assert!(matches!(t.event, TriggerEvent::Update { .. }));
        }
        _ => panic!("Expected CreateTrigger statement"),
    }
}

#[test]
fn test_drop_trigger_parse() {
    let sql = "DROP TRIGGER IF EXISTS update_timestamp";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::DropTrigger(t) => {
            assert_eq!(t.name, "update_timestamp");
            assert!(t.if_exists);
        }
        _ => panic!("Expected DropTrigger statement"),
    }
}

#[test]
fn test_trigger_manager() {
    let mut manager = TriggerManager::new();
    
    let trigger = TriggerMetadata {
        name: "test_trigger".to_string(),
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table: "users".to_string(),
        for_each_row: true,
        when_clause: None,
        body: vec![],
        enabled: true,
    };
    
    manager.register(trigger.clone()).unwrap();
    assert!(manager.get_trigger("test_trigger").is_some());
    
    let triggers = manager.find_triggers("users", TriggerTiming::Before, &TriggerEvent::Insert);
    assert_eq!(triggers.len(), 1);
    
    manager.drop_trigger("test_trigger", false).unwrap();
    assert!(manager.get_trigger("test_trigger").is_none());
}

// ============ P5-4: Window Function Tests ============
#[test]
fn test_window_function_parse() {
    let sql = "SELECT ROW_NUMBER() OVER (ORDER BY salary DESC) AS rank FROM employees";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 1);
            assert!(matches!(s.columns[0], SelectColumn::WindowFunc(_, _)));
        }
        _ => panic!("Expected Select statement"),
    }
}

#[test]
fn test_window_function_rank() {
    let sql = "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary) FROM employees";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    assert!(matches!(stmt, Statement::Select(_)));
}

#[test]
fn test_window_function_lag_lead() {
    let sql = "SELECT LAG(salary, 1, 0) OVER (ORDER BY id) FROM employees";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    assert!(matches!(stmt, Statement::Select(_)));
}

// ============ P5-5: CTE Tests ============
#[test]
fn test_recursive_cte_parse() {
    let sql = r#"
        WITH RECURSIVE hierarchy(id, name, level) AS (
            SELECT id, name, 1 FROM employees WHERE manager_id IS NULL
            UNION ALL
            SELECT e.id, e.name, h.level + 1 
            FROM employees e JOIN hierarchy h ON e.manager_id = h.id
        )
        SELECT * FROM hierarchy
    "#;
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    assert!(matches!(stmt, Statement::Select(_)));
}

// ============ P5-6: FTS5 Tests ============
#[test]
fn test_fts5_parse() {
    let sql = "CREATE VIRTUAL TABLE docs USING FTS5(title, content)";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::CreateVirtualTable(vt) => {
            assert_eq!(vt.name, "docs");
            match vt.module {
                VirtualTableModule::Fts5(cols) => {
                    assert_eq!(cols, vec!["title", "content"]);
                }
                _ => panic!("Expected FTS5 module"),
            }
        }
        _ => panic!("Expected CreateVirtualTable statement"),
    }
}

#[test]
fn test_fts_tokenizer() {
    let text = "Hello World! This is a test.";
    let tokens = tokenize(text, TokenizerType::Simple);
    assert_eq!(tokens, vec!["hello", "world", "this", "is", "a", "test"]);
}

#[test]
fn test_fts5_table() {
    let mut fts = Fts5Table::new("docs".to_string(), vec!["title".to_string(), "content".to_string()]);
    
    // Insert documents
    let doc_id = fts.insert(&[
        "Hello World".to_string(),
        "This is a test document".to_string()
    ]).unwrap();
    
    assert_eq!(doc_id, 1);
    assert_eq!(fts.doc_count(), 1);
    
    // Search
    let results = fts.search("test").unwrap();
    assert!(!results.is_empty());
    
    // Match query
    let matches = fts.match_query("hello", None).unwrap();
    assert!(matches.contains(&doc_id));
}

// ============ P5-7: R-Tree Tests ============
#[test]
fn test_rtree_parse() {
    let sql = "CREATE VIRTUAL TABLE places USING RTREE(id, minX, maxX, minY, maxY)";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::CreateVirtualTable(vt) => {
            assert_eq!(vt.name, "places");
            match vt.module {
                VirtualTableModule::Rtree { id_column, min_x, max_x, min_y, max_y } => {
                    assert_eq!(id_column, "id");
                    assert_eq!(min_x, "minX");
                    assert_eq!(max_x, "maxX");
                    assert_eq!(min_y, "minY");
                    assert_eq!(max_y, "maxY");
                }
                _ => panic!("Expected RTREE module"),
            }
        }
        _ => panic!("Expected CreateVirtualTable statement"),
    }
}

#[test]
fn test_rtree_index() {
    let mut rtree = RtreeIndex::new("places".to_string());
    
    // Insert bounding boxes
    rtree.insert(BoundingBox::new(0.0, 10.0, 0.0, 10.0), 1).unwrap();
    rtree.insert(BoundingBox::new(5.0, 15.0, 5.0, 15.0), 2).unwrap();
    rtree.insert(BoundingBox::new(20.0, 30.0, 20.0, 30.0), 3).unwrap();
    
    // Range search
    let results = rtree.search_range(BoundingBox::new(4.0, 12.0, 4.0, 12.0));
    assert!(results.contains(&1));
    assert!(results.contains(&2));
    assert!(!results.contains(&3));
    
    // Nearest neighbor search
    let nearest = rtree.nearest_neighbors(0.0, 0.0, 2);
    assert_eq!(nearest.len(), 2);
    assert_eq!(nearest[0].0, 1); // Closest to (0,0)
}

#[test]
fn test_bounding_box() {
    let bbox1 = BoundingBox::new(0.0, 10.0, 0.0, 10.0);
    let bbox2 = BoundingBox::new(5.0, 15.0, 5.0, 15.0);
    
    assert!(bbox1.intersects(&bbox2));
    assert!(!bbox1.contains(&bbox2));
    assert!(bbox1.contains_point(5.0, 5.0));
    assert!(!bbox1.contains_point(15.0, 15.0));
}

// ============ P5-8: JSON Tests ============
#[test]
fn test_json_parse() {
    // Test basic JSON parsing
    let json = JsonValue::parse(r#"{"name": "John", "age": 30}"#).unwrap();
    match &json {
        JsonValue::Object(obj) => {
            assert_eq!(obj.len(), 2);
            assert!(obj.contains_key("name"));
            assert!(obj.contains_key("age"));
        }
        _ => panic!("Expected object"),
    }
}

#[test]
fn test_json_extract() {
    let json = JsonValue::parse(r#"{"person": {"name": "John", "age": 30}}"#).unwrap();
    
    let name = json.extract("$.person.name");
    assert_eq!(name, Some(&JsonValue::String("John".to_string())));
    
    let age = json.extract("$.person.age");
    assert_eq!(age, Some(&JsonValue::Number(30.0)));
}

#[test]
fn test_json_functions() {
    // Test json_valid
    assert!(JsonFunctions::json_valid(r#"{"a": 1}"#));
    assert!(!JsonFunctions::json_valid("invalid json"));
    
    // Test json_type
    assert_eq!(JsonFunctions::json_type(r#"{"a": 1}"#, Some("$.a")).unwrap(), "real");
    assert_eq!(JsonFunctions::json_type(r#"{"a": "text"}"#, Some("$.a")).unwrap(), "text");
    
    // Test json_extract
    let result = JsonFunctions::json_extract(r#"{"name": "John"}"#, "$.name").unwrap();
    assert_eq!(result, Some("\"John\"".to_string()));
}

#[test]
fn test_json_function_parse() {
    // Test JSON_EXTRACT in SQL
    let sql = "SELECT JSON_EXTRACT(data, '$.name') FROM users";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    assert!(matches!(stmt, Statement::Select(_)));
    
    // Test JSON_ARRAY
    let sql = "SELECT JSON_ARRAY(1, 2, 3)";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    assert!(matches!(stmt, Statement::Select(_)));
    
    // Test JSON_OBJECT
    let sql = "SELECT JSON_OBJECT('name', 'John', 'age', 30)";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    assert!(matches!(stmt, Statement::Select(_)));
}

// ============ P5-3: View Tests ============
#[test]
fn test_view_with_check_option_parse() {
    let sql = "CREATE VIEW high_salary AS SELECT * FROM employees WHERE salary > 50000 WITH CHECK OPTION";
    let mut parser = Parser::new(sql).unwrap();
    let stmt = parser.parse().unwrap();
    
    match stmt {
        Statement::CreateView(v) => {
            assert_eq!(v.name, "high_salary");
            assert!(v.with_check_option);
        }
        _ => panic!("Expected CreateView statement"),
    }
}

// ============ Integration Tests ============
#[test]
fn test_phase5_features_comprehensive() {
    // Test that all Phase 5 SQL syntax can be parsed
    
    // Trigger
    let sql = "CREATE TRIGGER trg BEFORE INSERT ON t BEGIN INSERT INTO log VALUES (NEW.id); END";
    assert!(Parser::new(sql).unwrap().parse().is_ok());
    
    // Window function
    let sql = "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) FROM emp";
    assert!(Parser::new(sql).unwrap().parse().is_ok());
    
    // Recursive CTE
    let sql = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5) SELECT * FROM r";
    assert!(Parser::new(sql).unwrap().parse().is_ok());
    
    // FTS5
    let sql = "CREATE VIRTUAL TABLE ft USING FTS5(content)";
    assert!(Parser::new(sql).unwrap().parse().is_ok());
    
    // R-Tree
    let sql = "CREATE VIRTUAL TABLE rt USING RTREE(id, minX, maxX, minY, maxY)";
    assert!(Parser::new(sql).unwrap().parse().is_ok());
    
    // JSON function
    let sql = "SELECT JSON_EXTRACT(data, '$.key') FROM t";
    assert!(Parser::new(sql).unwrap().parse().is_ok());
}
