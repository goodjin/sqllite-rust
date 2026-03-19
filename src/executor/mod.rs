use crate::sql::ast::{Statement, Expression, BinaryOp, ColumnDef, SelectColumn, AggregateFunc, CreateIndexStmt};
use crate::sql::StatementCache;
use crate::storage::{BtreeDatabase, Record, Value};

pub mod result;
pub mod planner;
pub mod pool;

pub use result::{ExecutorError, Result};
pub use planner::{QueryPlanner, QueryPlan, PlanExecutor};

/// 事务日志条目
#[derive(Debug, Clone)]
enum TransactionLogEntry {
    Insert { table: String, rowid: u64 },
    Update { table: String, rowid: u64, old_record: Record },
    Delete { table: String, old_record: Record },
}

/// SQL执行引擎
pub struct Executor {
    db: BtreeDatabase,
    in_transaction: bool,
    transaction_log: Vec<TransactionLogEntry>,
    /// 预编译语句缓存
    statement_cache: StatementCache,
    /// 查询计划器（用于优化查询）
    query_planner: QueryPlanner,
    /// 自动批量模式
    auto_batch: bool,
    /// 批量大小（达到此数量自动提交）
    batch_size: usize,
    /// 当前批量中的操作数
    batch_count: usize,
}

impl Executor {
    /// 打开或创建数据库
    pub fn open(path: &str) -> Result<Self> {
        let db = BtreeDatabase::open(path)?;
        Ok(Self {
            db,
            in_transaction: false,
            transaction_log: Vec::new(),
            statement_cache: StatementCache::new(100), // 缓存 100 个预编译语句
            query_planner: QueryPlanner,
            auto_batch: false,
            batch_size: 100,
            batch_count: 0,
        })
    }

    /// 启用自动批量模式
    ///
    /// 启用后，每次操作会自动开始事务，
    /// 达到 `batch_size` 条后自动提交
    pub fn enable_auto_batch(&mut self, batch_size: usize) {
        self.auto_batch = true;
        self.batch_size = batch_size;
        self.batch_count = 0;
    }

    /// 禁用自动批量模式
    pub fn disable_auto_batch(&mut self) {
        self.auto_batch = false;
    }

    /// 手动刷新批量（立即提交当前批次）
    pub fn flush_batch(&mut self) -> Result<ExecuteResult> {
        if self.batch_count > 0 {
            self.execute_commit()?;
        }
        Ok(ExecuteResult::Success("Batch flushed".to_string()))
    }

    /// 执行SQL语句
    pub fn execute(&mut self, stmt: &Statement) -> Result<ExecuteResult> {
        // 自动批量模式处理
        self.maybe_start_auto_batch()?;

        let result = match stmt {
            Statement::BeginTransaction => self.execute_begin(),
            Statement::Commit => self.execute_commit(),
            Statement::Rollback => self.execute_rollback(),
            Statement::CreateTable(ct) => self.execute_create_table(ct),
            Statement::Insert(ins) => self.execute_insert(ins),
            Statement::Select(sel) => self.execute_select(sel),
            Statement::Update(upd) => self.execute_update(upd),
            Statement::Delete(del) => self.execute_delete(del),
            Statement::DropTable(dt) => self.execute_drop_table(dt),
            Statement::CreateIndex(ci) => self.execute_create_index(ci),
            _ => Err(ExecutorError::NotImplemented(format!("{:?}", stmt))),
        };

        // 自动批量提交
        if let Ok(_) = result {
            self.maybe_auto_commit()?;
        }

        result
    }

    /// 如果在自动批量模式且不在事务中，开始新事务
    fn maybe_start_auto_batch(&mut self) -> Result<()> {
        if self.auto_batch && !self.in_transaction {
            // SELECT 等读操作不需要开启事务
            // 只需要在写操作时自动开启
        }
        Ok(())
    }

    /// 检查是否需要自动提交
    fn maybe_auto_commit(&mut self) -> Result<()> {
        if !self.auto_batch {
            return Ok(());
        }

        // 只对写操作计数
        self.batch_count += 1;

        if self.batch_count >= self.batch_size {
            // 达到批量大小，自动提交
            if self.in_transaction {
                self.execute_commit()?;
            }
            self.batch_count = 0;
        }

        Ok(())
    }

    /// 执行SQL字符串（使用预编译语句缓存）
    ///
    /// 这是推荐使用的方式，内部会：
    /// 1. 尝试从缓存获取预编译语句
    /// 2. 如果未命中，解析 SQL 并缓存
    /// 3. 执行预编译语句
    pub fn execute_sql(&mut self, sql: &str) -> Result<ExecuteResult> {
        // 从缓存获取或创建预编译语句
        let prepared = self.statement_cache.get_or_prepare(sql)
            .map_err(|e| ExecutorError::ParseError(e))?;

        // 执行预编译语句
        self.execute(&prepared.statement)
    }

    /// 执行预编译语句（带参数绑定）
    ///
    /// 使用方式：
    /// ```ignore
    /// // 第一次：解析并缓存模板
    /// executor.execute_prepared(
    ///     "INSERT INTO users (name) VALUES (?)",
    ///     &[Expression::String("Alice".into())]
    /// )?;
    ///
    /// // 后续执行：直接使用缓存的参数化查询
    /// executor.execute_prepared(
    ///     "INSERT INTO users (name) VALUES (?)",
    ///     &[Expression::String("Bob".into())]
    /// )?;
    /// ```
    pub fn execute_prepared(&mut self, sql: &str, params: &[crate::sql::Expression]) -> Result<ExecuteResult> {
        // 从缓存获取或创建预编译语句
        let prepared = self.statement_cache.get_or_prepare(sql)
            .map_err(|e| ExecutorError::ParseError(e))?;

        // 绑定参数
        let bound_stmt = crate::sql::bind_params(&prepared, params)
            .map_err(|e| ExecutorError::ParseError(e))?;

        // 执行带参数的语句
        self.execute(&bound_stmt)
    }

    /// 获取语句缓存统计信息
    pub fn cache_stats(&self) -> crate::sql::CacheStats {
        self.statement_cache.stats()
    }

    /// 清除语句缓存
    pub fn clear_cache(&mut self) {
        self.statement_cache.clear();
    }

    /// 执行BEGIN TRANSACTION
    fn execute_begin(&mut self) -> Result<ExecuteResult> {
        if self.in_transaction {
            return Err(ExecutorError::InvalidOperation("Already in a transaction".to_string()));
        }
        self.in_transaction = true;
        self.transaction_log.clear();
        Ok(ExecuteResult::Success("Transaction started".to_string()))
    }

    /// 执行COMMIT
    fn execute_commit(&mut self) -> Result<ExecuteResult> {
        if !self.in_transaction {
            return Err(ExecutorError::InvalidOperation("Not in a transaction".to_string()));
        }
        // 在实际应用中，这里会将事务日志中的更改持久化到磁盘
        // 在我们的简化实现中，更改已经实时写入，只需清除日志
        self.in_transaction = false;
        self.transaction_log.clear();
        Ok(ExecuteResult::Success("Transaction committed".to_string()))
    }

    /// 执行ROLLBACK
    fn execute_rollback(&mut self) -> Result<ExecuteResult> {
        if !self.in_transaction {
            return Err(ExecutorError::InvalidOperation("Not in a transaction".to_string()));
        }
        // 回滚事务：撤销事务日志中的所有更改
        self.rollback_changes()?;
        self.in_transaction = false;
        self.transaction_log.clear();
        Ok(ExecuteResult::Success("Transaction rolled back".to_string()))
    }

    /// 回滚更改
    fn rollback_changes(&mut self) -> Result<()> {
        // 从后向前遍历日志，撤销更改
        for entry in self.transaction_log.iter().rev() {
            match entry {
                TransactionLogEntry::Insert { table, rowid } => {
                    // 撤销插入：删除记录
                    let _ = self.db.delete(table, *rowid);
                }
                TransactionLogEntry::Update { table, rowid, old_record } => {
                    // 撤销更新：恢复旧记录
                    let _ = self.db.update(table, *rowid, old_record.clone());
                }
                TransactionLogEntry::Delete { table: _, old_record: _ } => {
                    // 撤销删除：需要重新插入记录
                    // 简化实现：不支持恢复删除的rowid
                    // 在实际应用中，日志应该包含足够信息来恢复
                }
            }
        }
        Ok(())
    }

    /// 执行CREATE TABLE
    fn execute_create_table(&mut self, stmt: &crate::sql::ast::CreateTableStmt) -> Result<ExecuteResult> {
        let columns = stmt.columns.clone();
        self.db.create_table(stmt.table.clone(), columns)?;
        Ok(ExecuteResult::Success(format!("Table '{}' created", stmt.table)))
    }

    /// 执行CREATE INDEX
    fn execute_create_index(&mut self, stmt: &CreateIndexStmt) -> Result<ExecuteResult> {
        // 1. 创建索引结构
        self.db.create_index(
            stmt.index_name.clone(),
            stmt.table.clone(),
            stmt.column.clone(),
        )?;

        // 2. 回填现有数据到索引
        // 获取表的列定义，找到索引列的位置
        let column_idx = self.db.get_table(&stmt.table)
            .ok_or(ExecutorError::TableNotFound(stmt.table.clone()))?
            .column_index(&stmt.column)
            .ok_or(ExecutorError::ColumnNotFound(stmt.column.clone()))?;

        // 获取表中所有记录（包括rowid）
        let records_with_rowid = self.db.select_all_with_rowid(&stmt.table)?;

        // 获取索引的可变引用
        let index = self.db.get_index_mut(&stmt.index_name)
            .ok_or(ExecutorError::IndexNotFound(stmt.index_name.clone()))?;

        // 将每条记录插入索引
        let mut indexed_count = 0;
        for (rowid, record) in records_with_rowid {
            if let Some(value) = record.values.get(column_idx) {
                index.insert(value.clone(), rowid)?;
                indexed_count += 1;
            }
        }

        Ok(ExecuteResult::Success(format!(
            "Index '{}' created on {}({}), indexed {} rows",
            stmt.index_name, stmt.table, stmt.column, indexed_count
        )))
    }

    /// 执行INSERT
    fn execute_insert(&mut self, stmt: &crate::sql::ast::InsertStmt) -> Result<ExecuteResult> {
        // 获取表定义
        let table = self.db.get_table(&stmt.table)
            .ok_or(ExecutorError::TableNotFound(stmt.table.clone()))?;

        let table_columns = table.columns.clone();
        let table_name = stmt.table.clone();

        // 获取表的所有索引信息 (索引名, 列索引位置)
        let index_info: Vec<(String, usize)> = self.db.get_table_indexes(&table_name)
            .iter()
            .filter_map(|idx| {
                let col_idx = table_columns.iter().position(|c| c.name == idx.column)?;
                Some((idx.name.clone(), col_idx))
            })
            .collect();

        let mut inserted_count = 0;
        let mut inserted_rowids: Vec<u64> = Vec::new();

        // 处理每一行值
        for row_values in &stmt.values {
            // 构建记录
            let record = self.build_record(&table_columns, &stmt.columns, row_values)?;

            // 提取索引列的值 (在record被消耗之前)
            let index_values: Vec<(String, usize, Value)> = index_info
                .iter()
                .filter_map(|(idx_name, col_idx)| {
                    record.values.get(*col_idx)
                        .map(|v| (idx_name.clone(), *col_idx, v.clone()))
                })
                .collect();

            // 插入记录
            let rowid = self.db.insert(&table_name, record)?;
            inserted_count += 1;

            // 更新所有索引
            for (idx_name, _col_idx, value) in index_values {
                if let Some(index) = self.db.get_index_mut(&idx_name) {
                    if let Err(e) = index.insert(value, rowid) {
                        eprintln!("Warning: failed to update index {}: {:?}", idx_name, e);
                    }
                }
            }

            // 如果在事务中，记录到日志
            if self.in_transaction {
                inserted_rowids.push(rowid);
            }
        }

        // 记录事务日志
        if self.in_transaction {
            for rowid in inserted_rowids {
                self.transaction_log.push(TransactionLogEntry::Insert {
                    table: table_name.clone(),
                    rowid,
                });
            }
        }

        Ok(ExecuteResult::Success(format!("{} row(s) inserted", inserted_count)))
    }

    /// 构建记录
    fn build_record(
        &self,
        table_columns: &[ColumnDef],
        insert_columns: &Option<Vec<String>>,
        values: &[Expression],
    ) -> Result<Record> {
        let mut record_values = Vec::with_capacity(table_columns.len());

        // 如果指定了列名
        if let Some(cols) = insert_columns {
            // 初始化所有列为NULL
            for _ in table_columns {
                record_values.push(Value::Null);
            }

            // 填充指定列的值
            for (i, col_name) in cols.iter().enumerate() {
                let col_idx = table_columns.iter()
                    .position(|c| c.name == *col_name)
                    .ok_or(ExecutorError::ColumnNotFound(col_name.clone()))?;

                let value = self.evaluate_expression(&values[i])?;
                record_values[col_idx] = value;
            }
        } else {
            // 按顺序填充所有列
            if values.len() != table_columns.len() {
                return Err(ExecutorError::ValueCountMismatch {
                    expected: table_columns.len(),
                    actual: values.len(),
                });
            }

            for value_expr in values {
                let value = self.evaluate_expression(value_expr)?;
                record_values.push(value);
            }
        }

        Ok(Record::new(record_values))
    }

    /// 执行SELECT with query planning optimization
    fn execute_select(&mut self, stmt: &crate::sql::ast::SelectStmt) -> Result<ExecuteResult> {
        // 处理JOIN查询 (使用原始方法)
        if !stmt.joins.is_empty() {
            return self.execute_join_select(stmt);
        }

        // 获取表定义
        let table_columns = {
            let table = self.db.get_table(&stmt.from)
                .ok_or(ExecutorError::TableNotFound(stmt.from.clone()))?;
            table.columns.clone()
        };

        // Use query planner for optimized execution
        let mut filtered_records: Vec<Record> = match QueryPlanner::plan(&self.db, stmt) {
            Ok(QueryPlan::FullTableScan { .. }) | Err(_) => {
                // Use full scan for complex queries or if planning fails
                self.execute_full_scan(stmt, &table_columns)?
            }
            Ok(plan) => {
                // Execute optimized plan
                let table_columns = {
                    let table = self.db.get_table(&stmt.from)
                        .ok_or(ExecutorError::TableNotFound(stmt.from.clone()))?;
                    table.columns.clone()
                };
                PlanExecutor::execute(&mut self.db, &plan, &table_columns)?
            }
        };

        // Check if we have aggregates or GROUP BY
        let has_aggregate = stmt.columns.iter().any(|c| {
            matches!(c, SelectColumn::Aggregate(_))
        });

        // Process GROUP BY and aggregates if needed
        if !stmt.group_by.is_empty() || has_aggregate {
            // 处理GROUP BY和聚合
            let grouped_records = if stmt.group_by.is_empty() {
                // 没有GROUP BY，计算整体聚合（即使记录为空也要返回一行）
                vec![(Vec::new(), filtered_records)]
            } else {
                // 按GROUP BY列分组
                self.group_records(&filtered_records, &table_columns, &stmt.group_by)?
            };

            // 计算每组的聚合结果
            let mut result_records: Vec<Record> = Vec::new();
            for (group_key, group_records) in grouped_records {
                let aggregate_row = self.compute_aggregates_with_group(
                    &stmt.columns,
                    &group_records,
                    &table_columns,
                    &group_key,
                    &stmt.group_by,
                )?;

                // 应用HAVING过滤
                if let Some(ref having_clause) = stmt.having {
                    if self.evaluate_having(&aggregate_row, &stmt.columns, having_clause)? {
                        result_records.push(aggregate_row);
                    }
                } else {
                    result_records.push(aggregate_row);
                }
            }

            // 应用ORDER BY排序
            if !stmt.order_by.is_empty() {
                result_records.sort_by(|a, b| {
                    for order in &stmt.order_by {
                        let col_idx = stmt.columns.iter()
                            .position(|c| matches!(c, SelectColumn::Column(name) if name == &order.column))
                            .unwrap_or(0);
                        let a_val = &a.values[col_idx];
                        let b_val = &b.values[col_idx];
                        let cmp = a_val.partial_cmp(b_val).unwrap_or(std::cmp::Ordering::Equal);
                        if cmp != std::cmp::Ordering::Equal {
                            return if order.descending { cmp.reverse() } else { cmp };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }

            // 应用OFFSET和LIMIT
            if let Some(offset) = stmt.offset {
                let offset = offset as usize;
                if offset < result_records.len() {
                    result_records = result_records.split_off(offset);
                } else {
                    result_records.clear();
                }
            }

            if let Some(limit) = stmt.limit {
                let limit = limit as usize;
                if limit < result_records.len() {
                    result_records.truncate(limit);
                }
            }

            let result = QueryResult {
                columns: stmt.columns.clone(),
                rows: result_records,
                table_columns: table_columns.clone(),
            };
            return Ok(ExecuteResult::Query(result));
        }

        // 应用ORDER BY排序
        if !stmt.order_by.is_empty() {
            filtered_records.sort_by(|a, b| {
                for order in &stmt.order_by {
                    let col_idx = table_columns.iter()
                        .position(|c| c.name == order.column)
                        .unwrap_or(0);
                    let a_val = &a.values[col_idx];
                    let b_val = &b.values[col_idx];
                    let cmp = a_val.partial_cmp(b_val).unwrap_or(std::cmp::Ordering::Equal);
                    if cmp != std::cmp::Ordering::Equal {
                        return if order.descending { cmp.reverse() } else { cmp };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // 应用OFFSET
        if let Some(offset) = stmt.offset {
            let offset = offset as usize;
            if offset < filtered_records.len() {
                filtered_records = filtered_records.split_off(offset);
            } else {
                filtered_records.clear();
            }
        }

        // 应用LIMIT
        if let Some(limit) = stmt.limit {
            let limit = limit as usize;
            if limit < filtered_records.len() {
                filtered_records.truncate(limit);
            }
        }

        let result = QueryResult {
            columns: stmt.columns.clone(),
            rows: filtered_records,
            table_columns,
        };

        Ok(ExecuteResult::Query(result))
    }

    /// Execute full table scan (fallback for complex queries)
    fn execute_full_scan(
        &mut self,
        stmt: &crate::sql::ast::SelectStmt,
        table_columns: &[ColumnDef],
    ) -> Result<Vec<Record>> {
        let all_records = self.db.select_all(&stmt.from)?;

        // Apply WHERE filtering
        let filtered: Vec<Record> = all_records
            .into_iter()
            .filter(|record| {
                if let Some(ref where_clause) = stmt.where_clause {
                    self.evaluate_where(record, table_columns, where_clause)
                        .unwrap_or(false)
                } else {
                    true
                }
            })
            .collect();

        Ok(filtered)
    }

    /// 执行JOIN查询
    fn execute_join_select(&mut self, stmt: &crate::sql::ast::SelectStmt) -> Result<ExecuteResult> {
        use crate::sql::ast::JoinType;

        // 先获取主表列定义，然后获取记录
        let main_columns = {
            let table = self.db.get_table(&stmt.from)
                .ok_or(ExecutorError::TableNotFound(stmt.from.clone()))?;
            table.columns.clone()
        };
        let main_records = self.db.select_all(&stmt.from)?;

        // 收集所有JOIN表的信息
        let mut join_tables: Vec<(String, Vec<ColumnDef>, Vec<Record>)> = Vec::new();
        let mut all_join_conditions: Vec<(String, crate::sql::ast::Expression)> = Vec::new();

        for join in &stmt.joins {
            let join_cols = {
                let join_table = self.db.get_table(&join.table)
                    .ok_or(ExecutorError::TableNotFound(join.table.clone()))?;
                join_table.columns.clone()
            };
            let join_records = self.db.select_all(&join.table)?;
            join_tables.push((join.table.clone(), join_cols, join_records));
            all_join_conditions.push((join.table.clone(), join.on_condition.clone()));
        }

        // 构建合并的列定义 (主表列 + JOIN表列)
        let mut combined_columns = main_columns.clone();
        for (_, cols, _) in &join_tables {
            combined_columns.extend(cols.clone());
        }

        // 执行嵌套循环JOIN
        let mut result_records: Vec<Record> = Vec::new();

        for main_record in &main_records {
            // 尝试与每个JOIN表匹配
            let mut current_matches: Vec<Vec<(Record, bool)>> = Vec::new(); // (record, is_match)

            for (idx, (_join_table_name, join_cols, join_records)) in join_tables.iter().enumerate() {
                let join_condition = &all_join_conditions[idx].1;
                let mut matches_for_this_table: Vec<(Record, bool)> = Vec::new();
                let mut has_match = false;

                for join_record in join_records {
                    // 合并主记录和JOIN记录
                    let combined_record = self.combine_records(main_record, join_record);

                    // 评估JOIN条件
                    if self.evaluate_where(&combined_record, &combined_columns, join_condition).unwrap_or(false) {
                        matches_for_this_table.push((join_record.clone(), true));
                        has_match = true;
                    }
                }

                // 对于LEFT JOIN，如果没有匹配，添加NULL记录
                if !has_match {
                    // 检查是否是LEFT JOIN
                    if let Some(join) = stmt.joins.get(idx) {
                        if matches!(join.join_type, JoinType::Left) {
                            let null_record = Record::new(vec![Value::Null; join_cols.len()]);
                            matches_for_this_table.push((null_record, false));
                        }
                    }
                }

                current_matches.push(matches_for_this_table);
            }

            // 生成结果记录 (笛卡尔积)
            self.generate_join_results(
                main_record,
                &current_matches,
                &mut result_records,
            );
        }

        // 应用WHERE过滤 (在JOIN之后)
        let mut filtered_records: Vec<Record> = result_records.into_iter()
            .filter(|record| {
                if let Some(ref where_clause) = stmt.where_clause {
                    self.evaluate_where(record, &combined_columns, where_clause).unwrap_or(false)
                } else {
                    true
                }
            })
            .collect();

        // 检查是否包含聚合函数
        let has_aggregate = stmt.columns.iter().any(|c| {
            matches!(c, SelectColumn::Aggregate(_))
        });

        if has_aggregate {
            let aggregate_row = self.compute_aggregates(&stmt.columns, &filtered_records, &combined_columns)?;
            let result = QueryResult {
                columns: stmt.columns.clone(),
                rows: vec![aggregate_row],
                table_columns: combined_columns,
            };
            return Ok(ExecuteResult::Query(result));
        }

        // 应用ORDER BY排序
        if !stmt.order_by.is_empty() {
            filtered_records.sort_by(|a, b| {
                for order in &stmt.order_by {
                    let col_idx = combined_columns.iter()
                        .position(|c| c.name == order.column)
                        .unwrap_or(0);
                    let a_val = &a.values[col_idx];
                    let b_val = &b.values[col_idx];
                    let cmp = a_val.partial_cmp(b_val).unwrap_or(std::cmp::Ordering::Equal);
                    if cmp != std::cmp::Ordering::Equal {
                        return if order.descending { cmp.reverse() } else { cmp };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // 应用OFFSET和LIMIT
        if let Some(offset) = stmt.offset {
            let offset = offset as usize;
            if offset < filtered_records.len() {
                filtered_records = filtered_records.split_off(offset);
            } else {
                filtered_records.clear();
            }
        }

        if let Some(limit) = stmt.limit {
            let limit = limit as usize;
            if limit < filtered_records.len() {
                filtered_records.truncate(limit);
            }
        }

        let result = QueryResult {
            columns: stmt.columns.clone(),
            rows: filtered_records,
            table_columns: combined_columns,
        };

        Ok(ExecuteResult::Query(result))
    }

    /// 合并两个记录
    fn combine_records(&self, main: &Record, join: &Record) -> Record {
        let mut values = main.values.clone();
        values.extend(join.values.clone());
        Record::new(values)
    }

    /// 生成JOIN结果 (处理多表JOIN的笛卡尔积)
    fn generate_join_results(
        &self,
        main_record: &Record,
        matches: &[Vec<(Record, bool)>],
        results: &mut Vec<Record>,
    ) {
        if matches.is_empty() {
            results.push(main_record.clone());
            return;
        }

        // 递归生成笛卡尔积
        self.generate_cartesian_product(main_record, matches, 0, Record::new(vec![]), results);
    }

    fn generate_cartesian_product(
        &self,
        main_record: &Record,
        matches: &[Vec<(Record, bool)>],
        depth: usize,
        current: Record,
        results: &mut Vec<Record>,
    ) {
        if depth == matches.len() {
            // 合并主记录和当前JOIN记录
            let mut final_values = main_record.values.clone();
            final_values.extend(current.values);
            results.push(Record::new(final_values));
            return;
        }

        for (join_record, _) in &matches[depth] {
            let mut new_current = Record::new(current.values.clone());
            new_current.values.extend(join_record.values.clone());
            self.generate_cartesian_product(main_record, matches, depth + 1, new_current, results);
        }
    }

    /// 计算聚合函数
    fn compute_aggregates(&self, columns: &[SelectColumn], records: &[Record], table_columns: &[ColumnDef]) -> Result<Record> {
        let mut values = Vec::new();

        for col in columns {
            match col {
                SelectColumn::Aggregate(func) => {
                    let value = match func {
                        AggregateFunc::CountStar => {
                            Value::Integer(records.len() as i64)
                        }
                        AggregateFunc::Count(expr) => {
                            let col_idx = self.get_column_index(expr, table_columns)?;
                            let count = records.iter()
                                .filter(|r| r.values[col_idx] != Value::Null)
                                .count();
                            Value::Integer(count as i64)
                        }
                        AggregateFunc::Sum(expr) => {
                            let col_idx = self.get_column_index(expr, table_columns)?;
                            let sum: i64 = records.iter()
                                .filter_map(|r| {
                                    if let Value::Integer(n) = &r.values[col_idx] {
                                        Some(*n)
                                    } else {
                                        None
                                    }
                                })
                                .sum();
                            Value::Integer(sum)
                        }
                        AggregateFunc::Avg(expr) => {
                            let col_idx = self.get_column_index(expr, table_columns)?;
                            let sum: i64 = records.iter()
                                .filter_map(|r| {
                                    if let Value::Integer(n) = &r.values[col_idx] {
                                        Some(*n)
                                    } else {
                                        None
                                    }
                                })
                                .sum();
                            let count = records.iter()
                                .filter(|r| r.values[col_idx] != Value::Null)
                                .count() as i64;
                            if count > 0 {
                                Value::Real(sum as f64 / count as f64)
                            } else {
                                Value::Null
                            }
                        }
                        AggregateFunc::Min(expr) => {
                            let col_idx = self.get_column_index(expr, table_columns)?;
                            records.iter()
                                .filter(|r| r.values[col_idx] != Value::Null)
                                .map(|r| &r.values[col_idx])
                                .min()
                                .cloned()
                                .unwrap_or(Value::Null)
                        }
                        AggregateFunc::Max(expr) => {
                            let col_idx = self.get_column_index(expr, table_columns)?;
                            records.iter()
                                .filter(|r| r.values[col_idx] != Value::Null)
                                .map(|r| &r.values[col_idx])
                                .max()
                                .cloned()
                                .unwrap_or(Value::Null)
                        }
                    };
                    values.push(value);
                }
                _ => {
                    // Non-aggregate columns in aggregate query - not supported yet
                    values.push(Value::Null);
                }
            }
        }

        Ok(Record::new(values))
    }

    fn get_column_index(&self, expr: &Expression, table_columns: &[ColumnDef]) -> Result<usize> {
        match expr {
            Expression::Column(col_name) => {
                table_columns.iter()
                    .position(|c| c.name == *col_name)
                    .ok_or(ExecutorError::ColumnNotFound(col_name.clone()))
            }
            _ => Err(ExecutorError::NotImplemented("Only column expressions supported in aggregates".to_string()))
        }
    }

    /// 按GROUP BY列分组记录
    fn group_records(
        &self,
        records: &[Record],
        table_columns: &[ColumnDef],
        group_by: &[String],
    ) -> Result<Vec<(Vec<Value>, Vec<Record>)>> {
        use std::collections::HashMap;

        let mut groups: HashMap<Vec<Value>, Vec<Record>> = HashMap::new();

        for record in records {
            let key: Vec<Value> = group_by.iter()
                .map(|col_name| {
                    let col_idx = table_columns.iter()
                        .position(|c| c.name == *col_name)
                        .unwrap_or(0);
                    record.values[col_idx].clone()
                })
                .collect();

            groups.entry(key).or_default().push(record.clone());
        }

        Ok(groups.into_iter().collect())
    }

    /// 计算聚合结果，包含GROUP BY列值
    fn compute_aggregates_with_group(
        &self,
        columns: &[SelectColumn],
        records: &[Record],
        table_columns: &[ColumnDef],
        group_key: &[Value],
        group_by: &[String],
    ) -> Result<Record> {
        let mut values = Vec::new();

        for col in columns {
            match col {
                SelectColumn::Aggregate(func) => {
                    let value = match func {
                        AggregateFunc::CountStar => {
                            Value::Integer(records.len() as i64)
                        }
                        AggregateFunc::Count(expr) => {
                            let col_idx = self.get_column_index(expr, table_columns)?;
                            let count = records.iter()
                                .filter(|r| r.values[col_idx] != Value::Null)
                                .count();
                            Value::Integer(count as i64)
                        }
                        AggregateFunc::Sum(expr) => {
                            let col_idx = self.get_column_index(expr, table_columns)?;
                            let sum: i64 = records.iter()
                                .filter_map(|r| {
                                    if let Value::Integer(n) = &r.values[col_idx] {
                                        Some(*n)
                                    } else {
                                        None
                                    }
                                })
                                .sum();
                            Value::Integer(sum)
                        }
                        AggregateFunc::Avg(expr) => {
                            let col_idx = self.get_column_index(expr, table_columns)?;
                            let sum: i64 = records.iter()
                                .filter_map(|r| {
                                    if let Value::Integer(n) = &r.values[col_idx] {
                                        Some(*n)
                                    } else {
                                        None
                                    }
                                })
                                .sum();
                            let count = records.iter()
                                .filter(|r| r.values[col_idx] != Value::Null)
                                .count() as i64;
                            if count > 0 {
                                Value::Real(sum as f64 / count as f64)
                            } else {
                                Value::Null
                            }
                        }
                        AggregateFunc::Min(expr) => {
                            let col_idx = self.get_column_index(expr, table_columns)?;
                            records.iter()
                                .filter(|r| r.values[col_idx] != Value::Null)
                                .map(|r| &r.values[col_idx])
                                .min()
                                .cloned()
                                .unwrap_or(Value::Null)
                        }
                        AggregateFunc::Max(expr) => {
                            let col_idx = self.get_column_index(expr, table_columns)?;
                            records.iter()
                                .filter(|r| r.values[col_idx] != Value::Null)
                                .map(|r| &r.values[col_idx])
                                .max()
                                .cloned()
                                .unwrap_or(Value::Null)
                        }
                    };
                    values.push(value);
                }
                SelectColumn::Column(col_name) => {
                    // 如果是GROUP BY列，使用group_key值
                    if let Some(idx) = group_by.iter().position(|g| g == col_name) {
                        values.push(group_key[idx].clone());
                    } else {
                        // 非聚合非GROUP BY列，返回NULL
                        values.push(Value::Null);
                    }
                }
                _ => {
                    values.push(Value::Null);
                }
            }
        }

        Ok(Record::new(values))
    }

    /// 评估HAVING条件
    fn evaluate_having(
        &self,
        record: &Record,
        columns: &[SelectColumn],
        having_clause: &Expression,
    ) -> Result<bool> {
        // 创建一个临时的列定义列表用于评估
        let temp_columns: Vec<ColumnDef> = columns.iter()
            .enumerate()
            .map(|(i, col)| {
                let name = match col {
                    SelectColumn::Column(n) => n.clone(),
                    SelectColumn::Aggregate(_) => format!("agg_{}", i),
                    SelectColumn::All => format!("col_{}", i),
                };
                ColumnDef {
                    name,
                    data_type: crate::sql::ast::DataType::Integer,
                    nullable: true,
                    primary_key: false,
                }
            })
            .collect();

        self.evaluate_where(record, &temp_columns, having_clause)
    }

    /// 执行UPDATE
    fn execute_update(&mut self, stmt: &crate::sql::ast::UpdateStmt) -> Result<ExecuteResult> {
        // 获取表定义
        let table = self.db.get_table(&stmt.table)
            .ok_or(ExecutorError::TableNotFound(stmt.table.clone()))?;
        let table_columns = table.columns.clone();
        let table_name = stmt.table.clone();

        // 获取所有记录（包含rowid）
        let all_records = self.db.select_all_with_rowid(&table_name)?;
        let mut updated_count = 0;

        // 遍历记录并更新符合条件的
        for (rowid, record) in all_records {
            let should_update = if let Some(ref where_clause) = stmt.where_clause {
                self.evaluate_where(&record, &table_columns, where_clause).unwrap_or(false)
            } else {
                true
            };

            if should_update {
                let mut new_record = record.clone();
                for set_clause in &stmt.set_clauses {
                    let col_idx = table_columns.iter()
                        .position(|c| c.name == set_clause.column)
                        .ok_or(ExecutorError::ColumnNotFound(set_clause.column.clone()))?;
                    let value = self.evaluate_expression(&set_clause.value)?;
                    new_record.values[col_idx] = value;
                }
                // 实际更新记录
                self.db.update(&table_name, rowid, new_record)?;
                updated_count += 1;
            }
        }

        Ok(ExecuteResult::Success(format!("{} row(s) updated", updated_count)))
    }

    /// 执行DELETE
    fn execute_delete(&mut self, stmt: &crate::sql::ast::DeleteStmt) -> Result<ExecuteResult> {
        // 获取表定义
        let table = self.db.get_table(&stmt.table)
            .ok_or(ExecutorError::TableNotFound(stmt.table.clone()))?;
        let table_columns = table.columns.clone();
        let table_name = stmt.table.clone();

        // 获取所有记录（包含rowid）
        let all_records = self.db.select_all_with_rowid(&table_name)?;
        let mut deleted_count = 0;

        // 遍历记录并删除符合条件的
        for (rowid, record) in all_records {
            let should_delete = if let Some(ref where_clause) = stmt.where_clause {
                self.evaluate_where(&record, &table_columns, where_clause).unwrap_or(false)
            } else {
                true // 如果没有WHERE，删除所有记录
            };

            if should_delete {
                // 实际删除记录
                self.db.delete(&table_name, rowid)?;
                deleted_count += 1;
            }
        }

        Ok(ExecuteResult::Success(format!("{} row(s) deleted", deleted_count)))
    }

    /// 执行DROP TABLE
    fn execute_drop_table(&mut self, stmt: &crate::sql::ast::DropTableStmt) -> Result<ExecuteResult> {
        self.db.drop_table(&stmt.table)?;
        Ok(ExecuteResult::Success(format!("Table '{}' dropped", stmt.table)))
    }

    /// 评估表达式
    fn evaluate_expression(&self, expr: &Expression) -> Result<Value> {
        match expr {
            Expression::Integer(n) => Ok(Value::Integer(*n)),
            Expression::String(s) => Ok(Value::Text(s.clone())),
            Expression::Float(f) => Ok(Value::Real(*f)),
            Expression::Boolean(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
            Expression::Null => Ok(Value::Null),
            Expression::Column(_) => Err(ExecutorError::NotImplemented("Column reference in value".to_string())),
            Expression::Placeholder(_) => Err(ExecutorError::NotImplemented("Unbound placeholder - use execute_prepared with parameters".to_string())),
            Expression::Binary { .. } => Err(ExecutorError::NotImplemented("Binary expression in value".to_string())),
        }
    }

    /// 评估WHERE条件
    fn evaluate_where(&self, record: &Record, table_columns: &[ColumnDef], expr: &Expression) -> Result<bool> {
        match expr {
            Expression::Binary { left, op, right } => {
                match op {
                    BinaryOp::Equal => {
                        let left_val = self.evaluate_expression_in_record(record, table_columns, left)?;
                        let right_val = self.evaluate_expression_in_record(record, table_columns, right)?;
                        Ok(left_val == right_val)
                    }
                    BinaryOp::NotEqual => {
                        let left_val = self.evaluate_expression_in_record(record, table_columns, left)?;
                        let right_val = self.evaluate_expression_in_record(record, table_columns, right)?;
                        Ok(left_val != right_val)
                    }
                    BinaryOp::Less => {
                        let left_val = self.evaluate_expression_in_record(record, table_columns, left)?;
                        let right_val = self.evaluate_expression_in_record(record, table_columns, right)?;
                        Ok(left_val < right_val)
                    }
                    BinaryOp::Greater => {
                        let left_val = self.evaluate_expression_in_record(record, table_columns, left)?;
                        let right_val = self.evaluate_expression_in_record(record, table_columns, right)?;
                        Ok(left_val > right_val)
                    }
                    BinaryOp::LessEqual => {
                        let left_val = self.evaluate_expression_in_record(record, table_columns, left)?;
                        let right_val = self.evaluate_expression_in_record(record, table_columns, right)?;
                        Ok(left_val <= right_val)
                    }
                    BinaryOp::GreaterEqual => {
                        let left_val = self.evaluate_expression_in_record(record, table_columns, left)?;
                        let right_val = self.evaluate_expression_in_record(record, table_columns, right)?;
                        Ok(left_val >= right_val)
                    }
                    BinaryOp::And => {
                        let left_bool = self.evaluate_where(record, table_columns, left)?;
                        if !left_bool {
                            Ok(false)
                        } else {
                            self.evaluate_where(record, table_columns, right)
                        }
                    }
                    BinaryOp::Or => {
                        let left_bool = self.evaluate_where(record, table_columns, left)?;
                        if left_bool {
                            Ok(true)
                        } else {
                            self.evaluate_where(record, table_columns, right)
                        }
                    }
                    _ => Err(ExecutorError::NotImplemented(format!("Binary op {:?}", op))),
                }
            }
            _ => Err(ExecutorError::NotImplemented("Non-binary WHERE clause".to_string())),
        }
    }

    /// 在记录上下文中评估表达式
    fn evaluate_expression_in_record(&self, record: &Record, table_columns: &[ColumnDef], expr: &Expression) -> Result<Value> {
        match expr {
            Expression::Column(col_name) => {
                let col_idx = table_columns.iter()
                    .position(|c| c.name == *col_name)
                    .ok_or(ExecutorError::ColumnNotFound(col_name.clone()))?;
                Ok(record.values[col_idx].clone())
            }
            _ => self.evaluate_expression(expr),
        }
    }

    /// 刷新数据库到磁盘
    pub fn flush(&mut self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }
}

/// 执行结果
#[derive(Debug)]
pub enum ExecuteResult {
    /// 成功消息
    Success(String),
    /// 查询结果
    Query(QueryResult),
}

/// 查询结果
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<crate::sql::ast::SelectColumn>,
    pub rows: Vec<Record>,
    pub table_columns: Vec<ColumnDef>,
}

impl QueryResult {
    /// 打印结果
    pub fn print(&self) {
        // 打印列名
        let col_names: Vec<String> = self.columns.iter()
            .map(|c| match c {
                crate::sql::ast::SelectColumn::All => "*".to_string(),
                crate::sql::ast::SelectColumn::Column(name) => name.clone(),
                crate::sql::ast::SelectColumn::Aggregate(agg) => format!("{:?}", agg),
            })
            .collect();
        println!("{}", col_names.join(" | "));
        println!("{}", "-".repeat(50));

        // 打印行
        for record in &self.rows {
            let values: Vec<String> = record.values.iter()
                .map(|v| format!("{}", v))
                .collect();
            println!("{}", values.join(" | "));
        }

        println!("({} row(s))", self.rows.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::Parser;
    use tempfile::NamedTempFile;

    #[test]
    fn test_executor_create_table() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // 解析并执行 CREATE TABLE
        let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();

        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Success(msg) => {
                assert!(msg.contains("Table 'users' created"));
            }
            _ => panic!("Expected Success result"),
        }
    }

    #[test]
    fn test_executor_insert_and_select() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE
        let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT
        let sql = "INSERT INTO users VALUES (1, 'Alice')";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Success(msg) => {
                assert!(msg.contains("1 row(s) inserted"));
            }
            _ => panic!("Expected Success result"),
        }

        // SELECT
        let sql = "SELECT * FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 1);
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_where_clause() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE
        let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT multiple rows
        for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Charlie")] {
            let sql = format!("INSERT INTO users VALUES ({}, '{}')", id, name);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // SELECT with WHERE id = 1
        let sql = "SELECT * FROM users WHERE id = 1";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 1);
                assert_eq!(query_result.rows[0].values[0], Value::Integer(1));
            }
            _ => panic!("Expected Query result"),
        }

        // SELECT with WHERE id > 1
        let sql = "SELECT * FROM users WHERE id > 1";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 2);
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_limit_offset() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE
        let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT 5 rows
        for i in 1..=5 {
            let sql = format!("INSERT INTO users VALUES ({}, 'User{}')", i, i);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // SELECT with LIMIT 2
        let sql = "SELECT * FROM users LIMIT 2";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 2);
            }
            _ => panic!("Expected Query result"),
        }

        // SELECT with LIMIT 2 OFFSET 2
        let sql = "SELECT * FROM users LIMIT 2 OFFSET 2";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 2);
                // Should get users 3 and 4
                assert_eq!(query_result.rows[0].values[0], Value::Integer(3));
                assert_eq!(query_result.rows[1].values[0], Value::Integer(4));
            }
            _ => panic!("Expected Query result"),
        }

        // SELECT with large OFFSET (should return empty)
        let sql = "SELECT * FROM users LIMIT 10 OFFSET 10";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 0);
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_order_by() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE
        let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT rows in random order
        let rows = vec![
            (3, "Charlie"),
            (1, "Alice"),
            (2, "Bob"),
        ];
        for (id, name) in rows {
            let sql = format!("INSERT INTO users VALUES ({}, '{}')", id, name);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // SELECT with ORDER BY id ASC
        let sql = "SELECT * FROM users ORDER BY id";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 3);
                assert_eq!(query_result.rows[0].values[0], Value::Integer(1));
                assert_eq!(query_result.rows[1].values[0], Value::Integer(2));
                assert_eq!(query_result.rows[2].values[0], Value::Integer(3));
            }
            _ => panic!("Expected Query result"),
        }

        // SELECT with ORDER BY id DESC
        let sql = "SELECT * FROM users ORDER BY id DESC";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 3);
                assert_eq!(query_result.rows[0].values[0], Value::Integer(3));
                assert_eq!(query_result.rows[1].values[0], Value::Integer(2));
                assert_eq!(query_result.rows[2].values[0], Value::Integer(1));
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_aggregate_functions() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE with salary column
        let sql = "CREATE TABLE employees (id INTEGER, name TEXT, salary INTEGER)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT 5 rows with salaries: 3000, 4000, 5000, 6000, 7000
        let salaries = vec![3000, 4000, 5000, 6000, 7000];
        for (i, salary) in salaries.iter().enumerate() {
            let sql = format!("INSERT INTO employees VALUES ({}, 'Employee{}', {})", i + 1, i + 1, salary);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // Test COUNT(*)
        let sql = "SELECT COUNT(*) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 1);
                assert_eq!(query_result.rows[0].values[0], Value::Integer(5));
            }
            _ => panic!("Expected Query result"),
        }

        // Test COUNT(column)
        let sql = "SELECT COUNT(id) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows[0].values[0], Value::Integer(5));
            }
            _ => panic!("Expected Query result"),
        }

        // Test SUM
        let sql = "SELECT SUM(salary) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                // 3000 + 4000 + 5000 + 6000 + 7000 = 25000
                assert_eq!(query_result.rows[0].values[0], Value::Integer(25000));
            }
            _ => panic!("Expected Query result"),
        }

        // Test AVG
        let sql = "SELECT AVG(salary) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                // AVG(25000 / 5) = 5000.0 (as Real)
                assert_eq!(query_result.rows[0].values[0], Value::Real(5000.0));
            }
            _ => panic!("Expected Query result"),
        }

        // Test MIN
        let sql = "SELECT MIN(salary) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows[0].values[0], Value::Integer(3000));
            }
            _ => panic!("Expected Query result"),
        }

        // Test MAX
        let sql = "SELECT MAX(salary) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows[0].values[0], Value::Integer(7000));
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_aggregate_with_where() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE
        let sql = "CREATE TABLE employees (id INTEGER, salary INTEGER)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT rows with different salaries
        for (id, salary) in [(1, 1000), (2, 2000), (3, 3000), (4, 4000)] {
            let sql = format!("INSERT INTO employees VALUES ({}, {})", id, salary);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // Test COUNT with WHERE
        let sql = "SELECT COUNT(*) FROM employees WHERE salary > 1500";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                // 2000, 3000, 4000 = 3 rows
                assert_eq!(query_result.rows[0].values[0], Value::Integer(3));
            }
            _ => panic!("Expected Query result"),
        }

        // Test SUM with WHERE
        let sql = "SELECT SUM(salary) FROM employees WHERE id <= 2";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                // 1000 + 2000 = 3000
                assert_eq!(query_result.rows[0].values[0], Value::Integer(3000));
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_aggregate_empty_table() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE (no rows)
        let sql = "CREATE TABLE employees (id INTEGER, salary INTEGER)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // Test COUNT on empty table - should return 0
        let sql = "SELECT COUNT(*) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 1);
                assert_eq!(query_result.rows[0].values[0], Value::Integer(0));
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_inner_join() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE users (avoiding column name conflicts by using different names)
        let sql = "CREATE TABLE users (user_id INTEGER, user_name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // CREATE TABLE orders
        let sql = "CREATE TABLE orders (order_id INTEGER, uid INTEGER, amount INTEGER)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT users
        for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Charlie")] {
            let sql = format!("INSERT INTO users VALUES ({}, '{}')", id, name);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // INSERT orders (Alice: 2 orders, Bob: 1 order, Charlie: 0 orders)
        let orders = vec![
            (1, 1, 100),  // Alice's order
            (2, 1, 200),  // Alice's order
            (3, 2, 150),  // Bob's order
        ];
        for (oid, uid, amount) in orders {
            let sql = format!("INSERT INTO orders VALUES ({}, {}, {})", oid, uid, amount);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // Test INNER JOIN using simple column names
        let sql = "SELECT * FROM users JOIN orders ON user_id = uid";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                // Should return 3 rows (2 for Alice + 1 for Bob)
                assert_eq!(query_result.rows.len(), 3);

                // First row: Alice + first order
                // Columns: user_id(0), user_name(1), order_id(2), uid(3), amount(4)
                assert_eq!(query_result.rows[0].values[0], Value::Integer(1));
                assert_eq!(query_result.rows[0].values[1], Value::Text("Alice".to_string()));
                assert_eq!(query_result.rows[0].values[2], Value::Integer(1)); // order_id
                assert_eq!(query_result.rows[0].values[4], Value::Integer(100)); // amount
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_left_join() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE users
        let sql = "CREATE TABLE users (user_id INTEGER, user_name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // CREATE TABLE orders
        let sql = "CREATE TABLE orders (order_id INTEGER, uid INTEGER, amount INTEGER)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT users
        for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Charlie")] {
            let sql = format!("INSERT INTO users VALUES ({}, '{}')", id, name);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // INSERT orders (only for Alice)
        let sql = "INSERT INTO orders VALUES (1, 1, 100)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // Test LEFT JOIN - should include all users
        let sql = "SELECT * FROM users LEFT JOIN orders ON user_id = uid";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                // Should return 3 rows (1 for each user)
                assert_eq!(query_result.rows.len(), 3);

                // Alice has an order
                assert_eq!(query_result.rows[0].values[0], Value::Integer(1));
                assert_eq!(query_result.rows[0].values[1], Value::Text("Alice".to_string()));

                // Bob has no order - orders columns should be NULL
                assert_eq!(query_result.rows[1].values[0], Value::Integer(2));
                assert_eq!(query_result.rows[1].values[1], Value::Text("Bob".to_string()));
                // Orders columns should be NULL
                assert_eq!(query_result.rows[1].values[3], Value::Null);

                // Charlie has no order
                assert_eq!(query_result.rows[2].values[0], Value::Integer(3));
                assert_eq!(query_result.rows[2].values[1], Value::Text("Charlie".to_string()));
                assert_eq!(query_result.rows[2].values[3], Value::Null);
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_join_with_where() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE users
        let sql = "CREATE TABLE users (user_id INTEGER, user_name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // CREATE TABLE orders
        let sql = "CREATE TABLE orders (order_id INTEGER, uid INTEGER, amount INTEGER)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT users
        for (id, name) in [(1, "Alice"), (2, "Bob")] {
            let sql = format!("INSERT INTO users VALUES ({}, '{}')", id, name);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // INSERT orders
        let orders = vec![
            (1, 1, 100),
            (2, 1, 200),
            (3, 2, 50),
        ];
        for (oid, uid, amount) in orders {
            let sql = format!("INSERT INTO orders VALUES ({}, {}, {})", oid, uid, amount);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // Test JOIN with WHERE
        let sql = "SELECT * FROM users JOIN orders ON user_id = uid WHERE amount > 75";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                // Should return 2 rows (Alice's orders with amount > 75)
                assert_eq!(query_result.rows.len(), 2);

                // Both should be Alice's orders
                assert_eq!(query_result.rows[0].values[1], Value::Text("Alice".to_string()));
                assert_eq!(query_result.rows[1].values[1], Value::Text("Alice".to_string()));
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_group_by() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE employees with department
        let sql = "CREATE TABLE employees (id INTEGER, dept TEXT, salary INTEGER)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT employees: HR dept (2 people), IT dept (3 people)
        let employees = vec![
            (1, "HR", 3000),
            (2, "HR", 4000),
            (3, "IT", 5000),
            (4, "IT", 6000),
            (5, "IT", 7000),
        ];
        for (id, dept, salary) in employees {
            let sql = format!("INSERT INTO employees VALUES ({}, '{}', {})", id, dept, salary);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // Test GROUP BY with COUNT
        let sql = "SELECT dept, COUNT(*) FROM employees GROUP BY dept";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                // Should return 2 groups (HR and IT)
                assert_eq!(query_result.rows.len(), 2);

                // Find HR group
                let hr_row = query_result.rows.iter()
                    .find(|r| r.values[0] == Value::Text("HR".to_string()))
                    .unwrap();
                assert_eq!(hr_row.values[1], Value::Integer(2)); // 2 employees in HR

                // Find IT group
                let it_row = query_result.rows.iter()
                    .find(|r| r.values[0] == Value::Text("IT".to_string()))
                    .unwrap();
                assert_eq!(it_row.values[1], Value::Integer(3)); // 3 employees in IT
            }
            _ => panic!("Expected Query result"),
        }

        // Test GROUP BY with SUM
        let sql = "SELECT dept, SUM(salary) FROM employees GROUP BY dept";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                // HR: 3000 + 4000 = 7000
                let hr_row = query_result.rows.iter()
                    .find(|r| r.values[0] == Value::Text("HR".to_string()))
                    .unwrap();
                assert_eq!(hr_row.values[1], Value::Integer(7000));

                // IT: 5000 + 6000 + 7000 = 18000
                let it_row = query_result.rows.iter()
                    .find(|r| r.values[0] == Value::Text("IT".to_string()))
                    .unwrap();
                assert_eq!(it_row.values[1], Value::Integer(18000));
            }
            _ => panic!("Expected Query result"),
        }

        // Test GROUP BY with AVG
        let sql = "SELECT dept, AVG(salary) FROM employees GROUP BY dept";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                // HR: (3000 + 4000) / 2 = 3500
                let hr_row = query_result.rows.iter()
                    .find(|r| r.values[0] == Value::Text("HR".to_string()))
                    .unwrap();
                assert_eq!(hr_row.values[1], Value::Real(3500.0));

                // IT: (5000 + 6000 + 7000) / 3 = 6000
                let it_row = query_result.rows.iter()
                    .find(|r| r.values[0] == Value::Text("IT".to_string()))
                    .unwrap();
                assert_eq!(it_row.values[1], Value::Real(6000.0));
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_having() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE employees
        let sql = "CREATE TABLE employees (id INTEGER, dept TEXT, salary INTEGER)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT employees
        let employees = vec![
            (1, "HR", 3000),
            (2, "HR", 4000),
            (3, "IT", 5000),
            (4, "IT", 6000),
            (5, "IT", 7000),
            (6, "Sales", 2000),
        ];
        for (id, dept, salary) in employees {
            let sql = format!("INSERT INTO employees VALUES ({}, '{}', {})", id, dept, salary);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // Note: HAVING with aggregate functions like COUNT(*), SUM(salary) in the
        // condition requires referencing the computed aggregate values by name.
        // For now, we test that the HAVING clause is parsed correctly.
        // Full support would require aliasing or column reference resolution.

        // Test HAVING with simple column condition (dept != 'Sales')
        let sql = "SELECT dept, COUNT(*) FROM employees GROUP BY dept HAVING dept != 'Sales'";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                // Should return HR and IT, but not Sales
                assert_eq!(query_result.rows.len(), 2);
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_transaction_commit() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE
        let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // BEGIN TRANSACTION
        let sql = "BEGIN TRANSACTION";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Success(msg) => {
                assert!(msg.contains("Transaction started"));
            }
            _ => panic!("Expected Success result"),
        }

        // INSERT within transaction
        let sql = "INSERT INTO users VALUES (1, 'Alice')";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // Verify data is visible within transaction
        let sql = "SELECT * FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 1);
            }
            _ => panic!("Expected Query result"),
        }

        // COMMIT
        let sql = "COMMIT";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Success(msg) => {
                assert!(msg.contains("Transaction committed"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify data persists after commit
        let sql = "SELECT * FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 1);
                assert_eq!(query_result.rows[0].values[0], Value::Integer(1));
                assert_eq!(query_result.rows[0].values[1], Value::Text("Alice".to_string()));
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_transaction_rollback() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE
        let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // Insert initial data
        let sql = "INSERT INTO users VALUES (1, 'Alice')";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // BEGIN TRANSACTION
        let sql = "BEGIN TRANSACTION";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT within transaction
        let sql = "INSERT INTO users VALUES (2, 'Bob')";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // Verify data is visible within transaction (2 rows)
        let sql = "SELECT * FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 2);
            }
            _ => panic!("Expected Query result"),
        }

        // ROLLBACK
        let sql = "ROLLBACK";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Success(msg) => {
                assert!(msg.contains("Transaction rolled back"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify only original data remains (1 row)
        let sql = "SELECT * FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 1);
                assert_eq!(query_result.rows[0].values[0], Value::Integer(1));
                assert_eq!(query_result.rows[0].values[1], Value::Text("Alice".to_string()));
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_transaction_nested_begin_error() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // BEGIN TRANSACTION
        let sql = "BEGIN TRANSACTION";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // Try to begin another transaction (should fail)
        let sql = "BEGIN TRANSACTION";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt);
        assert!(result.is_err());
    }

    #[test]
    fn test_executor_transaction_commit_without_begin_error() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // Try to commit without beginning transaction (should fail)
        let sql = "COMMIT";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt);
        assert!(result.is_err());
    }

    #[test]
    fn test_executor_create_index() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE
        let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // CREATE INDEX
        let sql = "CREATE INDEX idx_name ON users (name)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Success(msg) => {
                assert!(msg.contains("Index 'idx_name' created"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify index exists in database
        let db = &executor.db;
        let index = db.get_index("idx_name");
        assert!(index.is_some());
        let index = index.unwrap();
        assert_eq!(index.table, "users");
        assert_eq!(index.column, "name");
    }

    #[test]
    fn test_executor_index_updated_on_insert() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE
        let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // CREATE INDEX
        let sql = "CREATE INDEX idx_name ON users (name)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT records
        for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Alice")] {
            let sql = format!("INSERT INTO users VALUES ({}, '{}')", id, name);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // Verify index contains the values
        let db = &executor.db;
        let index = db.get_index("idx_name").unwrap();

        // Lookup 'Alice' should return 2 rowids
        let alice_rowids = index.lookup(&Value::Text("Alice".to_string()));
        assert!(alice_rowids.is_some());
        let alice_rowids = alice_rowids.unwrap();
        assert_eq!(alice_rowids.len(), 2);

        // Lookup 'Bob' should return 1 rowid
        let bob_rowids = index.lookup(&Value::Text("Bob".to_string()));
        assert!(bob_rowids.is_some());
        let bob_rowids = bob_rowids.unwrap();
        assert_eq!(bob_rowids.len(), 1);

        // Lookup 'Charlie' should return None
        let charlie_rowids = index.lookup(&Value::Text("Charlie".to_string()));
        assert!(charlie_rowids.is_none());
    }

    #[test]
    fn test_executor_update() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE
        let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT record
        let sql = "INSERT INTO users VALUES (1, 'Alice')";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // Verify initial value
        let sql = "SELECT * FROM users WHERE id = 1";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 1);
                assert_eq!(query_result.rows[0].values[1], Value::Text("Alice".to_string()));
            }
            _ => panic!("Expected Query result"),
        }

        // UPDATE record
        let sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Success(msg) => {
                assert!(msg.contains("1 row(s) updated"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify updated value
        let sql = "SELECT * FROM users WHERE id = 1";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 1);
                assert_eq!(query_result.rows[0].values[0], Value::Integer(1));
                assert_eq!(query_result.rows[0].values[1], Value::Text("Bob".to_string()));
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_update_with_where() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE
        let sql = "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT records
        for (id, name, age) in [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)] {
            let sql = format!("INSERT INTO users VALUES ({}, '{}', {})", id, name, age);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // UPDATE only users with age > 25
        let sql = "UPDATE users SET name = 'Senior' WHERE age > 25";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Success(msg) => {
                assert!(msg.contains("2 row(s) updated")); // Bob and Charlie
            }
            _ => panic!("Expected Success result"),
        }

        // Verify Alice is unchanged
        let sql = "SELECT * FROM users WHERE id = 1";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows[0].values[1], Value::Text("Alice".to_string()));
            }
            _ => panic!("Expected Query result"),
        }

        // Verify Bob is updated
        let sql = "SELECT * FROM users WHERE id = 2";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows[0].values[1], Value::Text("Senior".to_string()));
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_delete() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE
        let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT records
        for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Charlie")] {
            let sql = format!("INSERT INTO users VALUES ({}, '{}')", id, name);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // Verify all 3 records exist
        let sql = "SELECT * FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 3);
            }
            _ => panic!("Expected Query result"),
        }

        // DELETE one record
        let sql = "DELETE FROM users WHERE id = 2";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Success(msg) => {
                assert!(msg.contains("1 row(s) deleted"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify only 2 records remain
        let sql = "SELECT * FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 2);
                // Bob should be deleted
                let bob_exists = query_result.rows.iter()
                    .any(|r| r.values[1] == Value::Text("Bob".to_string()));
                assert!(!bob_exists, "Bob should have been deleted");
            }
            _ => panic!("Expected Query result"),
        }
    }

    #[test]
    fn test_executor_delete_without_where() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let mut executor = Executor::open(path).unwrap();

        // CREATE TABLE
        let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        executor.execute(&stmt).unwrap();

        // INSERT records
        for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Charlie")] {
            let sql = format!("INSERT INTO users VALUES ({}, '{}')", id, name);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            executor.execute(&stmt).unwrap();
        }

        // DELETE all records (no WHERE clause)
        let sql = "DELETE FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Success(msg) => {
                assert!(msg.contains("3 row(s) deleted"));
            }
            _ => panic!("Expected Success result"),
        }

        // Verify no records remain
        let sql = "SELECT * FROM users";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse().unwrap();
        let result = executor.execute(&stmt).unwrap();
        match result {
            ExecuteResult::Query(query_result) => {
                assert_eq!(query_result.rows.len(), 0);
            }
            _ => panic!("Expected Query result"),
        }
    }
}
