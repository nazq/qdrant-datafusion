# qdrant-datafusion Performance Optimizations

## Executive Summary

This document outlines **actionable optimization opportunities** for the qdrant-datafusion library, prioritized by performance impact. The codebase is already well-optimized with clean architecture, but several enhancements can improve memory efficiency, network utilization, and query performance for high-throughput production workloads.

**Current Status**: Production-ready with solid performance for typical workloads (0-10K points per query)
**Target**: Enterprise-scale optimization for 100K+ point queries and high-frequency workloads

---

## Table of Contents

1. [Optimization Priorities](#optimization-priorities)
2. [Critical Bottlenecks](#critical-bottlenecks)
3. [Quick Wins](#quick-wins)
4. [High-Value Features](#high-value-features)
5. [Advanced Optimizations](#advanced-optimizations)
6. [Implementation Roadmap](#implementation-roadmap)

---

## Optimization Priorities

### Impact vs Effort Matrix

```
High Impact │
           │  ┌─────────────┐
           │  │ 1. Pagination│
           │  │ 2. Filter   │
           │  │    Pushdown │
           │  └─────────────┘
           │
           │          ┌──────────────┐
           │          │ 5. HashMap   │
           │          │    Optimize  │
           │          └──────────────┘
           │
           │  ┌──────────────┐
           │  │ 3. Arc<Str> │
           │  │ 4. VecSelect│
           │  └──────────────┘       ┌──────────────┐
           │                         │ 6. Numeric ID│
           │                         │ 7. Binary    │
Low Impact │                         │    Payload   │
           └─────────────────────────┴──────────────┘
              Low Effort              High Effort
```

### Priority Ranking

| Priority | Optimization | Effort | Impact | Status |
|----------|--------------|--------|--------|--------|
| 🔴 **P0** | Pagination support | 2 days | HIGH | Critical for large queries |
| 🔴 **P0** | Filter pushdown | 2-3 days | HIGH | Major network/CPU savings |
| 🟡 **P1** | Arc\<String\> for collection | 1 hour | MEDIUM | Hot path optimization |
| 🟡 **P1** | VectorSelectorSpec Arc wrap | 1 hour | MEDIUM | Reduces clone overhead |
| 🟢 **P2** | HashMap → specialized lookup | 1 day | MEDIUM | Per-point allocation |
| 🟢 **P2** | Numeric ID support | 1 day | LOW | Type precision |
| ⚪ **P3** | Binary payload storage | 2 days | LOW | Advanced use case |

---

## Critical Bottlenecks

### 1. MEMORY SPIKE: Single-Batch Model

**Severity**: 🔴 **CRITICAL** for large result sets

#### Problem

**Location**: `src/table.rs:312`

```rust
let mut builder = QdrantRecordBatchBuilder::new(schema, points.len());
```

**Current behavior**:
- Entire result set fetched in single network call
- All points allocated in memory simultaneously
- RecordBatch built before first row returned

**Impact**:
```
Query returning 1,000,000 points:
- Average point size: 10KB (1024-dim vector + metadata)
- Total memory: 1M × 10KB = 10GB allocation spike
- Time to first row: Network + deserialization (no streaming)
```

#### Solution: Implement Pagination

**Design**:
```rust
// Add configuration
pub const BATCH_SIZE: usize = 10_000;  // Configurable chunk size

// Modify execute_qdrant_query to use scroll API
async fn execute_qdrant_query_paginated(
    client: Arc<Qdrant>,
    collection: String,
    schema: SchemaRef,
    vector_selector: VectorSelectorSpec,
    payload_selector: bool,
    filter: &Option<Vec<Expr>>,
    limit: Option<usize>,
    batch_size: usize,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    let mut offset = 0;
    let limit = limit.unwrap_or(usize::MAX);

    futures::stream::try_unfold(
        (client, collection, offset, limit),
        move |(client, collection, mut offset, remaining)| async move {
            if remaining == 0 {
                return Ok(None);
            }

            let chunk_size = std::cmp::min(batch_size, remaining);

            let mut query_builder = QueryPointsBuilder::new(&collection)
                .offset(offset as u64)
                .limit(chunk_size as u64);

            // Apply selectors...
            let response = client.query(query_builder).await?;
            let points = response.result;

            if points.is_empty() {
                return Ok(None);
            }

            // Build RecordBatch for this chunk
            let mut builder = QdrantRecordBatchBuilder::new(
                Arc::clone(&schema),
                points.len()
            );
            for point in points {
                builder.append_point(point);
            }
            let batch = builder.finish()?;

            offset += chunk_size;
            let remaining = remaining - chunk_size;

            Ok(Some((batch, (client, collection, offset, remaining))))
        },
    )
}
```

**Benefits**:
- **Memory**: O(batch_size × D) instead of O(total_points × D)
- **Latency**: First batch returned faster
- **Scalability**: Handles unlimited result sets

**Changes Required**:
1. `src/table.rs:269-320` - Replace single query with paginated stream
2. `src/stream.rs:13-23` - Modify QdrantQueryStream to handle multiple batches
3. Add configuration for batch size (env var or builder method)

**Testing**:
- E2E test with 50K+ point result set
- Verify memory usage stays bounded
- Benchmark latency improvement for first batch

**Estimated Effort**: 2 days (implementation + testing)

---

### 2. NO FILTER PUSHDOWN

**Severity**: 🔴 **HIGH** for selective queries

#### Problem

**Location**: `src/table.rs:181`

```rust
_filters: &[Expr],  // Currently IGNORED
```

**Current behavior**:
- DataFusion receives filters from SQL WHERE clause
- Filters NOT sent to Qdrant
- All data fetched from database
- DataFusion filters locally after network transfer

**Impact**:
```sql
-- Query: Find electronics in large collection
SELECT * FROM products WHERE payload->>'category' = 'electronics'

Current:
  1. Fetch 1,000,000 points from Qdrant (10GB network transfer)
  2. Filter in DataFusion → 100,000 matching points
  3. Total time: ~60 seconds (network + CPU)

With pushdown:
  1. Send filter to Qdrant
  2. Qdrant returns 100,000 points (1GB network transfer)
  3. Total time: ~6 seconds (10x faster)
```

#### Solution: Filter Expression Compilation

**Design**:

```rust
// New module: src/filter.rs
pub struct QdrantFilterCompiler {
    /// Maps payload keys to Qdrant field paths
    payload_schema: HashMap<String, FieldPath>,
}

impl QdrantFilterCompiler {
    pub fn compile(&self, expr: &Expr) -> Option<qdrant_client::Filter> {
        match expr {
            // Binary expressions
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                match op {
                    Operator::Eq => self.compile_eq(left, right),
                    Operator::NotEq => self.compile_ne(left, right),
                    Operator::Lt => self.compile_lt(left, right),
                    Operator::LtEq => self.compile_lte(left, right),
                    Operator::Gt => self.compile_gt(left, right),
                    Operator::GtEq => self.compile_gte(left, right),
                    Operator::And => self.compile_and(left, right),
                    Operator::Or => self.compile_or(left, right),
                    _ => None,  // Unsupported operator
                }
            }

            // Literal access: payload->>'key'
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                // Handle JSON extraction functions
                if func.name() == "json_extract" {
                    self.compile_payload_access(args)
                } else {
                    None
                }
            }

            // Unsupported expression
            _ => None,
        }
    }

    fn compile_eq(&self, left: &Expr, right: &Expr) -> Option<qdrant_client::Filter> {
        // Extract field and value
        let (field, value) = self.extract_comparison(left, right)?;

        // Build Qdrant filter
        Some(Filter {
            must: vec![Condition::Field(FieldCondition::new_match(
                field,
                value.into(),
            ))],
            ..Default::default()
        })
    }

    // ... other comparison operators
}
```

**Integration**:

```rust
// In execute_qdrant_query (src/table.rs:269)
async fn execute_qdrant_query(
    // ... existing params
    filter: &Option<Vec<Expr>>,  // Use instead of ignore
) -> DataFusionResult<RecordBatch> {
    let mut query_builder = QueryPointsBuilder::new(&collection);

    // NEW: Compile and apply filters
    if let Some(exprs) = filter {
        let compiler = QdrantFilterCompiler::new();
        if let Some(qdrant_filter) = compiler.compile_exprs(exprs) {
            query_builder = query_builder.filter(qdrant_filter);
        }
        // If compilation fails, DataFusion will filter locally (fallback)
    }

    // ... rest of query building
}
```

**Supported Filters** (Phase 1):

| DataFusion Expression | Qdrant Filter | Example |
|----------------------|---------------|---------|
| `payload->>'key' = 'value'` | `match` condition | Category filter |
| `payload->>'key' > 10` | `range` condition | Price filter |
| `AND` / `OR` | `must` / `should` | Combined conditions |
| `payload->>'key' IN (...)` | `match any` | Multi-value filter |

**Benefits**:
- **Network**: 10-100x reduction for selective queries
- **CPU**: Qdrant filters at database level (optimized)
- **Latency**: Proportional to result size, not collection size

**Changes Required**:
1. New file: `src/filter.rs` - Expression compiler
2. `src/table.rs:181` - Use filter parameter instead of ignore
3. `src/table.rs:285-299` - Add filter to query builder
4. Tests for all supported filter types

**Testing**:
- Unit tests for each filter operator
- E2E tests with complex filter combinations
- Benchmark queries before/after pushdown

**Estimated Effort**: 2-3 days (implementation + comprehensive testing)

---

## Quick Wins

### 3. Arc\<String\> for Collection Name

**Severity**: 🟡 **MEDIUM** (hot path allocation)

#### Problem

**Location**: `src/table.rs:344`

```rust
pub struct QdrantScanExec {
    collection: String,  // ← Owned String
}

fn execute(&self, ...) -> DataFusionResult<SendableRecordBatchStream> {
    let collection = self.collection.clone();  // ← String allocation per query
    // ...
}
```

**Impact**:
- Every query execution allocates new String for collection name
- Typical size: 20-50 bytes
- High-frequency workloads: 1000s of allocations/second

#### Solution

**Change field type**:
```rust
pub struct QdrantScanExec {
    collection: Arc<String>,  // ← Shared ownership
}
```

**Update clone sites**:
```rust
fn execute(&self, ...) -> DataFusionResult<SendableRecordBatchStream> {
    let collection = Arc::clone(&self.collection);  // ← Cheap pointer copy
    // ...
}
```

**Changes Required**:
- `src/table.rs:214` - Field type
- `src/table.rs:177` - QdrantTableProvider::scan() construction
- `src/table.rs:344` - execute() clone site

**Estimated Effort**: 30 minutes

---

### 4. VectorSelectorSpec Arc Wrapper

**Severity**: 🟡 **MEDIUM** (reduces clone overhead)

#### Problem

**Location**: `src/table.rs:346`

```rust
pub enum VectorSelectorSpec {
    None,
    All,
    Named(Vec<String>),  // ← Clones entire Vec<String> per query
}

fn execute(&self, ...) -> DataFusionResult<SendableRecordBatchStream> {
    let vector_selector = self.vector_selector.clone();  // ← Heap allocation
    // ...
}
```

**Impact**:
- Queries with 5+ named vectors clone entire Vec<String>
- Each String also cloned individually
- Typical overhead: 100-500 bytes per query

#### Solution

**Option 1: Arc wrapper** (recommended)
```rust
pub enum VectorSelectorSpec {
    None,
    All,
    Named(Arc<Vec<String>>),  // ← Shared ownership
}
```

**Option 2: SmallVec optimization** (for typical cases)
```rust
use smallvec::SmallVec;

pub enum VectorSelectorSpec {
    None,
    All,
    Named(SmallVec<[String; 4]>),  // ← Inline storage for ≤4 vectors
}
```

**Recommendation**: Use **Option 1** (Arc) for simplicity and consistent performance.

**Changes Required**:
- `src/utils.rs:12-19` - Enum definition
- `src/utils.rs:80` - build_vector_selector() construction
- `src/table.rs:346` - Clone site

**Estimated Effort**: 30 minutes

---

## High-Value Features

### 5. Specialized Vector Lookup

**Severity**: 🟢 **MEDIUM** (per-point allocation)

#### Problem

**Location**: `src/arrow/deserialize.rs:316-340`

```rust
fn build_vector_lookup(vectors: Option<VectorsOutput>) -> HashMap<String, Vector> {
    let mut lookup = HashMap::new();  // ← Fresh HashMap per point
    // ... populate lookup
    lookup
}

// Called from append_point:216
let vector_lookup = build_vector_lookup(vectors);  // ← Per-point allocation
```

**Impact**:
- HashMap allocated and deallocated for every point
- Typical size: 1-5 vectors per point
- Hash computation overhead for small maps

#### Solution Options

**Option A: Pre-allocate HashMap with capacity**
```rust
fn build_vector_lookup(vectors: Option<VectorsOutput>) -> HashMap<String, Vector> {
    let mut lookup = HashMap::with_capacity(5);  // ← Pre-allocate for typical case
    // ... populate
    lookup
}
```

**Option B: Specialized lookup for common cases**
```rust
enum VectorLookup {
    Unnamed(Option<Vector>),  // Single unnamed vector (most common)
    Named(HashMap<String, Vector>),  // Multiple named vectors
}

fn build_vector_lookup(vectors: Option<VectorsOutput>) -> VectorLookup {
    match vectors.and_then(|v| v.vectors_options) {
        Some(vectors_output::VectorsOptions::Vector(output)) => {
            // Unnamed vector case - no HashMap
            VectorLookup::Unnamed(Vector::from_vector_output(output))
        }
        Some(vectors_output::VectorsOptions::Vectors(named)) => {
            // Named vectors - use HashMap
            let mut lookup = HashMap::with_capacity(named.vectors.len());
            for (name, output) in named.vectors {
                if let Some(vec) = Vector::from_vector_output(output) {
                    lookup.insert(name, vec);
                }
            }
            VectorLookup::Named(lookup)
        }
        None => VectorLookup::Unnamed(None),
    }
}

impl VectorLookup {
    fn get(&self, name: &str) -> Option<&Vector> {
        match self {
            VectorLookup::Unnamed(vec) if name == "vector" => vec.as_ref(),
            VectorLookup::Named(map) => map.get(name),
            _ => None,
        }
    }
}
```

**Recommendation**: Use **Option B** for optimal performance (avoids HashMap for unnamed vectors).

**Changes Required**:
- `src/arrow/deserialize.rs:316-340` - Replace HashMap with VectorLookup enum
- `src/arrow/deserialize.rs:216` - Update call site
- `src/arrow/deserialize.rs:244-285` - Update .get() calls

**Estimated Effort**: 4 hours (implementation + testing)

---

### 6. Numeric ID Support

**Severity**: 🟢 **LOW** (type precision)

#### Problem

**Location**: `src/arrow/deserialize.rs:225`

```rust
Some(point_id::PointIdOptions::Num(n)) => {
    builder.append_value(n.to_string());  // ← Numeric ID converted to String
}
```

**Current schema**: `Field::new("id", DataType::Utf8, false)`

**Impact**:
- Numeric IDs lose native integer representation
- String conversion overhead per point
- Queries can't use numeric comparisons on ID
- Memory overhead: 8 bytes (u64) → ~20 bytes (String)

#### Solution

**Option A: Union type** (both numeric and UUID)
```rust
// Schema
Field::new(
    "id",
    DataType::Union(
        UnionFields::new(
            vec![0, 1],
            vec![
                Field::new("numeric", DataType::UInt64, false),
                Field::new("uuid", DataType::Utf8, false),
            ]
        ),
        UnionMode::Dense,
    ),
    false,
)

// Builder
match id.point_id_options {
    Some(point_id::PointIdOptions::Num(n)) => {
        union_builder.append("numeric", UInt64Array::from(vec![n]))?;
    }
    Some(point_id::PointIdOptions::Uuid(uuid)) => {
        union_builder.append("uuid", StringArray::from(vec![uuid]))?;
    }
    None => union_builder.append_null(),
}
```

**Option B: Configuration flag** (simpler)
```rust
pub struct QdrantTableProvider {
    // ...
    id_type: IdType,
}

pub enum IdType {
    String,   // Current behavior (compatible)
    Numeric,  // Use UInt64 for numeric IDs
    Auto,     // Detect from first point
}
```

**Recommendation**: Use **Option B** (configuration) to maintain backwards compatibility.

**Changes Required**:
- `src/arrow/schema.rs:62` - Conditional ID field type
- `src/arrow/deserialize.rs:124` - Conditional builder type
- `src/arrow/deserialize.rs:223-229` - Conditional append logic
- Add IdType configuration to TableProvider

**Estimated Effort**: 1 day (implementation + migration testing)

---

## Advanced Optimizations

### 7. Binary Payload Storage

**Severity**: ⚪ **LOW** (advanced use case)

#### Problem

**Location**: `src/arrow/deserialize.rs:237`

```rust
if let Some(payload) = &payload
    && let Ok(json) = serde_json::to_string(&payload)
{
    builder.append_value(json);  // ← JSON serialization per point
}
```

**Impact**:
- JSON serialization overhead per point
- String allocation per point (typical: 50-500 bytes)
- Limited query support (string operations only)

#### Solution

**Option A: Binary (MessagePack)**
```rust
Field::new("payload", DataType::Binary, true)

// Serialization
if let Some(payload) = &payload {
    let bytes = rmp_serde::to_vec(&payload)?;
    binary_builder.append_value(&bytes);
}
```

**Option B: Structured schema** (best for known payloads)
```rust
// If payload has known structure:
struct Payload {
    category: String,
    price: f64,
    tags: Vec<String>,
}

// Generate structured fields instead of JSON blob:
Field::new("category", DataType::Utf8, true),
Field::new("price", DataType::Float64, true),
Field::new("tags", DataType::List(...), true),
```

**Recommendation**: Use **Option B** when payload schema is known, **Option A** for dynamic payloads.

**Benefits**:
- **Memory**: More compact representation
- **Query**: Native operations on structured fields
- **Performance**: No JSON parsing in DataFusion

**Changes Required**:
- Major refactor of payload handling
- Schema generation becomes payload-aware
- Need payload schema definition system

**Estimated Effort**: 2-3 days (complex change)

---

## Implementation Roadmap

### Phase 1: Quick Wins (Week 1)

**Goal**: Low-hanging fruit optimizations with immediate impact

**Tasks**:
1. ✅ Arc\<String\> for collection name (30 min)
   - Files: `src/table.rs:214,177,344`
   - Testing: Existing tests should pass
   - Benchmark: Measure allocation reduction

2. ✅ VectorSelectorSpec Arc wrapper (30 min)
   - Files: `src/utils.rs:12-19,80`, `src/table.rs:346`
   - Testing: Existing tests should pass
   - Benchmark: Measure clone overhead reduction

3. ✅ HashMap capacity pre-allocation (15 min)
   - File: `src/arrow/deserialize.rs:317`
   - Change: `HashMap::new()` → `HashMap::with_capacity(5)`
   - Testing: Existing tests should pass

**Expected Impact**: 5-10% reduction in query execution overhead

---

### Phase 2: Pagination (Week 2)

**Goal**: Enable large result set handling

**Tasks**:
1. ✅ Design pagination API (4 hours)
   - Configuration mechanism (env var, builder method, or both)
   - Default batch size selection
   - Backward compatibility strategy

2. ✅ Implement scroll-based query (1 day)
   - File: `src/table.rs:269-320` - Refactor to stream
   - Use `futures::stream::try_unfold` for pagination
   - Handle offset/limit correctly

3. ✅ Update QdrantQueryStream (4 hours)
   - File: `src/stream.rs:13-23`
   - Support multiple batches instead of single batch
   - Proper error propagation across batches

4. ✅ E2E testing (4 hours)
   - Test with 50K+ point collections
   - Verify memory stays bounded
   - Benchmark latency to first batch
   - Test LIMIT clause interaction

**Expected Impact**:
- Memory usage: O(total_points) → O(batch_size)
- Time to first batch: ~50% reduction for large queries

---

### Phase 3: Filter Pushdown (Week 3-4)

**Goal**: Optimize selective queries

**Tasks**:
1. ✅ Design filter compiler (1 day)
   - Define supported expression types
   - Map DataFusion Expr → Qdrant Filter
   - Fallback strategy for unsupported filters

2. ✅ Implement basic filters (2 days)
   - New file: `src/filter.rs`
   - Support: `=`, `!=`, `<`, `>`, `<=`, `>=`
   - Support: `AND`, `OR`
   - Payload field extraction

3. ✅ Integration with query builder (4 hours)
   - File: `src/table.rs:285-299`
   - Compile filters before query
   - Apply compiled filter to QueryPointsBuilder

4. ✅ Comprehensive testing (1 day)
   - Unit tests for each operator
   - E2E tests with complex filters
   - Verify correctness against non-pushed filters
   - Benchmark performance gains

5. ✅ Advanced filters (optional, 1 day)
   - Support: `IN`, `NOT IN`
   - Support: `LIKE` (substring matching)
   - Support: `IS NULL`, `IS NOT NULL`

**Expected Impact**:
- Network: 10-100x reduction for selective queries
- Latency: Proportional to filtered result size

---

### Phase 4: Advanced Optimizations (Future)

**Goal**: Squeeze every last bit of performance

**Tasks**:
1. Specialized vector lookup (4 hours)
2. Numeric ID support (1 day)
3. Binary payload storage (2-3 days)
4. Connection pool tuning (1 day)
5. Observability/metrics (1 day)

**Expected Impact**: 5-15% additional performance improvement

---

## Benchmarking Strategy

### Baseline Metrics (Before Optimization)

**Test Collections**:
1. **Small**: 1K points, 384-dim vectors
2. **Medium**: 100K points, 768-dim vectors
3. **Large**: 1M points, 1536-dim vectors

**Query Patterns**:
1. **Full scan**: `SELECT * FROM collection`
2. **Projection**: `SELECT id, embedding FROM collection`
3. **Limit**: `SELECT * FROM collection LIMIT 100`
4. **Filter** (after pushdown): `SELECT * FROM collection WHERE payload->>'category' = 'X'`

**Metrics**:
- Query latency (p50, p95, p99)
- Memory usage (peak, average)
- Network bandwidth (bytes transferred)
- CPU usage (DataFusion vs network I/O)

### Target Improvements

| Optimization | Metric | Target Improvement |
|--------------|--------|-------------------|
| Pagination | Peak memory (large queries) | 90% reduction |
| Filter pushdown | Selective query latency | 10-100x faster |
| Arc\<String\> | Allocation count | 5-10% reduction |
| VectorSelectorSpec | Clone overhead | 2-5% reduction |
| HashMap optimization | Per-point allocation | 1-2% reduction |

---

## Migration Guide

### Breaking Changes

**None expected** - All optimizations are backward compatible.

**API Changes**:
1. Pagination: Opt-in via configuration (default: single-batch for compatibility)
2. Filter pushdown: Automatic (transparent to users)
3. Numeric ID: Opt-in via configuration

### Configuration

**Proposed API**:
```rust
// Builder pattern for advanced configuration
let table = QdrantTableProvider::builder()
    .client(client)
    .collection("my_collection")
    .batch_size(10_000)           // Enable pagination
    .id_type(IdType::Numeric)      // Use UInt64 for IDs
    .enable_filter_pushdown(true)  // Default: true
    .build()
    .await?;

// Simple API (backward compatible)
let table = QdrantTableProvider::try_new(client, "my_collection").await?;
```

---

## Testing Requirements

### Unit Tests

- Filter compiler (each operator)
- VectorLookup enum (unnamed/named cases)
- Pagination logic (offset/limit edge cases)

### Integration Tests

- E2E pagination (50K+ points)
- E2E filter pushdown (verify correctness)
- Memory usage tests (bounded allocation)
- Performance regression tests

### Benchmarks

- Criterion benchmarks for hot paths
- Memory profiling (heaptrack, valgrind)
- Network profiling (tcpdump analysis)

---

## Summary

### Immediate Actions (This Week)

1. **Arc\<String\> changes** (30 min) - Reduce hot path allocations
2. **VectorSelectorSpec Arc wrap** (30 min) - Reduce clone overhead
3. **HashMap capacity** (15 min) - Pre-allocate for typical case

**Total effort**: ~2 hours
**Expected impact**: 5-10% query overhead reduction

### Short-Term (Next 2 Weeks)

4. **Pagination** (2 days) - Critical for large result sets
5. **Filter pushdown** (2-3 days) - Major performance win

**Total effort**: 4-5 days
**Expected impact**:
- Memory: 90% reduction for large queries
- Latency: 10-100x for selective queries

### Long-Term (Next Quarter)

6. **Advanced optimizations** (1-2 weeks)
   - Specialized vector lookup
   - Numeric ID support
   - Observability/metrics

**Total effort**: 1-2 weeks
**Expected impact**: 5-15% additional improvement

---

## Conclusion

The qdrant-datafusion codebase has a **solid foundation**. The optimizations outlined here are **additive enhancements** that build on the existing clean architecture.

**Priority focus**: Pagination and filter pushdown provide the highest ROI for production workloads.

**Quick wins**: Arc-based sharing changes can be implemented immediately with minimal risk.

**Architecture**: No fundamental changes needed - the schema-driven design supports all proposed optimizations naturally.
