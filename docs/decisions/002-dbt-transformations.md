# ADR 002: DBT for Transformations and Three-Layer Architecture

## Status

Accepted

## Context

We need a transformation framework that:
- Enables SQL-based data transformations
- Supports testing and documentation
- Provides clear separation of concerns
- Scales from development to production
- Integrates with our orchestration layer (Dagster)

We also need to decide on a data architecture pattern that:
- Organizes transformations logically
- Enables reusability
- Provides quality gates
- Is maintainable and understandable

### Alternatives Considered

#### Transformation Frameworks

1. **DBT (Data Build Tool)**
   - SQL-based transformations
   - Built-in testing framework
   - Version control friendly
   - Strong community

2. **Python/Pandas**
   - Full programming language flexibility
   - Complex transformations possible
   - Harder to maintain
   - No built-in testing

3. **SQL Scripts**
   - Simple and direct
   - No framework overhead
   - No testing or documentation
   - Hard to maintain

#### Architecture Patterns

1. **Three-Layer (Medallion)**
   - Staging → Intermediate → Marts
   - Clear separation of concerns
   - Industry standard

2. **Two-Layer**
   - Raw → Marts
   - Simpler structure
   - Less reusability

3. **Single Layer**
   - All transformations in one step
   - Simplest approach
   - Hard to maintain

## Decision

We will use **DBT** for SQL transformations with a **three-layer architecture** (staging → intermediate → marts).

## Rationale

### Why DBT?

#### 1. SQL-Based Transformations


DBT uses SQL, which is:
- Familiar to data analysts and engineers
- Declarative and easy to understand
- Optimized by database query planners
- Version control friendly

```sql
-- Clear, readable transformations
SELECT
    CAST(TRIM(customer_id) AS INTEGER) as customer_id,
    LOWER(TRIM(name)) as name
FROM {{ ref('customers_raw') }}
```

#### 2. Built-in Testing Framework

```yaml
models:
  - name: stg_customers__cleaned
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
```

Benefits:
- Data quality tests as code
- Automatic test execution
- Clear test results
- No custom test framework needed

#### 3. Documentation as Code

```yaml
models:
  - name: stg_customers__cleaned
    description: "Cleaned and normalized customer data"
    columns:
      - name: customer_id
        description: "Unique customer identifier"
```

Benefits:
- Documentation lives with code
- Auto-generated documentation site
- Always up-to-date
- Searchable and browsable

#### 4. Dependency Management

```sql
-- DBT automatically tracks dependencies
SELECT *
FROM {{ ref('stg_customers__cleaned') }}
JOIN {{ ref('stg_accounts__cleaned') }}
  ON customer_id
```

Benefits:
- Automatic dependency resolution
- Correct execution order
- Lineage visualization
- Refactoring safety

#### 5. Environment Management

```yaml
# profiles.yml
dev:
  target: duckdb
  outputs:
    duckdb:
      type: duckdb
      path: data/duckdb/lending_club.duckdb

prod:
  target: databricks
  outputs:
    databricks:
      type: databricks
      # ... connection details
```

Benefits:
- Same code, different environments
- Easy local development
- Production deployment without changes

### Why Three-Layer Architecture?

#### Layer 1: Staging

**Purpose**: Clean and normalize raw data

**Characteristics**:
- One model per source
- Light transformations only
- Materialized as views
- Foundation for all downstream models

**Example**:
```sql
-- stg_customers__cleaned.sql
SELECT
    CAST(TRIM(customer_id) AS INTEGER) as customer_id,
    LOWER(TRIM(name)) as name,
    CASE
        WHEN LOWER(has_loan) = 'yes' THEN 'Yes'
        WHEN LOWER(has_loan) = 'no' THEN 'No'
        ELSE 'None'
    END as has_loan
FROM {{ source('raw', 'customers') }}
```

**Benefits**:
- Single source of truth for each source
- Consistent naming and types
- Quality gates before business logic
- Easy to debug data issues

#### Layer 2: Intermediate

**Purpose**: Reusable business logic components

**Characteristics**:
- Purpose-specific transformations
- Joins, filters, aggregations
- Materialized as ephemeral (not stored)
- Reusable across multiple marts

**Example**:
```sql
-- int_accounts__with_customer.sql
SELECT
    a.account_id,
    a.customer_id,
    a.balance,
    a.account_type,
    c.has_loan
FROM {{ ref('stg_accounts__cleaned') }} a
INNER JOIN {{ ref('stg_customers__cleaned') }} c
    ON a.customer_id = c.customer_id
```

**Benefits**:
- DRY principle (Don't Repeat Yourself)
- Multiple marts can reference same intermediate
- Business logic isolated from presentation
- Easier to test and maintain

#### Layer 3: Marts

**Purpose**: Business-ready analytical outputs

**Characteristics**:
- Final outputs for consumption
- Complex business logic
- Materialized as tables
- Optimized for query performance

**Example**:
```sql
-- account_summary.sql
SELECT
    customer_id,
    account_id,
    balance as original_balance,
    CASE
        WHEN balance < 10000 THEN 0.01
        WHEN balance < 20000 THEN 0.015
        ELSE 0.02
    END + CASE WHEN has_loan = 'Yes' THEN 0.005 ELSE 0 END as interest_rate,
    -- ... more calculations
FROM {{ ref('int_savings_accounts_only') }}
```

**Benefits**:
- Business logic clearly separated
- Optimized for end-user queries
- Easy to understand business rules
- Performance through materialization

### Architecture Comparison

| Aspect | Three-Layer | Two-Layer | Single-Layer |
|--------|-------------|-----------|--------------|
| Separation of Concerns | ✅ Excellent | ⚠️ Moderate | ❌ Poor |
| Reusability | ✅ High | ⚠️ Medium | ❌ Low |
| Maintainability | ✅ Easy | ⚠️ Moderate | ❌ Hard |
| Debugging | ✅ Easy | ⚠️ Moderate | ❌ Hard |
| Complexity | ⚠️ More models | ✅ Fewer models | ✅ Simplest |
| Performance | ✅ Optimized | ✅ Good | ⚠️ Variable |

### Materialization Strategy

```yaml
# dbt_project.yml
models:
  staging:
    +materialized: view      # No storage, always fresh
  intermediate:
    +materialized: ephemeral # Computed on-the-fly
  marts:
    +materialized: table     # Persistent, fast queries
```

**Rationale**:
- **Views** for staging: No storage overhead, always current
- **Ephemeral** for intermediate: No unnecessary storage
- **Tables** for marts: Fast query performance for end users

## Consequences

### Positive

1. **Maintainability**: Clear structure, easy to navigate
2. **Testability**: Built-in testing framework
3. **Documentation**: Auto-generated, always current
4. **Reusability**: Intermediate models shared across marts
5. **Quality**: Quality gates at each layer
6. **Collaboration**: SQL accessible to analysts
7. **Version Control**: All transformations in Git
8. **Lineage**: Automatic dependency tracking

### Negative

1. **Learning Curve**: Team needs to learn DBT
2. **More Models**: Three layers means more files
3. **Complexity**: More structure to understand
4. **Overhead**: DBT framework adds some overhead

### Neutral

1. **SQL Limitation**: Complex logic may need Python
2. **Testing**: Need to write comprehensive tests
3. **Documentation**: Need to maintain documentation

## Implementation

### Project Structure

```
dbt_project/
├── models/
│   ├── staging/
│   │   ├── _staging.yml
│   │   ├── stg_customers__cleaned.sql
│   │   └── stg_accounts__cleaned.sql
│   ├── intermediate/
│   │   ├── _intermediate.yml
│   │   ├── int_accounts__with_customer.sql
│   │   └── int_savings_accounts_only.sql
│   └── marts/
│       ├── _marts.yml
│       └── account_summary.sql
├── tests/
│   └── test_interest_calculation.sql
└── dbt_project.yml
```

### Naming Conventions

- **Staging**: `stg_<source>__<entity>`
  - Example: `stg_customers__cleaned`
- **Intermediate**: `int_<entity>__<description>`
  - Example: `int_accounts__with_customer`
- **Marts**: `<business_concept>`
  - Example: `account_summary`

### Testing Strategy

1. **Staging Layer**:
   - Unique tests on primary keys
   - Not null tests on required fields
   - Accepted values for categorical columns

2. **Intermediate Layer**:
   - Relationship tests for joins
   - Row count validations

3. **Marts Layer**:
   - Business logic tests
   - Custom data tests
   - Calculation validations

## Validation

Success criteria:
- ✅ All models execute successfully
- ✅ All tests pass
- ✅ Lineage graph shows clear flow
- ✅ Documentation generated
- ✅ Performance meets requirements

## Trade-offs Accepted

1. **More Models vs. Maintainability**: Accept more files for better organization
2. **Learning Curve vs. Long-term Benefits**: Invest in learning for better outcomes
3. **Framework Overhead vs. Features**: Accept DBT overhead for testing and docs
4. **SQL Limitation vs. Accessibility**: Accept SQL limitations for analyst accessibility

## Alternatives Rejected

### Python/Pandas Transformations

**Rejected because**:
- Harder to maintain
- No built-in testing
- Less accessible to analysts
- More complex debugging

**When to use**: Complex transformations that can't be done in SQL

### Two-Layer Architecture

**Rejected because**:
- Less reusability
- Business logic mixed with cleaning
- Harder to maintain as complexity grows

**When to use**: Very simple pipelines with few transformations

### Single-Layer Architecture

**Rejected because**:
- Poor separation of concerns
- Hard to debug
- Not maintainable at scale

**When to use**: One-off scripts, not production pipelines

## Future Considerations

1. **Incremental Models**: For large datasets, use incremental materialization
2. **Snapshots**: For slowly changing dimensions
3. **Macros**: For reusable SQL logic
4. **Packages**: Leverage DBT packages for common patterns
5. **Exposures**: Document downstream consumers

## References

- [DBT Documentation](https://docs.getdbt.com)
- [DBT Best Practices](https://docs.getdbt.com/guides/best-practices)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [DBT Style Guide](https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md)

## Notes

- Decision made: 2024-10-15
- Last updated: 2024-10-30
- Reviewers: Data Engineering Team
- Status: Implemented and validated
- Related ADRs: ADR-001 (Dagster Orchestration)
