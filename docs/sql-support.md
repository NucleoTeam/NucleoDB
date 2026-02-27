# SQL Support

## Overview

NucleoDB provides a SQL query interface via the `SQLHandler` class, which uses the JSQLParser library (v4.6) to parse SQL strings and translate them into NucleoDB operations. The SQL interface supports SELECT, INSERT, UPDATE, and DELETE statements.

## Entry Point

**Source:** `NucleoDB.java`

The `NucleoDB` class exposes SQL methods:

```java
// Generic SQL execution (returns Object — type depends on statement)
public Object sql(String sqlStr)
public Object sql(String sqlStr, Class clazz)

// Typed SELECT
public <T> List<T> select(String sqlStr, Class clazz)

// INSERT
public DataEntry insert(String sqlStr)

// UPDATE
public boolean update(String sqlStr)
```

### Statement Routing

```java
Statement sqlStatement = CCJSqlParserUtil.parse(sqlStr);

if (sqlStatement instanceof Select)  → SQLHandler.handleSelect()
if (sqlStatement instanceof Insert)  → SQLHandler.handleInsert()
if (sqlStatement instanceof Update)  → SQLHandler.handleUpdate()
if (sqlStatement instanceof Delete)  → SQLHandler.handleDelete()
```

## SELECT

**Source:** `SQLHandler.handleSelect()`

### Supported Features

| Feature | Supported | Example |
|---------|-----------|---------|
| WHERE clause | Yes | `WHERE name = 'John'` |
| AND / OR | Yes | `WHERE name = 'John' AND age = 30` |
| Parentheses | Yes | `WHERE (a = 1 OR b = 2) AND c = 3` |
| Equality (=) | Yes | `WHERE name = 'John'` |
| Not equal (!=) | Yes | `WHERE name != 'John'` |
| IN | Yes | `WHERE id IN ('a', 'b', 'c')` |
| LIKE | Yes | `WHERE name LIKE 'joh'` (maps to search/contains) |
| ORDER BY | Yes | `ORDER BY name ASC, age DESC` |
| LIMIT | Yes | `LIMIT 10` |
| OFFSET | Yes | `LIMIT 10 OFFSET 20` |

### Query Processing Flow

```
SQL String: "SELECT * FROM author WHERE name = 'John' ORDER BY age LIMIT 10"
    │
    ▼
┌─────────────────────────────────────────────┐
│  1. Parse with CCJSqlParserUtil.parse()     │
│  2. Extract table name: "author"            │
│  3. Look up DataTable: nucleoDB.getTable()  │
│  4. Evaluate WHERE clause:                  │
│     → evaluateWhere(expression, table)      │
│  5. Apply ORDER BY:                         │
│     → createSorting(orderByElements)        │
│  6. Apply LIMIT/OFFSET:                     │
│     → subList(offset, offset+count)         │
│  7. Map DataEntry → target class            │
│     → JSON serialize/deserialize            │
│  8. Set @PrimaryKey fields to entry.key     │
│  9. Return List<T>                          │
└─────────────────────────────────────────────┘
```

### WHERE Clause Evaluation

```
evaluateWhere(expression, table)
    │
    ├── AndExpression
    │   left = evaluateWhere(leftExpr)
    │   right = evaluateWhere(rightExpr)
    │   return left ∩ right  (retainAll)
    │
    ├── OrExpression
    │   left = evaluateWhere(leftExpr)
    │   right = evaluateWhere(rightExpr)
    │   return left ∪ right  (addAll)
    │
    ├── InExpression
    │   column = left expression column name
    │   values = right expression list
    │   return table.in(column, values)
    │
    ├── LikeExpression
    │   column = left expression
    │   pattern = right expression
    │   return table.search(column, pattern)
    │
    ├── Parenthesis
    │   return evaluateWhere(inner expression)
    │
    ├── EqualsTo
    │   column = left expression
    │   value = right expression (String/Double/Long)
    │   return table.get(column, value)
    │
    └── NotEqualsTo
        column = left expression
        value = right expression
        return table.getNotEqual(column, value)
```

### ORDER BY Processing

```java
public static Comparator createSorting(List<OrderByElement> orderByElements) {
    for each orderByElement:
        column = "data." + columnName    // Navigate through DataEntry.data
        function = createComparatorFunction(column)

        if first element:
            comparator = new NullComparator(function)
        else:
            comparator = comparator.thenComparing(new NullComparator(function))

        if DESC:
            comparator = comparator.reversed()
}
```

The `NullComparator` handles null values gracefully and supports comparison of String, Integer, Long, Double, Float, and arbitrary objects (via JSON serialization).

### Column Value Resolution

The `createComparatorFunction()` creates a function that navigates nested object properties:

```java
// For column "data.address.city":
//   1. Split by "." → ["data", "address", "city"]
//   2. For each segment:
//      - Use PropertyDescriptor to get the getter
//      - Invoke the getter on the current object
//   3. Support "i0", "i1" syntax for list indexing
//      e.g., "data.tags.i0" → entry.getData().getTags().get(0)
```

Results are cached in a static `cache` map for performance.

### LIMIT and OFFSET

```java
int offset = 0;
int count = 25;  // Default limit

if (plainSelect.getLimit() != null) {
    offset = plainSelect.getLimit().getOffset();
    count = plainSelect.getLimit().getRowCount();
}

sortedEntries = sortedEntries.subList(
    Math.min(offset, size),
    Math.min(offset + count, size)
);
```

Default limit is 25 rows if no LIMIT clause is specified.

### Result Mapping

Results are mapped from `DataEntry` to the target class:

```java
sortedEntries.stream().map(f -> {
    // 1. Serialize entry.getData() to JSON
    // 2. Deserialize to target class
    Object o = Serializer.readValue(
        Serializer.writeValueAsString(f.getData()),
        clazz
    );

    // 3. Set @PrimaryKey annotated fields to entry.getKey()
    keyFields.forEach(pd -> pd.getWriteMethod().invoke(o, f.getKey()));

    return o;
}).collect(toList());
```

The `@PrimaryKey` annotation (from `database/utils/sql/PrimaryKey.java`) marks fields in the result class that should receive the DataEntry UUID.

## INSERT

**Source:** `SQLHandler.handleInsert()`

### Flow

```
SQL: "INSERT INTO author SET name = 'John', age = 30"
    │
    ▼
┌─────────────────────────────────────────┐
│  1. Parse table name: "author"          │
│  2. Get DataTable and data class        │
│  3. Instantiate empty data object       │
│  4. For each SET column:                │
│     → setColumnVal(expression, column,  │
│                     obj, field)          │
│  5. Save to table (via saveSync)        │
│  6. Return new DataEntry                │
└─────────────────────────────────────────┘
```

### Column Value Setting

`setColumnVal()` handles various expression types:

| Expression Type | Java Action |
|----------------|-------------|
| `StringValue` | `PropertyDescriptor.write(obj, value)` |
| `LongValue` | Auto-converts to Integer or Long based on field type |
| `DoubleValue` | Auto-converts to Float or Double based on field type |
| `RowConstructor` | Handles nested objects and list initialization |
| `ValueListExpression` | Processes list of values |
| `EqualsTo` | Recursive column=value within nested structures |
| `Parenthesis` | Unwrap and recurse |

### Nested Object Support

The INSERT handler supports creating nested objects:

```sql
INSERT INTO author SET
    name = 'John',
    address = (street = '123 Main', city = 'Springfield')
```

This creates the `address` sub-object with its fields populated via recursive `setColumnVal()` calls.

### List Support

```sql
INSERT INTO author SET
    name = 'John',
    tags = ('fiction', 'drama')
```

RowConstructor expressions create and populate list fields, handling type detection via `ParameterizedType`.

## UPDATE

**Source:** `SQLHandler.handleUpdate()`

### Flow

```
SQL: "UPDATE author SET name = 'Jane' WHERE name = 'John'"
    │
    ▼
┌──────────────────────────────────────────┐
│  1. Parse table name: "author"           │
│  2. Get DataTable                        │
│  3. Evaluate WHERE → Set<DataEntry>      │
│  4. For each UpdateSet:                  │
│     → Extract columns and expressions    │
│     → applyChangesToEntries()            │
│  5. For each modified entry:             │
│     → entry.versionIncrease()            │
│     → table.saveAsync(entry, callback)   │
│     → CountDownLatch.await()             │
│  6. Return true/false                    │
└──────────────────────────────────────────┘
```

### Applying Changes

`applyChangesToEntries()` navigates nested object paths and applies the new value:

```java
// For column "address.city":
// 1. Split by "." → ["address", "city"]
// 2. Navigate: entry.getData() → .getAddress()
// 3. Set: .setCity(newValue)

// Special support:
// - "i0", "i1" for list indexing
// - "new" for creating new list elements
```

### Synchronous Saves

Each updated entry is saved through the standard event-sourcing pipeline:

```java
for (DataEntry dataEntryUnsaved : dataEntries) {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    dataEntryUnsaved.versionIncrease();
    table.saveAsync(dataEntryUnsaved, dataEntryNew -> {
        countDownLatch.countDown();
    });
    countDownLatch.await();  // Wait for Kafka round-trip
}
```

## DELETE

**Source:** `SQLHandler.handleDelete()`

### Flow

```
SQL: "DELETE FROM author WHERE name = 'John' ORDER BY age LIMIT 5"
    │
    ▼
┌────────────────────────────────────────────┐
│  1. Parse table name: "author"             │
│  2. Get DataTable                          │
│  3. Evaluate WHERE → Set<DataEntry>        │
│  4. Apply ORDER BY (if present)            │
│  5. Apply LIMIT/OFFSET (if present)        │
│  6. For each entry (in parallel threads):  │
│     → table.deleteAsync(entry, callback)   │
│  7. CountDownLatch.await() for all         │
│  8. Return true/false                      │
└────────────────────────────────────────────┘
```

### Parallel Deletion

Deletes are executed in parallel, each on a new thread:

```java
CountDownLatch countDownLatch = new CountDownLatch(dataEntries.size());
dataEntries.forEach(x -> {
    new Thread(() -> {
        if (table.getEntries().contains(x)) {
            table.deleteAsync(x, (d) -> countDownLatch.countDown());
        } else {
            countDownLatch.countDown();
        }
    }).start();
});
countDownLatch.await();
```

## Expression Type Mapping

| SQL Type | JSQLParser Class | Java Type |
|----------|-----------------|-----------|
| `'string'` | `StringValue` | `String` |
| `123` | `LongValue` | `Long` / `Integer` |
| `12.5` | `DoubleValue` | `Double` / `Float` |
| `(a, b, c)` | `RowConstructor` | Nested object / List |
| `(1, 2, 3)` | `ValueListExpression` | List of values |

## Usage Examples

### SELECT

```java
// Simple query
List<Author> authors = nucleoDB.select(
    "SELECT * FROM author WHERE name = 'John'",
    Author.class
);

// With ordering and pagination
List<Author> authors = nucleoDB.select(
    "SELECT * FROM author WHERE age > 20 ORDER BY name ASC LIMIT 10 OFFSET 0",
    Author.class
);

// IN clause
List<Author> authors = nucleoDB.select(
    "SELECT * FROM author WHERE name IN ('John', 'Jane', 'Bob')",
    Author.class
);
```

### INSERT

```java
DataEntry entry = nucleoDB.insert(
    "INSERT INTO author SET name = 'John', age = 30"
);
```

### UPDATE

```java
boolean success = nucleoDB.update(
    "UPDATE author SET name = 'Jane' WHERE name = 'John'"
);
```

### DELETE

```java
nucleoDB.sql("DELETE FROM author WHERE name = 'John'");
```

## Limitations

1. **No JOIN support** — Each query operates on a single table
2. **No aggregate functions** — No COUNT, SUM, AVG, etc.
3. **No subqueries** — Nested SELECT is not supported
4. **LIKE maps to contains** — Not standard SQL LIKE with wildcards
5. **Limited type coercion** — Some edge cases in numeric type handling
6. **Default limit of 25** — SELECT without LIMIT returns max 25 rows
7. **INSERT uses SET syntax** — Not standard `INSERT INTO ... VALUES` syntax
8. **No comparison operators (>, <)** in WHERE — Only =, !=, IN, LIKE are supported in the SQL evaluator (range queries available via programmatic API)
