# Indexing

## Overview

NucleoDB provides a pluggable indexing system that supports two index types: **TrieIndex** for string-based searches (prefix, suffix, contains) and **TreeIndex** for range-based queries (greater than, less than, equals). Indexes are declared via the `@Index` annotation and automatically maintained as data changes.

## Index Architecture

```
┌─────────────────────────────────────────────────┐
│                 DataTable<T>                      │
│                                                   │
│  indexes: Map<String, IndexWrapper<T>>            │
│           │                                       │
│           ├─ "name"    → TrieIndex<T>            │
│           ├─ "age"     → TreeIndex<T>            │
│           └─ "email"   → TrieIndex<T>            │
│                                                   │
│  keyToEntry: Map<String, T>  ← "id" pseudo-index │
└─────────────────────────────────────────────────┘
```

## IndexWrapper (Abstract Base)

**Source:** `database/index/IndexWrapper.java`

The abstract base class for all index implementations.

### Abstract Methods

| Method | Description |
|--------|-------------|
| `add(T obj)` | Add an entry to the index |
| `delete(T obj)` | Remove an entry from the index |
| `modify(T obj)` | Update an entry in the index (typically delete + add) |
| `get(Object search)` | Exact match lookup |
| `contains(Object search)` | Partial/contains match |
| `startsWith(String str)` | Prefix match (TrieIndex) |
| `endsWith(String str)` | Suffix match (TrieIndex) |
| `greaterThan(Object obj)` | Range: `>` (TreeIndex) |
| `greaterThanEqual(Object obj)` | Range: `>=` (TreeIndex) |
| `lessThan(Object obj)` | Range: `<` (TreeIndex) |
| `lessThanEqual(Object obj)` | Range: `<=` (TreeIndex) |

### Index Value Resolution

IndexWrapper uses reflection to extract the indexed field value from a DataEntry:

```java
// Resolves "data.name" → entry.getData().getName()
List<Object> getIndexValue(T obj)
```

The index key path (e.g., `"name"`) is automatically prefixed with `"data."` to navigate through the DataEntry wrapper to the actual data object. Nested paths are supported (e.g., `"address.city"` → `entry.getData().getAddress().getCity()`).

## TrieIndex

**Source:** `database/index/TrieIndex.java`

A character-level trie data structure optimized for string search operations.

### Internal Structure

```
┌──────────────────────────────────────────┐
│               TrieIndex<T>               │
│                                          │
│  root: Node (root of the trie)           │
│  rootPartialNodes: Root[256]             │
│    (indexed by first character for       │
│     fast partial search entry points)    │
│  entries: TreeMap<String, Entry>         │
│    (key → Entry mapping for deletions)   │
└──────────────────────────────────────────┘
```

### Trie Node Structure

```
                    root (char=0)
                   / │ \
                 'j' 'a' 'b'
                 /    |    \
               'o'   'l'   'o'
               /      |      \
             'h'     'i'     'b'
             /        |
           'n'       'c'
            |         |
         [Entry]    'e'
         "john"      |
                   [Entry]
                   "alice"
```

Each `Node` contains:
- `character`: The character this node represents
- `nodes`: `TreeMap<Integer, Node>` — child nodes
- `entries`: Set of `Entry` objects at this exact string
- `partialEntries`: Set of `Entry` objects at this node and all descendants
- `parent`: Reference to parent node (for deletion traversal)

### Operations

#### Insert

```java
public void insert(Entry entry, String val) {
    val = val.toLowerCase();    // Case-insensitive
    Node tmp = this.root;
    for each character in val:
        if child node doesn't exist:
            create new Node
            set parent reference
            add to rootPartialNodes[character]  // For partial search
        tmp = child node
        tmp.partialEntries.add(entry)    // Every node on the path
    entry.lastNodes.add(tmp)             // Track leaf for deletion
    tmp.entries.add(entry)               // Exact match at leaf
}
```

#### Search (Exact Match)

```java
public Set<T> get(Object search) {
    // Traverse trie from root following each character
    // Return entries at the final node (exact matches)
    return searchData((String) search);
}
```

#### Contains (Partial Match)

```java
public Set<T> contains(Object searchStr) {
    // Use rootPartialNodes[firstChar] to find all nodes starting with the first char
    // Then traverse from each root matching the remaining characters
    // Return partialEntries at the final node
    return partialData((String) searchStr);
}
```

The `rootPartialNodes` array provides O(1) lookup for the first character of any search, then efficiently traverses all matching subtrees.

#### StartsWith

```java
public Set<T> startsWith(String findString) {
    // Traverse from root following each character
    // Return partialEntries at the final node (all entries with this prefix)
}
```

#### EndsWith

```java
public Set<T> endsWith(String findString) {
    // Similar to contains, but returns entries (leaf nodes only)
    // instead of partialEntries
}
```

#### Delete

```java
public void delete(Entry entry) {
    Stack<Node> lastNodes = entry.getLastNodes();
    for each leaf node:
        traverse upward from leaf to root:
            remove entry from entries and partialEntries
            if node is empty, remove from parent's children
    entries.remove(key);
}
```

### Complexity

| Operation | Time Complexity | Description |
|-----------|----------------|-------------|
| Insert | O(L) | L = string length |
| Exact search | O(L) | L = search string length |
| Contains | O(L + R) | L = string length, R = number of root matches |
| StartsWith | O(L) | L = prefix length |
| EndsWith | O(L + R) | L = suffix length, R = root matches |
| Delete | O(L * D) | L = string length, D = depth traversal |

## TreeIndex

**Source:** `database/index/TreeIndex.java`

A `TreeMap`-based index that supports exact match and range queries. Best suited for numeric fields and comparable types.

### Internal Structure

```
┌────────────────────────────────────────────┐
│              TreeIndex<T>                   │
│                                             │
│  index: TreeMap<Object, Set<T>>            │
│    (value → set of entries with that value) │
│                                             │
│  reverseMap: TreeMap<T, List<Object>>       │
│    (entry → list of indexed values)         │
│    (used for efficient deletion)            │
└────────────────────────────────────────────┘
```

### Operations

#### Add

```java
public void add(T dataEntry) {
    List<Object> values = getIndexValue(dataEntry);
    for each value:
        reverseMap[dataEntry].add(value)     // Track for deletion
        index[value].add(dataEntry)           // Primary index
}
```

#### Delete

```java
public void delete(T dataEntry) {
    List<Object> indexedValues = reverseMap.get(dataEntry);
    for each value:
        index[value].remove(dataEntry)
        if index[value] is empty:
            index.remove(value)
    reverseMap.remove(dataEntry);
}
```

#### Modify

```java
public void modify(T dataEntry) {
    delete(dataEntry);
    add(dataEntry);
}
```

#### Exact Match

```java
public Set<T> get(Object search) {
    // Type coercion: handles Float/Double/Long/Integer mismatches
    return index.get(search);
}
```

The `get` method includes type coercion logic to handle cases where the search value type differs from the stored type (e.g., searching with a `Long` in a `Float` index).

#### Range Queries

```java
// Uses TreeMap's sorted map views
public Set<T> lessThan(Object obj)         { return reduce(index.headMap(obj)); }
public Set<T> lessThanEqual(Object obj)    { return reduce(index.headMap(obj, true)); }
public Set<T> greaterThan(Object obj)      { return reduce(index.tailMap(obj)); }
public Set<T> greaterThanEqual(Object obj) { return reduce(index.tailMap(obj, true)); }
```

The `reduce` method flattens a `SortedMap<Object, Set<T>>` into a single `Set<T>`.

#### Contains

```java
public Set<T> contains(Object searchObj) {
    // Iterate all keys, filter by String.contains() or Object.equals()
    return index.keySet().stream()
        .filter(key -> key contains searchObj)
        .flatMap(key -> index.get(key))
        .collect(toSet());
}
```

### Complexity

| Operation | Time Complexity | Description |
|-----------|----------------|-------------|
| Add | O(V * log N) | V = values, N = index size |
| Delete | O(V * log N) | V = values, N = index size |
| Exact match | O(log N) | TreeMap lookup |
| Range query | O(log N + K) | K = results in range |
| Contains | O(N) | Full scan of keys |

## Index Configuration

### Via @Index Annotation

```java
@Table(tableName = "author", dataEntryClass = AuthorDE.class)
public class Author implements Serializable {

    @Index                          // Default: TrieIndex, key = "name"
    String name;

    @Index(type = TreeIndex.class)  // Explicit TreeIndex for range queries
    int age;

    @Index(value = "custom.path")   // Custom index key path
    String email;
}
```

### Via DataTableConfig.IndexConfig

```java
// Programmatic index configuration
Set<IndexConfig> indexes = new TreeSet<>();
indexes.add(new IndexConfig("name", TrieIndex.class));
indexes.add(new IndexConfig("age", TreeIndex.class));
```

### Index Discovery at Startup

```java
// In NucleoDB.processIndexListForClass()
getAllAnnotatedFields(clazz, Index.class, "").forEach(field -> {
    if (field.getAnnotation().value().isEmpty()) {
        // Use field path as index key
        indexes.add(new IndexConfig(field.getPath(), field.getAnnotation().type()));
    } else {
        // Use custom value as index key
        indexes.add(new IndexConfig(field.getAnnotation().value(), field.getAnnotation().type()));
    }
});
```

## Index Lifecycle

### Initialization (Table Startup)

```
1. DataTableConfig contains IndexConfig set
2. DataTable constructor creates IndexWrapper instances:
   for each IndexConfig:
       indexType.getDeclaredConstructor(String.class).newInstance(name)
       indexes.put(indexedKey, indexWrapper)
3. If loading saved data:
   for each entry in loaded entries:
       for each index:
           index.add(entry)
```

### Runtime Updates

```
On CREATE:
  for each index:
      index.add(newEntry)

On UPDATE:
  for each JSON Patch operation:
      if operation.path matches an indexed field:
          if op is "replace"/"add"/"copy":
              index.modify(entry)     // delete + add
          if op is "remove":
              index.delete(entry)

On DELETE:
  for each index:
      index.delete(entry)
```

## Primary Key ("id") Pseudo-Index

The `keyToEntry` map serves as a built-in primary key index:

```java
public Set<T> get(String key, Object value) {
    if (key.equals("id")) {
        T entry = this.keyToEntry.get(value);
        if (entry != null) {
            return new TreeSet(Arrays.asList(entry));
        }
        return new TreeSet<>();
    }
    // Fall through to regular index
    IndexWrapper<T> indexWrapper = this.indexes.get(key);
    return indexWrapper.get(value);
}
```

This provides O(1) lookup by UUID without requiring an explicit index declaration.

## Choosing the Right Index

| Use Case | Index Type | Why |
|----------|-----------|-----|
| Text search / partial match | `TrieIndex` | Efficient prefix/contains traversal |
| Exact string equality | `TrieIndex` | O(L) lookup by string length |
| Numeric equality | `TreeIndex` | O(log N) TreeMap lookup |
| Range queries (>, <, >=, <=) | `TreeIndex` | Uses TreeMap's sorted views |
| Enum / category fields | `TreeIndex` | Small cardinality, exact match |
| UUID / key lookup | Built-in `keyToEntry` | O(1) HashMap lookup |
