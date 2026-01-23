# PR #121 - Unresolved Review Comments Report

**Generated:** 2026-01-21
**PR:** [#121 - Batch Operations](https://github.com/nstoredev/NStore/pull/121)
**Total Unresolved Comments:** 8

---

## Summary by Category

| Category | Count |
|----------|-------|
| Foreach Loop Filtering | 5 |
| Code Style | 2 |
| Security | 1 |
| **Total** | **8** |

---

## 1. Foreach Loop Filtering

These foreach loops implicitly filter their target sequence - consider filtering the sequence explicitly using `.Where(...)`.

### 1.1 InMemoryPersistence.cs - Line 112
**File:** `src/NStore.Core/InMemory/InMemoryPersistence.cs`

### 1.2 InMemoryPersistence.cs - Line 253
**File:** `src/NStore.Core/InMemory/InMemoryPersistence.cs`

**Suggested fix:**
```csharp
var filteredChunks = recorder.Chunks.Where(chunk => seen.Add(chunk.Index));

foreach (var chunk in filteredChunks)
{
    cancellationToken.ThrowIfCancellationRequested();
    yield return Clone((MemoryChunk)chunk);
}
```

### 1.3 InMemoryPersistence.cs - Line 254
**File:** `src/NStore.Core/InMemory/InMemoryPersistence.cs`

### 1.4 BatchRepository.cs - Line 172
**File:** `src/NStore.Domain/BatchRepository.cs`

### 1.5 MongoPersistence.cs - Line 1046
**File:** `src/NStore.Persistence.Mongo/MongoPersistence.cs`

---

## 2. Code Style Issues

### 2.1 BatchRepository.cs - Line 171
**Issue:** Multiple `if` statements can be combined.

### 2.2 DelegateToMethodProcessor.cs - Line 71
**Issue:** Both branches of this `if` statement write to the same variable - consider using `?` (ternary operator) to express intent better.

---

## 3. Security Issues

### 3.1 newbuild.ps1 - Line 10
**Issue:** Supply-chain risk

**Details:** The use of `Install-package BuildUtils` without a pinned version or integrity verification introduces a supply-chain risk: each build may download and execute an arbitrary version of the `BuildUtils` package from the configured feed, which could be compromised to run malicious code with access to your build environment and secrets (e.g., `nugetApiKey`).

**Recommendation:** Pin `BuildUtils` to a specific trusted version (and/or vendor it locally) and add integrity verification so that builds cannot silently move to a tampered package release.

---

## Resolved Items

### Test Files
- [x] **PersistenceFixture.cs - Line 1692**: Foreach loop filtering - refactored to use `.Where()`
- [x] **DefaultSnapshotBatchStoreTests.cs**: CancellationTokenSource now properly disposed (already had `using`)
- [x] **BatchRepositoryTests.cs (2 occurrences)**: CancellationTokenSource already properly disposed with `using var`
- [x] **DefaultSnapshotBatchStoreTests.cs (3 occurrences)**: ContainsKey + indexer replaced with `TryGetValue`
- [x] **BatchRepositoryTests.cs - Line 769**: ContainsKey + indexer replaced with `TryGetValue`
- [x] **PersistenceFixture.cs - Line 1686**: Foreach loop now uses `.Where()` filtering
- [x] **DefaultSnapshotBatchStoreTests.cs - Variable `ex`**: Removed useless assignment
- [x] **BatchRepositoryTests.cs - `_snapshotStore` fields**: Already marked as `readonly`
- [x] **PersistenceBatchAppendDecoratorTests.cs - Line 726**: Refactored foreach to use `SelectMany`
- [x] **PersistenceBatchAppendDecoratorTests.cs**: Added `using` for CancellationTokenSource
- [x] **PersistenceFixture.cs - Line 1288**: Fixed useless `seed++` assignment (changed to `seed` on last usage)
- [x] **PersistenceFixture.cs - Line 1029**: Fixed useless `seed++` assignment (changed to `seed` on last usage)
- [x] **BatchRepositoryTests.cs - Variable `ex`**: False positive - variable IS used to check `ex.Message`

### Documentation/Spelling Issues
- [x] **IBatchRepository.cs - Line 44-47**: Fixed "pased" → "passed", "es" → "e.g.,", "api" → "API", "arise" → "arises"
- [x] **IBatchStream.cs - Line 26**: Fixed "whith" → "with"

---

## Action Items

- [ ] Refactor foreach loops to use explicit `.Where()` filtering (5 items - non-test files)
- [ ] Combine if statements and improve code style (2 items - non-test files)
- [ ] Address security concern in build script (1 item)
