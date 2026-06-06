# 三种并发搜索实现对比

## 第一章：ES7 自定义并发搜索实现

### 1.1 背景

ES7 使用 Lucene 8.x，虽然 Lucene 8.x 的 `IndexSearcher` 已经支持传入 `Executor` 和 `CollectorManager`，但 ES7 的 `QueryPhase` 走的是单 Collector 的串行搜索路径 `search(Query, Collector)`，并没有利用 Lucene 的并发能力。因此需要自己实现并发搜索。

核心文件：`server/src/main/java/org/elasticsearch/search/internal/ContextIndexSearcher.java`（esjd 项目）

### 1.2 架构总览

```
QueryPhase
  │
  ├── 串行路径（原生 ES7）:
  │     searcher.search(query, collector)
  │       → search(leaves, weight, collector)  // 逐个 segment 串行搜索
  │
  └── 并发路径（自定义）:
        searcher.searchConcurrent(query, collectorManager, scoreMode)
          → rewrite(query)
          → createWeight(query, scoreMode, 1)
          → computeSlices(leafSlices, maxSlices)     // 按文档数分组
          → 每个 slice 创建 FutureTask 提交线程池     // 并发执行
          → future.get() 逐个等待结果                 // 阻塞收集
          → collectorManager.reduce(collectors)       // 合并结果
```

### 1.3 核心函数详解

#### 1.3.1 searchConcurrent — 并发搜索入口

```java
// ContextIndexSearcher.java 第292行
public <C extends Collector, T> T searchConcurrent(
    Query query, CollectorManager<C, T> collectorManager, ScoreMode scoreMode
) throws IOException {
    // 步骤1：拿到 Lucene 默认的 slices（1个segment = 1个slice）
    LeafSlice[] leafSlices = this.getSlices();

    // 步骤2：重写查询 + 编译成 Weight
    query = rewrite(query);
    final Weight weight = createWeight(query, scoreMode, 1);

    // 步骤3：按文档数重新分组（打散 Lucene 默认分组，重新计算）
    List<List<LeafReaderContext>> slices = computeSlices(leafSlices, getMaxSlices());

    // 步骤4：每个 slice 创建一个 FutureTask，提交到线程池
    int subTask = slices.size();
    final List<FutureTask<C>> topDocsFutures = new ArrayList<>(subTask);
    for (List<LeafReaderContext> leaves : slices) {
        final C collector = collectorManager.newCollector();
        FutureTask<C> task = new FutureTask<>(() -> {
            search(leaves, weight, collector);  // 在线程池中执行搜索
            return collector;
        });
        this.getExecutor().execute(task);       // 提交到 SEARCH 线程池
        topDocsFutures.add(task);
    }

    // 步骤5：逐个阻塞等待结果
    final List<C> collectedCollectors = new ArrayList<>();
    for (Future<C> future : topDocsFutures) {
        try {
            collectedCollectors.add(future.get());
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    // 步骤6：合并所有 Collector 的结果
    return collectorManager.reduce(collectedCollectors);
}
```

**执行流程示意图：**

```
主线程:
  ┌─────────────────────────────────────────────────────────────────┐
  │ rewrite(query) → createWeight() → computeSlices()              │
  │                                                                 │
  │ 提交任务:                                                       │
  │   executor.execute(task0)  ──→  线程池线程A: search(slice0)     │
  │   executor.execute(task1)  ──→  线程池线程B: search(slice1)     │
  │   executor.execute(task2)  ──→  线程池线程C: search(slice2)     │
  │                                                                 │
  │ 等待结果:                                                       │
  │   future0.get()  ←── 阻塞等待线程A完成                          │
  │   future1.get()  ←── 阻塞等待线程B完成                          │
  │   future2.get()  ←── 阻塞等待线程C完成                          │
  │                                                                 │
  │ collectorManager.reduce(collectors)  → 返回合并结果             │
  └─────────────────────────────────────────────────────────────────┘
```

注意：这里有一个问题 — 如果线程B抛异常，主线程在 `future0.get()` 处阻塞时感知不到，必须等 future0 完成后轮到 future1.get() 才能发现异常。其他线程的任务也不会被取消，会继续执行浪费资源。

#### 1.3.2 computeSlices — 分组算法

```java
// ContextIndexSearcher.java 第357行
public static List<List<LeafReaderContext>> computeSlices(LeafSlice[] leafSlices, int subTasks) {
    // 步骤1：把 Lucene 默认的 slices 全部打散成一个 segment 列表
    List<LeafReaderContext> sortedLeaves = new ArrayList<>();
    Arrays.stream(leafSlices).forEach(leafSlice -> Collections.addAll(sortedLeaves, leafSlice.leaves));

    // 步骤2：计算每个 slice 的最大文档数（至少占总文档数的 10%）
    final int docsTotal = sortedLeaves.stream().mapToInt(l -> l.reader().maxDoc()).sum();
    final double percentageDocsPerThread = Math.max(0.1, 1.0 / subTasks);
    int maxDocsPerSlice = (int)(docsTotal * percentageDocsPerThread);

    // 步骤3：按文档数从大到小排序
    sortedLeaves.sort(Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));

    // 步骤4：分组
    final List<List<LeafReaderContext>> groupedLeaves = new ArrayList<>();
    long docSum = 0;
    List<LeafReaderContext> group = null;
    for (LeafReaderContext ctx : sortedLeaves) {
        if (ctx.reader().maxDoc() > maxDocsPerSlice) {
            // 大 segment 独占一个 slice
            groupedLeaves.add(Collections.singletonList(ctx));
        } else {
            // 小 segment 往当前组里塞
            if (group == null) {
                group = new ArrayList<>();
                group.add(ctx);
                groupedLeaves.add(group);
            } else {
                group.add(ctx);
            }
            docSum += ctx.reader().maxDoc();
            if (docSum > maxDocsPerSlice) {
                // 当前组满了，开新组
                group.sort(Comparator.comparingInt(l -> l.docBase));
                group = null;
                docSum = 0;
            }
        }
    }
    if (group != null) {
        group.sort(Comparator.comparingInt(l -> l.docBase));
    }
    return groupedLeaves;
}
```

**分组算法示意图：**

```
输入：8个 segment，maxSlices = 4
segments（按文档数降序）: [100万, 50万, 30万, 20万, 10万, 8万, 5万, 2万]
总文档数 = 225万
maxDocsPerSlice = max(10%, 25%) * 225万 = 56.25万

分组过程：
  100万 > 56.25万  → 独占 Slice 0: [100万]
  50万 < 56.25万   → 开新组 Slice 1: [50万]
  30万             → 加入 Slice 1: [50万, 30万] → docSum=80万 > 56.25万 → 组满
  20万             → 开新组 Slice 2: [20万]
  10万             → 加入 Slice 2: [20万, 10万]
  8万              → 加入 Slice 2: [20万, 10万, 8万] → docSum=38万
  5万              → 加入 Slice 2: [20万, 10万, 8万, 5万] → docSum=43万
  2万              → 加入 Slice 2: [20万, 10万, 8万, 5万, 2万] → docSum=45万

最终结果：
  Slice 0: [100万]                          = 100万 文档
  Slice 1: [50万, 30万]                     = 80万 文档
  Slice 2: [20万, 10万, 8万, 5万, 2万]      = 45万 文档
                                               ↑ 负载不均衡！
```

可以看到这个算法的问题：尾部的小 segment 全部堆在最后一个组里，导致负载不均衡。Slice 0 有 100万文档，Slice 2 只有 45万，线程0的工作量是线程2的两倍多。

#### 1.3.3 search(leaves, weight, collector) — 单个 slice 的串行搜索

```java
// ContextIndexSearcher.java 第214行
@Override
public void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
    try {
        for (LeafReaderContext ctx : leaves) {
            if (canMatch(ctx) == false) {
                continue;  // search_after segment 级跳过优化
            }
            searchLeaf(ctx, weight, collector);
        }
    } catch (QueryPhase.TimeExceededException e) {
        timeExceeded = true;
    }
}
```

每个线程拿到自己的 slice（一组 segment），串行遍历其中的每个 segment 调用 `searchLeaf`。

`canMatch(ctx)` 是 ES7 自定义的 search_after segment 级跳过优化：根据 segment 的 min/max 统计信息判断这个 segment 是否可以整个跳过。

#### 1.3.4 searchLeaf — 单个 segment 搜索

```java
// ContextIndexSearcher.java 第240行
private void searchLeaf(LeafReaderContext ctx, Weight weight, Collector collector) throws IOException {
    cancellable.checkCancelled();
    weight = wrapWeight(weight);  // 包装成 CancellableBulkScorer
    final LeafCollector leafCollector;
    try {
        leafCollector = collector.getLeafCollector(ctx);
    } catch (CollectionTerminatedException e) {
        return;  // 这个 segment 不需要搜了
    }
    Bits liveDocs = ctx.reader().getLiveDocs();
    BitSet liveDocsBitSet = getSparseBitSetOrNull(liveDocs);
    if (liveDocsBitSet == null) {
        // 正常路径：BulkScorer 批量打分
        BulkScorer bulkScorer = weight.bulkScorer(ctx);
        if (bulkScorer != null) {
            try {
                bulkScorer.score(leafCollector, liveDocs);
            } catch (CollectionTerminatedException e) {
                // 提前终止
            }
        }
    } else {
        // 稀疏 BitSet 路径（文档级安全场景）
        Scorer scorer = weight.scorer(ctx);
        if (scorer != null) {
            try {
                intersectScorerAndBitSet(scorer, liveDocsBitSet, leafCollector, ...);
            } catch (CollectionTerminatedException e) {
                // 提前终止
            }
        }
    }
}
```

**单个 segment 搜索流程：**

```
searchLeaf(segment_0)
  │
  ├── 1. checkCancelled() — 检查是否已取消
  │
  ├── 2. wrapWeight(weight) — 如果有取消检查，包装成 CancellableBulkScorer
  │
  ├── 3. collector.getLeafCollector(ctx) — 获取这个 segment 的收集器
  │     └── 如果抛 CollectionTerminatedException → 跳过这个 segment
  │
  ├── 4. 获取 liveDocs（标记哪些文档没被删除）
  │
  └── 5. 执行搜索（两条路径）
        ├── 正常路径: bulkScorer.score(leafCollector, liveDocs)
        │   → 遍历所有匹配文档，逐个打分并交给 collector 收集
        │
        └── 稀疏路径: intersectScorerAndBitSet(scorer, bitset, collector)
            → 用迭代器求交集，适用于文档级安全（DLS）场景
```

### 1.4 完整执行流程示例

假设一个分片有 5 个 segment，maxSlices = 3：

```
segments: [seg0(80万), seg1(40万), seg2(30万), seg3(15万), seg4(10万)]
总文档数 = 175万
maxDocsPerSlice = max(10%, 33%) * 175万 = 58.3万

Step 1: computeSlices 分组
  seg0(80万) > 58.3万 → 独占 Slice 0
  seg1(40万) → 新组 Slice 1
  seg2(30万) → 加入 Slice 1, docSum=70万 > 58.3万 → 组满
  seg3(15万) → 新组 Slice 2
  seg4(10万) → 加入 Slice 2, docSum=25万

  结果：
    Slice 0: [seg0(80万)]
    Slice 1: [seg1(40万), seg2(30万)]
    Slice 2: [seg3(15万), seg4(10万)]

Step 2: 并发执行
  ┌──────────────────────────────────────────────────────────┐
  │ 主线程: rewrite + createWeight + computeSlices           │
  │                                                          │
  │ 线程A (Slice 0):                                         │
  │   searchLeaf(seg0) → bulkScorer.score() → 收集80万文档   │
  │                                                          │
  │ 线程B (Slice 1):                                         │
  │   searchLeaf(seg1) → bulkScorer.score() → 收集40万文档   │
  │   searchLeaf(seg2) → bulkScorer.score() → 收集30万文档   │
  │                                                          │
  │ 线程C (Slice 2):                                         │
  │   searchLeaf(seg3) → bulkScorer.score() → 收集15万文档   │
  │   searchLeaf(seg4) → bulkScorer.score() → 收集10万文档   │
  │                                                          │
  │ 主线程: future0.get() → future1.get() → future2.get()   │
  │                                                          │
  │ 主线程: collectorManager.reduce([collectorA, B, C])      │
  │   → 合并三个 collector 的 top N 文档                      │
  │   → 返回全局 top N                                        │
  └──────────────────────────────────────────────────────────┘
```

### 1.5 ES7 实现的特点总结

| 特性 | 说明 |
|------|------|
| Lucene 版本 | 8.x |
| 并发调度方式 | 手动 FutureTask + executor.execute() |
| 结果等待方式 | 逐个 future.get() 阻塞等待 |
| 分组算法 | 大 segment 独占 + 小 segment 顺序填充 |
| 分组时机 | 每次搜索时重新计算（先拿 Lucene 默认分组，打散后重新分） |
| 打分优化 | 无自动 ConstantScoreQuery 优化，scoreMode 由外部传入 |
| 超时处理 | catch TimeExceededException，标记 timeExceeded |
| 聚合后处理 | 超时后不做聚合 postCollection |
| 异常传播 | 某个 slice 异常不会取消其他 slice |
| segment 跳过 | 有 search_after segment 级 canMatch 优化 |
| 段内并发 | 不支持 |

---

## 第二章：ES 新版（Lucene 9.12.2）并发搜索实现

### 2.1 背景

ES 新版（本项目 `elasticsearch`，基于 Lucene 9.12.2）将并发搜索作为原生能力内置到 `ContextIndexSearcher` 中，不再需要单独的 `searchConcurrent` 方法。它覆盖了 Lucene `IndexSearcher` 的 `slices()` 方法从源头接管分组逻辑，并利用 Lucene 的 `TaskExecutor` 框架进行并发调度。

核心文件：`server/src/main/java/org/elasticsearch/search/internal/ContextIndexSearcher.java`（elasticsearch 项目）

### 2.2 架构总览

```
QueryPhase.execute(searchContext)
  │
  └── addCollectorsAndSearch(searchContext)
        │
        └── searcher.search(query, collectorManager)          // 第311行，入口
              │
              ├── 1. newCollector() → 获取 scoreMode
              ├── 2. 是否需要打分？
              │     ├── 是 → rewrite(query)
              │     └── 否 → rewrite(ConstantScoreQuery(query))  ← 自动跳过打分
              ├── 3. createWeight(query, scoreMode, 1)
              │     └── 超时？→ 返回空结果（优雅降级）
              │
              └── search(weight, collectorManager, firstCollector)  // 第330行
                    │
                    ├── getSlices()  ← 构造时已通过 computeSlices() 算好
                    ├── 每个 slice 创建一个 Collector
                    ├── 每个 slice 包装成 Callable
                    ├── TaskExecutor.invokeAll(tasks)  ← 并发执行
                    └── collectorManager.reduce(collectors)  ← 合并结果
```

与 ES7 最大的区别：并发搜索不是一个独立方法，而是覆盖了 `IndexSearcher.search(Query, CollectorManager)` 本身。调用方无需关心是否并发，框架自动处理。

### 2.3 核心函数详解

#### 2.3.1 构造函数 — 分组在构造时完成

```java
// ContextIndexSearcher.java 第120行
ContextIndexSearcher(
    IndexReader reader, Similarity similarity,
    QueryCache queryCache, QueryCachingPolicy queryCachingPolicy,
    MutableQueryTimeout cancellable, boolean wrapWithExitableDirectoryReader,
    Executor executor, int maximumNumberOfSlices, int minimumDocsPerSlice
) throws IOException {
    // 调用 Lucene IndexSearcher 构造函数，传入 executor
    // 这会触发 slices() 方法调用，计算分组并缓存
    super(wrapWithExitableDirectoryReader
        ? new ExitableDirectoryReader((DirectoryReader) reader, cancellable)
        : reader, executor);
    setSimilarity(similarity);
    setQueryCache(queryCache);
    setQueryCachingPolicy(queryCachingPolicy);
    this.cancellable = cancellable;
    this.minimumDocsPerSlice = minimumDocsPerSlice;
    this.maximumNumberOfSlices = maximumNumberOfSlices;
}
```

关键点：`super(reader, executor)` 调用时，Lucene 的 `IndexSearcher` 构造函数会调用 `this.slices(reader.leaves())`。ES 覆盖了这个方法：

```java
// 第142行
@Override
protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
    LeafSlice[] leafSlices = computeSlices(getLeafContexts(), maximumNumberOfSlices, minimumDocsPerSlice);
    assert leafSlices.length <= maximumNumberOfSlices;
    return leafSlices;
}
```

**分组时机对比：**

```
ES7:  每次调用 searchConcurrent() 时重新计算分组
      getSlices() → 打散 → computeSlices() → 搜索

ES新版: 构造 ContextIndexSearcher 时一次性计算，缓存在父类字段中
        构造函数 → super() → slices() → computeSlices() → 缓存
        搜索时 getSlices() 直接拿缓存结果
```

#### 2.3.2 computeSlices — 贪心 + 优先队列分组算法

```java
// ContextIndexSearcher.java 第243行
public static LeafSlice[] computeSlices(List<LeafReaderContext> leaves, int maxSliceNum, int minDocsPerSlice) {
    if (maxSliceNum == 1) {
        return new LeafSlice[] { new LeafSlice(new ArrayList<>(leaves)) };
    }
    final int numDocs = leaves.stream().mapToInt(l -> l.reader().maxDoc()).sum();
    final double percentageDocsPerThread = Math.max(0.1, 1.0 / maxSliceNum);
    // 取百分比计算值和绝对最小值的较大者
    return computeSlices(leaves, Math.max(minDocsPerSlice, (int)(percentageDocsPerThread * numDocs)));
}

// 第258行 — 实际分组逻辑
private static LeafSlice[] computeSlices(List<LeafReaderContext> leaves, int minDocsPerSlice) {
    // 步骤1：按文档数从大到小排序
    List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);
    sortedLeaves.sort((c1, c2) -> Integer.compare(c2.reader().maxDoc(), c1.reader().maxDoc()));

    // 步骤2：贪心分组 — 往当前组里加 segment，满了就开新组
    final PriorityQueue<List<LeafReaderContext>> queue = new PriorityQueue<>(
        (c1, c2) -> Integer.compare(sumMaxDocValues(c1), sumMaxDocValues(c2))
    );
    long docSum = 0;
    List<LeafReaderContext> group = new ArrayList<>();
    for (LeafReaderContext ctx : sortedLeaves) {
        group.add(ctx);
        docSum += ctx.reader().maxDoc();
        if (docSum > minDocsPerSlice) {
            queue.add(group);
            group = new ArrayList<>();
            docSum = 0;
        }
    }

    // 步骤3：尾部小 segment 用优先队列均衡分配
    if (group.size() > 0) {
        if (queue.size() == 0) {
            queue.add(group);
        } else {
            // 逐个把剩余 segment 塞给当前文档数最少的组
            for (LeafReaderContext context : group) {
                final List<LeafReaderContext> head = queue.poll();  // 取最小的组
                head.add(context);
                queue.add(head);  // 放回队列
            }
        }
    }

    final LeafSlice[] slices = new LeafSlice[queue.size()];
    int upto = 0;
    for (List<LeafReaderContext> currentLeaf : queue) {
        slices[upto++] = new LeafSlice(currentLeaf);
    }
    return slices;
}
```

**分组算法示意图（同样的输入，对比 ES7）：**

```
输入：8个 segment，maxSliceNum = 4
segments（降序）: [100万, 50万, 30万, 20万, 10万, 8万, 5万, 2万]
总文档数 = 225万
minDocsPerSlice = max(minDocsPerSlice, max(10%, 25%) * 225万) = 56.25万

贪心分组阶段：
  100万 → group=[100万], docSum=100万 > 56.25万 → 入队, 开新组
  50万  → group=[50万], docSum=50万
  30万  → group=[50万, 30万], docSum=80万 > 56.25万 → 入队, 开新组
  20万  → group=[20万], docSum=20万
  10万  → group=[20万, 10万], docSum=30万
  8万   → group=[20万, 10万, 8万], docSum=38万
  5万   → group=[20万, 10万, 8万, 5万], docSum=43万
  2万   → group=[20万, 10万, 8万, 5万, 2万], docSum=45万
  → 循环结束，group 未满，进入尾部处理

队列状态：queue = [Slice(80万), Slice(100万)]

尾部优先队列分配：
  20万 → poll 最小组 Slice(80万) → 加入 → Slice(100万), push Slice(80万+20万=100万)
  10万 → poll 最小组 Slice(100万) → 加入 → push Slice(110万)
  8万  → poll 最小组 Slice(100万) → 加入 → push Slice(108万)
  5万  → poll 最小组 Slice(108万) → 加入 → push Slice(113万)
  2万  → poll 最小组 Slice(110万) → 加入 → push Slice(112万)

最终结果：
  Slice 0: [100万]                              = 100万
  Slice 1: [50万, 30万, 20万, 8万, 5万]         = 113万
  Slice 2: [10万, 2万] (从100万组分出来的)       = 112万
                                                    ↑ 比 ES7 均衡得多！

对比 ES7 的结果：
  ES7:    Slice 0=100万, Slice 1=80万, Slice 2=45万  (最大/最小 = 2.2x)
  ES新版: Slice 0=100万, Slice 1=113万, Slice 2=112万 (最大/最小 = 1.13x)
```

#### 2.3.3 search(Query, CollectorManager) — 搜索入口

```java
// ContextIndexSearcher.java 第311行
@Override
public <C extends Collector, T> T search(Query query, CollectorManager<C, T> collectorManager) throws IOException {
    final C firstCollector = collectorManager.newCollector();
    // 自动判断是否需要打分
    query = firstCollector.scoreMode().needsScores()
        ? rewrite(query)
        : rewrite(new ConstantScoreQuery(query));  // 不需要分数 → 跳过 BM25 计算
    final Weight weight;
    try {
        weight = createWeight(query, firstCollector.scoreMode(), 1);
    } catch (TimeExceededException e) {
        // createWeight 超时 → 优雅降级，返回空结果
        timeExceeded = true;
        doAggregationPostCollection(firstCollector);
        return collectorManager.reduce(Collections.singletonList(firstCollector));
    }
    return search(weight, collectorManager, firstCollector);
}
```

#### 2.3.4 search(Weight, CollectorManager, C) — 并发调度

```java
// ContextIndexSearcher.java 第330行
private <C extends Collector, T> T search(Weight weight, CollectorManager<C, T> collectorManager, C firstCollector)
    throws IOException {
    LeafSlice[] leafSlices = getSlices();  // 拿构造时缓存的分组
    if (leafSlices.length == 0) {
        doAggregationPostCollection(firstCollector);
        return collectorManager.reduce(Collections.singletonList(firstCollector));
    } else {
        // 每个 slice 一个 Collector
        final List<C> collectors = new ArrayList<>(leafSlices.length);
        collectors.add(firstCollector);
        for (int i = 1; i < leafSlices.length; ++i) {
            collectors.add(collectorManager.newCollector());
        }
        // 每个 slice 包装成 Callable
        final List<Callable<C>> listTasks = new ArrayList<>(leafSlices.length);
        for (int i = 0; i < leafSlices.length; ++i) {
            final LeafReaderContext[] leaves = leafSlices[i].leaves;
            final C collector = collectors.get(i);
            listTasks.add(() -> {
                search(Arrays.asList(leaves), weight, collector);
                return collector;
            });
        }
        // 并发执行所有任务
        List<C> collectedCollectors = getTaskExecutor().invokeAll(listTasks);
        return collectorManager.reduce(collectedCollectors);
    }
}
```

**并发调度对比：**

```
ES7:
  FutureTask + executor.execute()  → 手动提交
  future.get()                     → 逐个阻塞等待
  异常不会取消其他任务

ES新版:
  Callable + TaskExecutor.invokeAll()  → 统一提交
  invokeAll 内部管理                    → 自动等待所有任务
  某个任务异常 → 取消其他正在执行的任务   → 避免资源浪费
```

#### 2.3.5 search(leaves, weight, collector) — 遍历 segment + 聚合后处理

```java
// ContextIndexSearcher.java 第371行
@Override
public void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
    boolean success = false;
    try {
        super.search(leaves, weight, collector);  // Lucene 原生搜索
        success = true;
    } catch (TimeExceededException e) {
        timeExceeded = true;
    } finally {
        // 关键：无论成功还是超时，都要执行聚合后处理
        if (success || timeExceeded) {
            try {
                timeoutOverwrites.set(true);       // 临时禁用超时检查
                doAggregationPostCollection(collector);  // 聚合收尾
            } finally {
                timeoutOverwrites.set(false);
            }
        }
    }
}
```

这是 ES 新版相比 ES7 的重要改进：

```
ES7 超时处理：
  catch TimeExceededException → timeExceeded = true → 结束
  问题：聚合的 postCollection 没有执行，聚合结果可能不完整

ES新版 超时处理：
  catch TimeExceededException → timeExceeded = true
  finally → 临时禁用超时 → doAggregationPostCollection() → 恢复超时
  效果：即使超时，聚合也能正确收尾，返回部分但一致的结果
```

#### 2.3.6 searchLeaf — 单个 segment 搜索

```java
// ContextIndexSearcher.java 第427行
@Override
protected void searchLeaf(LeafReaderContext ctx, Weight weight, Collector collector) throws IOException {
    cancellable.checkCancelled();
    final LeafCollector leafCollector;
    try {
        leafCollector = collector.getLeafCollector(ctx);
    } catch (CollectionTerminatedException e) {
        return;
    }
    Bits liveDocs = ctx.reader().getLiveDocs();
    BitSet liveDocsBitSet = getSparseBitSetOrNull(liveDocs);
    if (liveDocsBitSet == null) {
        // 正常路径
        BulkScorer bulkScorer = weight.bulkScorer(ctx);
        if (bulkScorer != null) {
            if (cancellable.isEnabled()) {
                bulkScorer = new CancellableBulkScorer(bulkScorer, cancellable::checkCancelled);
            }
            try {
                bulkScorer.score(leafCollector, liveDocs);
            } catch (CollectionTerminatedException e) { }
        }
    } else {
        // 稀疏路径
        Scorer scorer = weight.scorer(ctx);
        if (scorer != null) {
            try {
                intersectScorerAndBitSet(scorer, liveDocsBitSet, leafCollector, ...);
            } catch (CollectionTerminatedException e) { }
        }
    }
    leafCollector.finish();  // ES7 没有这个调用
}
```

与 ES7 的 `searchLeaf` 对比：
- ES 新版的取消检查直接在 `searchLeaf` 里用 `CancellableBulkScorer` 包装，而 ES7 是在 `wrapWeight` 里包装
- ES 新版最后调用了 `leafCollector.finish()`，这是 Lucene 9.x 新增的 API，用于通知 collector 当前 segment 的收集已完成

### 2.4 完整执行流程示例

假设一个分片有 5 个 segment，maximumNumberOfSlices = 3，minimumDocsPerSlice = 10万：

```
segments: [seg0(80万), seg1(40万), seg2(30万), seg3(15万), seg4(10万)]
总文档数 = 175万
percentageDocsPerThread = max(10%, 33%) = 33%
minDocsPerSlice = max(10万, 33% * 175万) = max(10万, 58.3万) = 58.3万

Step 1: 构造时 computeSlices 分组
  贪心阶段：
    seg0(80万) → group=[seg0], docSum=80万 > 58.3万 → 入队
    seg1(40万) → group=[seg1], docSum=40万
    seg2(30万) → group=[seg1,seg2], docSum=70万 > 58.3万 → 入队
    seg3(15万) → group=[seg3], docSum=15万
    seg4(10万) → group=[seg3,seg4], docSum=25万 → 循环结束

  队列：[Slice(70万), Slice(80万)]
  剩余：[seg3(15万), seg4(10万)]

  尾部分配：
    seg3(15万) → 给最小组 Slice(70万) → Slice(85万)
    seg4(10万) → 给最小组 Slice(80万) → Slice(90万)

  最终分组：
    Slice 0: [seg0(80万), seg4(10万)]       = 90万
    Slice 1: [seg1(40万), seg2(30万), seg3(15万)] = 85万

Step 2: search(query, collectorManager) 入口
  → newCollector() → 判断 scoreMode
  → 假设按 timestamp 排序，不需要分数
  → rewrite(new ConstantScoreQuery(query))  ← 跳过 BM25
  → createWeight()

Step 3: search(weight, collectorManager, firstCollector) 并发调度
  ┌──────────────────────────────────────────────────────────────┐
  │                                                              │
  │ TaskExecutor.invokeAll([task0, task1])                       │
  │                                                              │
  │ 线程A (Slice 0):                                             │
  │   search([seg0, seg4], weight, collectorA)                   │
  │     → searchLeaf(seg0) → bulkScorer.score() → 收集80万文档   │
  │     → searchLeaf(seg4) → bulkScorer.score() → 收集10万文档   │
  │     → doAggregationPostCollection(collectorA)                │
  │                                                              │
  │ 线程B (Slice 1):                                             │
  │   search([seg1, seg2, seg3], weight, collectorB)             │
  │     → searchLeaf(seg1) → bulkScorer.score() → 收集40万文档   │
  │     → searchLeaf(seg2) → bulkScorer.score() → 收集30万文档   │
  │     → searchLeaf(seg3) → bulkScorer.score() → 收集15万文档   │
  │     → doAggregationPostCollection(collectorB)                │
  │                                                              │
  │ invokeAll 等待所有任务完成                                    │
  │ （如果线程A异常，线程B会被取消）                               │
  │                                                              │
  │ collectorManager.reduce([collectorA, collectorB])            │
  │   → 合并两个 collector 的 top N → 返回全局 top N             │
  └──────────────────────────────────────────────────────────────┘
```

### 2.5 ES 新版实现的特点总结

| 特性 | 说明 |
|------|------|
| Lucene 版本 | 9.12.2 |
| 并发调度方式 | Callable + Lucene TaskExecutor.invokeAll() |
| 结果等待方式 | invokeAll 统一管理，异常时取消其他任务 |
| 分组算法 | 贪心填充 + 尾部优先队列均衡分配 |
| 分组时机 | 构造时一次性计算，搜索时直接使用缓存 |
| 打分优化 | 自动 ConstantScoreQuery（不需要分数时跳过 BM25） |
| 超时处理 | 超时后仍执行聚合 postCollection，临时禁用超时防止收尾被中断 |
| 异常传播 | invokeAll 内部处理，某任务异常会取消其他任务 |
| segment 跳过 | 无 segment 级 canMatch（can_match 在分片级） |
| 段内并发 | 不支持 |
## 第三章：OpenSearch 3.4.0（Lucene 10.3.1）并发搜索 + 段内并发实现

### 3.1 背景

OpenSearch 3.4.0 基于 Lucene 10.3.1，是三个项目中 Lucene 版本最高的。它的并发搜索有两大特点：
1. 完全依赖 Lucene 原生的并发框架（不自己调度线程）
2. 支持段内并发（Intra-Segment Parallelism）— 把一个大 segment 按 docId 范围拆成多个 partition，分给不同线程并发搜索

核心文件：
- `server/src/main/java/org/opensearch/search/internal/ContextIndexSearcher.java`
- `server/src/main/java/org/opensearch/search/internal/MaxTargetSliceSupplier.java`
- `server/src/main/java/org/opensearch/search/query/QueryPhase.java`

### 3.2 架构总览

```
QueryPhase.execute(searchContext)
  │
  └── queryPhaseSearcher.searchWith(searchContext, searcher, query, collectors, ...)
        │                              ↑ 策略模式，可插拔
        └── DefaultQueryPhaseSearcher.searchWithCollector(...)
              │
              └── searcher.search(query, collector)     // 单 Collector 入口
                    │
                    ├── rewrite + createWeight
                    ├── 转成 LeafReaderContextPartition[]
                    └── search(partitions, weight, collector)  // 覆盖 Lucene 方法
                          │
                          └── for each partition:
                                searchLeaf(ctx, minDocId, maxDocId, weight, collector)
```

与 ES 新版最大的区别：
- OpenSearch 传入的是单个 `Collector`，不是 `CollectorManager`
- 并发调度完全交给 Lucene 的 `IndexSearcher` 框架（通过 executor）
- 支持 `LeafReaderContextPartition`，可以拆分 segment

### 3.3 并发搜索核心流程

#### 3.3.1 QueryPhaseSearcher — 可插拔的搜索策略

```java
// QueryPhaseSearcher.java
public interface QueryPhaseSearcher {
    boolean searchWith(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException;
}
```

OpenSearch 把搜索逻辑抽成了接口，默认实现是 `DefaultQueryPhaseSearcher`。插件可以替换这个实现来自定义搜索行为（比如向量搜索插件可以注入自己的搜索逻辑）。ES 新版没有这个扩展点。

#### 3.3.2 search(Query, Collector) — 搜索入口

```java
// ContextIndexSearcher.java 第271行
@Override
public void search(Query query, Collector collector) throws IOException {
    // 自动判断是否需要打分（和 ES 新版一样的优化）
    query = collector.scoreMode().needsScores()
        ? rewrite(query)
        : rewrite(new ConstantScoreQuery(query));
    Weight weight = createWeight(query, collector.scoreMode(), 1);

    // 把每个 segment 转成 LeafReaderContextPartition
    LeafReaderContextPartition[] partitions = getLeafContexts().stream()
        .map(LeafReaderContextPartition::createForEntireSegment)
        .toArray(LeafReaderContextPartition[]::new);

    search(partitions, weight, collector);
}
```

注意这里传的是单个 `Collector`，不是 `CollectorManager`。并发调度由 Lucene 的 `IndexSearcher` 在底层处理 — 当构造函数传了 `executor` 时，Lucene 会自动把不同 slice 的搜索任务分发到线程池。

#### 3.3.3 search(partitions, weight, collector) — 遍历 partition

```java
// ContextIndexSearcher.java 第303行
@Override
protected void search(LeafReaderContextPartition[] partitions, Weight weight, Collector collector)
    throws IOException {
    searchContext.indexShard().getSearchOperationListener().onPreSliceExecution(searchContext);
    try {
        // 时序数据优化：如果是升序查询，反向遍历 segment（从最老的开始）
        if (searchContext.shouldUseTimeSerrtOptimization()) {
            for (int i = partitions.length - 1; i >= 0; i--) {
                searchLeaf(partitions[i].ctx, partitions[i].minDocId, partitions[i].maxDocId,
                    weight, collector);
            }
        } else {
            for (LeafReaderContextPartition partition : partitions) {
                searchLeaf(partition.ctx, partition.minDocId, partition.maxDocId,
                    weight, collector);
            }
        }
        // 聚合后处理
        searchContext.bucketCollectorProcessor().processPostCollection(collector);
    } catch (Throwable t) {
        searchContext.indexShard().getSearchOperationListener().onFailedSliceExecution(searchContext);
        throw t;
    }
    searchContext.indexShard().getSearchOperationListener().onSliceExecution(searchContext);
}
```

关键点：
- 有搜索操作监听器（`onPreSliceExecution` / `onSliceExecution`），可以监控每个 slice 的执行
- 时序数据优化：默认 segment 按时间降序排列（最新在前），如果查询是升序，就反向遍历
- 聚合后处理在 slice 级别执行（每个线程处理完自己的 slice 后立即做聚合收尾）

#### 3.3.4 searchLeaf — 支持 docId 范围的 segment 搜索

```java
// ContextIndexSearcher.java 第335行
@Override
protected void searchLeaf(LeafReaderContext ctx, int minDocId, int maxDocId,
    Weight weight, Collector collector) throws IOException {

    // search_after segment 级跳过优化
    if (canMatch(ctx) == false) {
        return;
    }

    final LeafCollector leafCollector;
    try {
        cancellable.checkCancelled();
        weight = wrapWeight(weight);
        collector.setWeight(weight);
        leafCollector = collector.getLeafCollector(ctx);
    } catch (CollectionTerminatedException e) {
        return;
    } catch (QueryPhase.TimeExceededException e) {
        searchContext.setSearchTimedOut(true);
        return;
    }

    Bits liveDocs = ctx.reader().getLiveDocs();
    BitSet liveDocsBitSet = getSparseBitSetOrNull(liveDocs);
    if (liveDocsBitSet == null) {
        BulkScorer bulkScorer = weight.bulkScorer(ctx);
        if (bulkScorer != null) {
            try {
                // 关键区别：传入 minDocId 和 maxDocId，只搜索指定范围
                bulkScorer.score(leafCollector, liveDocs, minDocId, maxDocId);
            } catch (CollectionTerminatedException e) {
            } catch (QueryPhase.TimeExceedption e) {
                searchContext.setSearchTimedOut(true);
                return;
            }
        }
    } else {
        Scorer scorer = weight.scorer(ctx);
        if (scorer != null) {
            try {
                // 稀疏路径也支持范围
                intersectScorerAndBitSet(scorer, liveDocsBitSet, leafCollector,
                    minDocId, maxDocId, ...);
            } catch (...) { ... }
        }
    }

    // 流式聚合：每搜完一个 segment 就可以发送中间结果
    if (searchContext.isStreamSearch() && searchContext.getFlushMode() == FlushMode.PER_SEGMENT) {
        List<InternalAggregation> batch = seabucketCollectorProcessor().buildAggBatch(collector);
        if (!batch.isEmpty()) {
            sendBatch(batch);
        }
    }

    leafCollector.finish();
}
```

与 ES7 和 ES 新版的 `searchLeaf` 对比：

```
ES7:       searchLeaf(ctx, weight, collector)
             → bulkScorer.score(leafCollector, liveDocs)
             → 搜索整个 segment

ES新版:    searchLeaf(ctx, weight, collector)
             → bulkScorer.score(leafCollector, liveDocs)
             → 搜索整个 segment

OpenSearch: searchLeaf(ctx, minDocId, maxDocId, weight, collector)
             → bulkScorer.score(leafCollector, liveDocs, minDocId, maxDocId)
             → 只搜索 [minDocId, maxDocId) 范围内的文档
```

### 3.4 段内并发详解 — MaxTargetSliceSupplier

这是 OpenSearch 最独特的部分。`MaxTargetSliceSupplier` 负责把 segment 列表分组成 slice，支持把大 segment 拆分成多个 partition。

#### 3.4.1 入口 — 三种分组策略

```java
// MaxTargetSliceSupplier.java 第34行
static IndexSearcher.LeafSlice[] getSlices(
    List<LeafReaderContext> leaves,
    int targetMaxSlice,
    boolean useIntraSegmentSearch,    // 是否启用段内并发
    String partitionStrategy,          // 分区策略："auto" 或 "force"
    int minSegmentSize                 // 最小 segment 大小（低于此值不拆分）
) {
    if (useIntraSegmentSearch == false) {
        return getSlicesWholeSegments(leaves, targetMaxSlice);        // 策略1：整段分组
    } else if ("force".equals(partitionStrategy)) {
        return getSlicesWithForcePartitioning(leaves, targetMaxSlice); // 策略2：强制拆分
    } else {
        return getSlicesWithAutoPartitioning(leaves, targetMaxSlice, minSegmentSize); // 策略3：自动拆分
    }
}
```

#### 3.4.2 策略1：getSlicesWholeSegments — 整段分组（不拆分 segment）

```java
static IndexSearcher.LeafSlice[] getSlicesWholeSegments(List<LeafReaderContext> leaves, int targetMaxSlice) {
    List<LeafReaderContextPartition> partitions = new ArrayList<>(leaves.size());
    for (LeafReaderContext leaf : leaves) {
        partitions.add(LeafReaderContextPartition.createForEntireSegment(leaf));
    }
    return distributePartitions(partitions, targetMaxSlice);
}
```

每个 segment 作为一个完整的 partition，然后用 `distributePartitions` 分配到 slice 中。这和 ES 新版的行为类似。

#### 3.4.3 策略2：getSlicesWithForcePartitioning — 强制拆分所有 segment

```java
static IndexSearcher.LeafSlice[] getSlicesWithForcePartitioning(
    List<LeafReaderContext> leaves, int targetMaxSlice) {
    List<LeafReaderContextPartition> partitions = new ArrayList<>(leaves.size() * targetMaxSlice);
    for (LeafReaderContext leaf : leaves) {
        int numPartitions = Math.min(targetMaxSlice, leaf.reader().maxDoc());
        addPartitions(partitions, leaf, numPartitions);
    }
    return distributePartitions(partitions, targetMaxSlice);
}
```

每个 segment 都拆成 `targetMaxSlice` 份，不管大小。

```
示例：3个 segment，targetMaxSlice = 3

seg0(90万):  拆成3份 → [0,30万), [30万,60万), [60万,90万)
seg1(30万):  拆成3份 → [0,10万), [10万,20万), [20万,30万)
seg2(6万):   拆成3份 → [0,2万), [2万,4万), [4万,6万)

共 9 个 partition，分配到 3 个 slice
```

适用场景：segment 数量很少但单个很大（比如只有 1-2 个 segment），或者用于性能测试。

#### 3.4.4 策略3：getSlicesWithAutoPartitioning — 自动拆分大 segment

```java
static IndexSearcher.LeafSlice[] getSlicesWithAutoPartitioning(
    List<LeafReaderContext> leaves, int targetMaxSlice, int minSegmentSize) {
    // 计算每个 slice 的理想文档数
    long totalDocs = 0;
    for (LeafReaderContext leaf : leaves) {
        totalDocs += leaf.reader().maxDoc();
    }
    long maxDocsPerPartition = (totalDocs + targetMaxSlice - 1) / targetMaxSlice;  // 向上取整

    List<LeafReaderContextPartition> partitions = new ArrayList<>();
    for (LeafReaderContext leaf : leaves) {
        int segmentSize = leaf.reader().maxDoc();
        if (segmentSize > maxDocsPerPartition && segmentSize >= minSegmentSize) {
            // 大 segment：拆分
            int numPartitions = (int)((segmentSize + maxDocsPerPartition - 1) / maxDocsPerPartition);
            addPartitions(partitions, leaf, Math.min(numPartitions, targetMaxSlice));
        } else {
            // 小 segment：不拆
            partitions.add(LeafReaderContextPartition.createForEntireSegment(leaf));
        }
    }
    return distributePartitions(partitions, targetMaxSlice);
}
```

拆分条件（两个 AND）：
1. segment 文档数 > 每个 slice 的理想文档数（`maxDocsPerPartition`）
2. segment 文档数 >= `minSegmentSize`（防止拆太小的 segment）

```
示例：4个 segment，targetMaxSlice = 4，minSegmentSize = 50万

segments: [seg0(200万), seg1(80万), seg2(30万), seg3(10万)]
总文档数 = 320万
maxDocsPerPartition = ceil(320万/4) = 80万

seg0(200万): 200万 > 80万 且 >= 50万 → 拆分
  numPartitions = ceil(200万/80万) = 3, min(3, 4) = 3
  → [0, 66.6万), [66.6万, 133.3万), [133.3万, 200万)

seg1(80万): 80万 = 80万，不大于 → 不拆分
  → [0, 80万) 整段

seg2(30万): 30万 < 80万 → 不拆分
  → [0, 30万) 整段

seg3(10万): 10万 < 80万 → 不拆分
  → [0, 10万) 整段

共 6 个 partition，分配到 4 个 slice
```

#### 3.4.5 addPartitions — 段内拆分核心逻辑

```java
// MaxTargetSliceSupplier.java 第105行
private static void addPartitions(
    List<LeafReaderContextPartition> partitions, LeafReaderContext leaf, int numPartitions) {
    int segmentSize = leaf.reader().maxDoc();
    if (numPartitions > 1) {
        int docsPerPartition = segmentSize / numPartitions;
        for (int i = 0; i < numPartitions; i++) {
            int startDoc = i * docsPerPartition;
            int endDoc = (i == numPartitions - 1) ? segmentSize : startDoc + docsPerPartition;
            partitions.add(LeafReaderContextPartition.createFromAndTo(leaf, startDoc, endDoc));
        }
    } else {
        partitions.add(LeafReaderContextPartition.createForEntireSegment(leaf));
    }
}
```

按 docId 均匀切分：

```
segment_0 有 300万 文档，numPartitions = 3：
  docsPerPartition = 300万 / 3 = 100万

  i=0: startDoc=0,      endDoc=100万   → Partition [0, 100万)
  i=1: startDoc=100万,  endDoc=200万   → Partition [100万, 200万)
  i=2: startDoc=200万,  endDoc=300万   → Partition [200万, 300万)  ← 最后一个兜底到 segmentSize
```

`createFromAndTo(leaf, startDoc, endDoc)` 是 Lucene 10.x 的 API，创建一个只覆盖指定 docId 范围的 partition。后续 `searchLeaf` 调用 `bulkScorer.score(collector, liveDocs, startDoc, endDoc)` 时只搜索这个范围。

#### 3.4.6 distributePartitions — 带 segment 去重约束的 LPT 分配算法

这是段内并发最精妙的部分。拆分 segment 后有一个 Lucene 的硬约束：**同一个 segment 的不同 partition 必须在不同的 slice 里。**

原因：Lucene 的 `Collector.getLeafCollector(ctx)` 是按 segment 粒度的。如果同一个 slice 里有同一个 segment 的两个 partition，第二次调用 `getLeafCollector(同一个ctx)` 会覆盖第一次的状态，导致数据丢失。

```java
// MaxTargetSliceSupplier.java 第123行
static IndexSearcher.LeafSlice[] distributePartitions(
    List<LeafReaderContextPartition> partitions, int targetMaxSlice) {
    int sliceCount = Math.min(targetMaxSlice, partitions.size());

    // 按文档数从大到小排序
    partitions.sort(Collections.reverseOrder(
        Comparator.comparingInt(MaxTargetSliceSupplier::getPartitionDocCount)));

    // 创建带 segment 追踪的分组
    GroupWithSegmentTracking[] slices = new GroupWithSegmentTracking[sliceCount];
    for (int i = 0; i < sliceCount; i++) {
        slices[i] = new GroupWithSegmentTracking(i);
    }

    // LPT（Longest Processing Time）算法 + segment 去重约束
    for (LeafReaderContextPartition partition : partitions) {
        int segmentOrd = partition.ctx.ord;
        int docCount = getPartitionDocCount(partition);

        // 找负载最小的、且不包含这个 segment 的 slice
        GroupWithSegmentTracking targetSlice = null;
        long minLoad = Long.MAX_VALUE;
        for (GroupWithSegmentTracking slice : slices) {
            if (slice.hasSegment(segmentOrd) == false && slice.docCountSum < minLoad) {
                minLoad = slice.docCountSum;
                targetSlice = slice;
            }
        }
        targetSlice.addPartition(partition, docCount);
    }

    // 收集非空 slice
    List<IndexSearcher.LeafSlice> result = new ArrayList<>(sliceCount);
    for (GroupWithSegmentTracking slice : slices) {
        if (slice.partitions.isEmpty() == false) {
            result.add(new IndexSearcher.LeafSlice(slice.partitions));
        }
    }
    return result.toArray(new IndexSearcher.LeafSlice[0]);
}
```

`GroupWithSegmentTracking` 数据结构：

```java
static class GroupWithSegmentTracking {
    final int index;
    long docCountSum;                          // 当前组的总文档数
    final Set<Integer> segmentOrdinals;        // 已包含的 segment 编号集合
    final List<LeafReaderContextPartition> partitions;  // partition 列表

    public boolean hasSegment(int segmentOrd) {
        return segmentOrdinals.contains(segmentOrd);  // O(1) 查找
    }

    public void addPartition(LeafReaderContextPartition partition, long docCount) {
        this.partitions.add(partition);
        this.segmentOrdinals.add(partition.ctx.ord);   // 记录 segment 编号
        this.docCountSum += docCount;
    }
}
```

**完整分配示例（Auto 模式）：**

```
输入：
  seg0(200万), seg1(80万), seg2(30万), seg3(10万)
  targetMaxSlice = 4, minSegmentSize = 50万
  maxDocsPerPartition = ceil(320万/4) = 80万

Step 1: 生成 partition
  seg0(200万) → 拆成3份: seg0-p0(66.6万), seg0-p1(66.6万), seg0-p2(66.8万)
  seg1(80万)  → 不拆:    seg1(80万)
  seg2(30万)  → 不拆:    seg2(30万)
  seg3(10万)  → 不拆:    seg3(10万)

Step 2: 按文档数降序排序
  [seg1(80万), seg0-p2(66.8万), seg0-p0(66.6万), seg0-p1(66.6万), seg2(30万), seg3(10万)]

Step 3: distributePartitions 分配（4个 slice）
  初始: Slice0=0, Slice1=0, Slice2=0, Slice3=0

  seg1(80万):
    找最小且没有 seg1 的 slice → Slice0(0)
    → Slice0={seg1(80万)}=80万

  seg0-p2(66.8万):
    找最小且没有 seg0 的 slice → Slice1(0)
    → Slice1={seg0-p2(66.8万)}=66.8万

  seg0-p0(66.6万):
    找最小且没有 seg0 的 slice → Slice2(0)  ← 不能选 Slice1（已有 seg0）
    → Slice2={seg0-p0(66.6万)}=66.6万

  seg0-p1(66.6万):
    找最小且没有 seg0 的 slice → Slice3(0)  ← 不能选 Slice1、Slice2
    → Slice3={seg0-p1(66.6万)}=66.6万

  seg2(30万):
    找最小且没有 seg2 的 slice → Slice2(66.6万) 或 Slice3(66.6万)
    → Slice2={seg0-p0(66.6万), seg2(30万)}=96.6万

  seg3(10万):
    找最小且没有 seg3 的 slice → Slice3(66.6万)
    → Slice3={seg0-p1(66.6万), seg3(10万)}=76.6万

最终结果：
  Slice 0: [seg1(80万)]                          = 80万
  Slice 1: [seg0 docs 133.3万~200万]             = 66.8万
  Slice 2: [seg0 docs 0~66.6万, seg2(30万)]      = 96.6万
  Slice 3: [seg0 docs 66.6万~133.3万, seg3(10万)] = 76.6万

负载比: 最大96.6万 / 最小66.8万 = 1.45x
```

**并发执行示意图：**

```
┌────────────────────────────────────────────────────────────────────┐
│ Lucene IndexSearcher 并发框架（通过 executor 自动调度）             │
│                                                                    │
│ 线程A (Slice 0):                                                   │
│   searchLeaf(seg1, 0, 80万)                                        │
│     → bulkScorer.score(collector, liveDocs, 0, 80万)               │
│                                                                    │
│ 线程B (Slice 1):                                                   │
│   searchLeaf(seg0, 133.3万, 200万)                                 │
│     → bulkScorer.score(collector, liveDocs, 133.3万, 200万)        │
│                                                                    │
│ 线程C (Slice 2):                                                   │
│   searchLeaf(seg0, 0, 66.6万)                                      │
│     → bulkScorer.score(collector, liveDocs, 0, 66.6万)             │
│   searchLeaf(seg2, 0, 30万)                                        │
│     → bulkScorer.score(collector, liveDocs, 0, 30万)               │
│                                                                    │
│ 线程D (Slice 3):                                                   │
│   searchLeaf(seg0, 66.6万, 133.3万)                                │
│     → bulkScorer.score(collector, liveDocs, 66.6万, 133.3万)       │
│   searchLeaf(seg3, 0, 10万)                                        │
│     → bulkScorer.score(collector, liveDocs, 0, 10万)               │
│                                                                    │
│ 注意：seg0 被拆成3个 partition，分别在线程B、C、D中并发搜索          │
│ 每个线程有自己的 Collector，互不干扰                                │
└────────────────────────────────────────────────────────────────────┘
```

### 3.5 段内并发为什么能工作？

段内并发的核心问题是：同一个 segment 被多个线程同时搜索，会不会有线程安全问题？

答案是不会，因为：

1. **每个 slice 有独立的 Collector**：Lucene 的并发框架为每个 slice 创建独立的 Collector（通过 CollectorManager 或 Lucene 内部机制），所以不同线程的收集器互不干扰。

2. **BulkScorer.score(min, max) 是无状态的**：`bulkScorer.score(collector, liveDocs, minDocId, maxDocId)` 只读取倒排索引数据，不修改任何共享状态。倒排索引是不可变的（immutable），天然线程安全。

3. **Doc values 迭代器是独立创建的**：每个 LeafCollector 在 `getLeafCollector(ctx)` 时会创建自己的 doc values 迭代器，不共享。

4. **同 segment 不同 slice 的约束**：`distributePartitions` 确保同一个 segment 的不同 partition 在不同 slice 里，避免同一个 Collector 处理同一个 segment 的多个范围。

### 3.6 流式聚合

OpenSearch 还有一个独特特性 — 流式聚合（Streaming Aggregation）：

```java
// searchLeaf 末尾
if (searchContext.isStreamSearch() && searchContext.getFlushMode() == FlushMode.PER_SEGMENT) {
    List<InternalAggregation> batch = searchContext.bucketCollectorProcessor().buildAggBatch(collector);
    if (!batch.isEmpty()) {
        sendBatch(batch);  // 每搜完一个 segment 就发送中间聚合结果
    }
}
```

每搜完一个 segment（或 partition），就可以把聚合的中间结果流式发回协调节点，不用等所有 segment 搜完。这对大数据量的聚合查询特别有用 — 协调节点可以边收边合并，减少内存压力和延迟。

### 3.7 OpenSearch 实现的特点总结

| 特性 | 说明 |
|------|------|
| Lucene 版本 | 10.3.1 |
| 并发调度方式 | 完全依赖 Lucene 原生框架（通过 executor） |
| 搜索入口 | search(Query, Collector) — 单 Collector，Lucene 内部处理并发 |
| 分组算法 | LPT 优先队列 + segment 去重约束 |
| 分组时机 | 构造时通过 slices() 计算 |
| 段内并发 | 支持（auto 模式 + force 模式） |
| 打分优化 | 自动 ConstantScoreQuery |
| 超时处理 | catch TimeExceededException，标记 searchTimedOut |
| 扩展性 | QueryPhaseSearcher 接口可插拔 |
| 时序优化 | 支持反向遍历 segment（升序查询优化） |
| 流式聚合 | 支持 per-segment 流式发送中间结果 |
| segment 跳过 | 有 search_after segment 级 canMatch 优化 |
---
## 第四章：三种实现对比分析

### 4.1 总体架构对比

```
ES7 自定义版:
  QueryPhase → searchConcurrent(query, collectorManager, scoreMode)
                → 自己 rewrite + createWeight
                → 自己 computeSlices（每次重算）
                → 自己 FutureTask + executor.execute
                → 自己 future.get() 逐个等
                → collectorManager.reduce()

ES 新版:
  QueryPhase → searcher.search(query, collectorManager)
                → 覆盖 IndexSearcher.search()
                → 构造时已算好 slices
                → Callable + TaskExecutor.invokeAll()
                → collectorManager.reduce()

OpenSearch:
  QueryPhase → queryPhaseSearcher.searchWith(...)  ← 可插拔
                → searcher.search(query, collector)  ← 单 Collector
                → Lucene IndexSearcher 内部并发调度
                → 支持 LeafReaderContextPartition（段内并发）
```

三者的核心差异在于"谁来调度并发"：
- ES7：完全自己管理（手动线程池）
- ES 新版：自己管理，但用了 Lucene 的 TaskExecutor
- OpenSearch：完全交给 Lucene 框架

### 4.2 分组算法对比

#### 4.2.1 算法差异

```
ES7 — 顺序填充法：
  大 segment 独占 → 小 segment 按顺序往组里塞 → 满了开新组 → 尾部堆积

ES 新版 — 贪心 + 优先队列法：
  所有 segment 按顺序往组里塞 → 满了开新组 → 尾部小 segment 用优先队列分给最小的组

OpenSearch — 纯 LPT 优先队列法：
  固定组数 → 所有 partition 按大小降序 → 每个都分给当前最小的组（带 segment 去重约束）
```

#### 4.2.2 同一输入的分组结果对比

```
输入：segments = [100万, 50万, 30万, 20万, 10万, 8万, 5万, 2万]
总文档数 = 225万，目标 slice 数 = 3

═══════════════════════════════════════════════════════════════

ES7（maxDocsPerSlice = 56.25万）：
  Slice 0: [100万]                          = 100万
  Slice 1: [50万, 30万]                     = 80万
  Slice 2: [20万, 10万, 8万, 5万, 2万]      = 45万

  负载比: 100万/45万 = 2.22x  ← 最不均衡

═══════════════════════════════════════════════════════════════

ES 新版（minDocsPerSlice = 56.25万）：
  贪心阶段：
    Slice A: [100万]         = 100万
    Slice B: [50万, 30万]    = 80万
    剩余: [20万, 10万, 8万, 5万, 2万]

  尾部优先队列分配：
    20ice B(80万)  → Slice B = 100万
    10万 → Slice A(100万) → Slice A = 110万
    8万  → Slice B(100万) → Slice B = 108万
    5万  → Slice B(108万) → Slice B = 113万
    2万  → Slice A(110万) → Slice A = 112万

  Slice 0: [100万, 10万, 2万]              = 112万
  Slice 1: [50万, 30万, 20万, 8万, 5万]    = 113万

  负载比: 113万/112万 = 1.01x  ← 非常均衡（但只有2个slice）

═══════════════════════════════════════════════════════════════

OpenSearch 整段模式（targetMaxSlice = 3）：
  按大小降序: [100万, 50万, 30万, 20万, 10万, 8万, 5万, 2万]

  100万 → Slice 0(最小=0)   → Slice0=100万
  50万  → Slice 1(最小=0)   → Slice1=50万
  30万  → Slice 2(最小=0)   → Slice2=30万
  20万  → Slice 2(最小=30万) → Slice2=50万
  10万  → Slice 1(最小=50万) → Slice1=60万
  8万   → Slice 2(最小=50万) → Slice2=58万
  5万   → Slice 2(最小=58万) → Slice2=63万
  2万   → Slice 1(最小=60万) → Slice1=62万

  Slice 0: [100万]                          = 100万
  Slice 1: [50万, 10万, 2万]               = 62万
  Slice 2: [30万, 20万, 8万, 5万]          = 63万

  负载比: 100万/62万 = 1.61x  ← 中等均衡

═══════════════════════════════════════════════════════════════

OpenSearch 段内并发 Auto 模式（targetMaxSlice = 3, minSegmentSize = 50万）：
  maxDocsPerPartition = ceil(225万/3) = 75万

  100万 > 75万 且 >= 50万 → 拆成2份: seg0-p0(50万), seg0-p1(50万)
  其余不拆

  partitions 降序: [seg0-p0(50万), seg0-p1(50万), 50万, 30万, 20万, 10万, 8万, 5万, 2万]

  分配（带 segment 去重）：
  seg0-p0(50万) → Slice 0(0)                → Slice0=50万
  seg0-p1(50万) → Slice 1(0, 不能选Slice0)  → Slice1=50万
  50万          → Slice 2(0)                 → Slice2=50万
  30万          → Slice 0(50万)              → Slice0=80万
  20万          → Slice 1(50万)              → Slice1=70万
  10万          → Slice 2(50万)              → Slice2=60万
  8万           → Slice 2(60万)              → Slice2=68万
  5万           → Slice 2(68万)              → Slice2=73万
  2万           → Slice 1(70万)              → Slice1=72万

  Slice 0: [seg0 docs 0~50万, 30万]         = 80万
  Slice 1: [seg0 docs 50万~100万, 20万, 2万] = 72万
  Slice 2: [50万, 10万, 8万, 5万]           = 73万

  负载比: 80万/72万 = 1.11x  ← 非常均衡！
```

#### 4.2.3 分组算法总结

| 算法 | 均衡性 | 复杂度 | 段内拆分 |
|------|--------|--------|---------|
| ES7 顺序填充 | 差（尾部堆积） | O(N log N) | 不支持 |
| ES 新版 贪心+优先队列 | 好（尾部均衡分配） | O(N log N) | 不支持 |
| OpenSearch LPT+去重 | 好（全局均衡） | O(N × S)，S=slice数 | 支持 |
| OpenSearch LPT+去重+段内 | 最好（大segment也能均衡） | O(N × S) | 支持 |

### 4.3 并发调度机制对比

```
                    ES7              ES新版            OpenSearch
                    ─────────────    ─────────────     ─────────────
调度方式            手动 FutureTask   Callable+invokeAll  Lucene 原生框架
线程池              SEARCH 线程池     TaskExecutor        IndexSearcher executor
任务提交            executor.execute  invokeAll 批量提交   Lucene 内部管理
结果等待            future.get()逐个  invokeAll 统一等待   Lucene 内部管理
异常处理            不取消其他任务     取消其他任务         Lucene 内部处理
Collector 模型      CollectorManager  CollectorManager    单 Collector（Lucene内部分发）
```

**异常传播差异详解：**

```
ES7 — 异常不传播：
  线程A: 正常执行中...
  线程B: 抛出 IOException!
  线程C: 正常执行中...（不知道B出错了，继续浪费资源）
  主线程: future0.get() → 等待A完成 → future1.get() → 发现B的异常 → 抛出
  问题: A和C可能已经执行完了，浪费了计算资源

ES 新版 — invokeAll 异常取消：
  线程A: 正常执行中...
  线程B: 抛出 IOException!
  → invokeAll 检测到异常 → 取消线程A和C的任务
  → 立即抛出异常
  优势: 快速失败，不浪费资源

OpenSearch — Lucene 框架处理：
  由 Lucene IndexSearcher 内部的 TaskExecutor 管理
  行为类似 ES 新版的 invokeAll
```

### 4.4 超时与聚合处理对比

```
                    ES7              ES新版            OpenSearch
                    ─────────────    ─────────────     ─────────────
超时检查注册        addQueryCancellation  addQueryCancellation  addQueryCancellation
超时后聚合收尾      ✗ 不执行          ✓ 执行（临时禁用超时）  ✓ 执行
超时禁用机制        无                timeoutOverwrites     无（在 slice 级别处理）
聚合后处理位置      无                search(leaves)的finally  search(partitions)末尾
流式聚合            ✗                 ✗                     ✓ per-segment 流式发送
```

**ES7 的问题：**
```java
// ES7: 超时后直接结束，聚合 postCollection 没有执行
catch (QueryPhase.TimeExceededException e) {
    timeExceeded = true;
    // 没有 doAggregationPostCollection()！
}
// 后果：聚合结果可能不完整或状态不一致
```

**ES 新版的改进：**
```java
// ES新版: 超时后仍然执行聚合收尾
} finally {
    if (success || timeExceeded) {
        timeoutOverwrites.set(true);       // 临时禁用超时
        doAggregationPostCollection(collector);  // 确保聚合正确收尾
        timeoutOverwrites.set(false);
    }
}
```

### 4.5 搜索入口与扩展性对比

```
                    ES7              ES新版            OpenSearch
                    ─────────────    ─────────────     ─────────────
搜索入口            searchConcurrent()  search(Q, CM)     search(Q, C)
                    （独立方法）       （覆盖父类方法）    （覆盖父类方法）
调用方感知          需要显式调用       透明，自动并发      透明，自动并发
扩展点              无                 无                  QueryPhaseSearcher 接口
插件可替换搜索逻辑  ✗                  ✗                   ✓
```

OpenSearch 的 `QueryPhaseSearcher` 接口允许插件完全替换搜索逻辑，比如：
- 向量搜索插件可以注入 ANN（近似最近邻）搜索
- 自定义排序插件可以注入特殊的排序逻辑
- 安全插件可以注入文档级过滤

### 4.6 段内并发能力对比

```
                    ES7              ES新版            OpenSearch
                    ─────────────    ─────────────     ─────────────
Lucene 版本         8.x              9.12.2            10.3.1
LeafReaderContext   ✓                ✓                 ✓
  Partition
段内拆分 API        ✗（Lucene 8无）   ✗（Lucene 9无）    ✓（Lucene 10 提供）
searchLeaf 签名     (ctx, w, c)      (ctx, w, c)       (ctx, min, max, w, c)
BulkScorer.score    (c, liveDocs)    (c, liveDocs)     (c, liveDocs, min, max)
段内并发实现        ✗                 ✗                  ✓（auto + force 两种策略）
segment 去重约束    不需要            不需要             ✓（GroupWithSegmentTracking）
```

**段内并发的价值：**

```
场景：一个分片只有 1 个巨大的 segment（500万文档），targetMaxSlice = 4

没有段内并发（ES7 / ES新版）：
  只能用 1 个线程搜索这个 segment
  Slice 0: [seg0(500万)]  ← 单线程，无法并发
  其他 3 个线程空闲

有段内并发（OpenSearch）：
  seg0 拆成 4 个 partition
  Slice 0: [seg0 docs 0~125万]
  Slice 1: [seg0 docs 125万~250万]
  Slice 2: [seg0 docs 250万~375万]
  Slice 3: [seg0 docs 375万~500万]
  4 个线程并发搜索，理论加速 ~4x
```

### 4.7 其他特性对比

#### 4.7.1 search_after segment 跳过

```
                    ES7              ES新版            OpenSearch
                    ─────────────    ─────────────     ─────────────
segment 级 canMatch ✓                ✗                 ✓
实现位置            searchLeaf 开头   无                searchLeaf 开头
原理                根据 segment 的   —                 根据 segment 的
                    min/max 统计信息                    min/max 统计信息
                    判断是否可跳过                      判断是否可跳过
```

#### 4.7.2 时序数据优化

```
                    ES7              ES新版            OpenSearch
                    ─────────────    ─────────────     ─────────────
反向遍历 segment    ✗                ✗                 ✓
适用场景            —                —                 时序数据升序查询
```

#### 4.7.3 打分优化

```
                    ES7              ES新版            OpenSearch
                    ─────────────    ─────────────     ─────────────
自动 ConstantScore  ✗（外部控制）     ✓（自动判断）      ✓（自动判断）
Query 优化
```

### 4.8 综合对比表

| 维度 | ES7 自定义 | ES 新版 | OpenSearch 3.4 |
|------|-----------|---------|----------------|
| Lucene 版本 | 8.x | 9.12.2 | 10.3.1 |
| 并发调度 | 手动 FutureTask | TaskExecutor.invokeAll | Lucene 原生框架 |
| 分组算法 | 顺序填充（不均衡） | 贪心+优先队列（均衡） | LPT+去重（最均衡） |
| 分组时机 | 每次搜索重算 | 构造时一次性 | 构造时一次性 |
| 段内并发 | ✗ | ✗ | ✓（auto/force） |
| 异常传播 | 不取消其他任务 | 取消其他任务 | Lucene 内部处理 |
| 超时后聚合 | ✗ 不执行 | ✓ 执行 | ✓ 执行 |
| 打分优化 | ✗ 外部控制 | ✓ 自动 | ✓ 自动 |
| 扩展性 | ✗ | ✗ | ✓ QueryPhaseSearcher |
| segment 跳过 | ✓ canMatch | ✗ | ✓ canMatch |
| 时序优化 | ✗ | ✗ | ✓ 反向遍历 |
| 流式聚合 | ✗ | ✗ | ✓ per-segment |
| 代码侵入性 | 高（独立方法） | 低（覆盖父类） | 低（覆盖父类） |

### 4.9 总结

三种实现代表了并发搜索的三个演进阶段：

**ES7 自定义版**是最早期的实现，在 Lucene 8.x 没有原生并发搜索支持的情况下，通过手动管理线程池和 FutureTask 实现了基本的并发能力。分组算法简单但不够均衡，异常处理和超时处理都有不足。但它的 search_after segment 跳过优化是一个亮点。

**ES 新版**利用了 Lucene 9.x 的 TaskExecutor 框架，将并发搜索内置到 `ContextIndexSearcher` 中，调用方无需感知。分组算法通过优先队列实现了更好的负载均衡，超时处理也更完善（聚合后处理不会被跳过）。但不支持段内并发，当 segment 数量少但单个很大时，并发度受限。

**OpenSearch 3.4**走得最远，完全依赖 Lucene 10.x 的原生并发框架，并实现了段内并发。通过 `LeafReaderContextPartition` 把大 segment 按 docId 范围拆分，配合带 segment 去重约束的 LPT 分配算法，实现了最优的负载均衡。此外还有 QueryPhaseSearcher 扩展点、流式聚合、时序数据优化等独特特性。

从架构演进的角度看，趋势是：**自己管理并发 → 利用 Lucene 框架 → 完全交给 Lucene + 段内并发**。每一步都在减少自定义代码量，增加对 Lucene 原生能力的利用。
