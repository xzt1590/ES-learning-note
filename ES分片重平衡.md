# ES 分片分配完整调用链

## 完整分配流程

```
┌─────────────────────────────────────────────────────────────────┐
│ 【ES 启动】                                                      │
│ Node.start()                                                    │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ 【第1步：创建决策器】                                            │
│ ClusterModule.createAllocationDeciders()                        │
│   ├── new MaxRetryAllocationDecider()                          │
│   ├── new DiskThresholdDecider()                               │
│   ├── new AwarenessAllocationDecider()  ← 你的决策器           │
│   └── ... 更多决策器                                            │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ 【第2步：包装成集合】                                            │
│ new AllocationDeciders(deciderList)                             │
│   └── 存储所有决策器到数组                                       │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ 【第3步：传递给服务】                                            │
│ new AllocationService(allocationDeciders, ...)                  │
│   └── AllocationService 存储决策器集合                          │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ 【触发分配】                                                     │
│ 节点加入/索引创建/分片故障等                                     │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ 【第4步：创建分配上下文】                                        │
│ AllocationService.createRoutingAllocation()                     │
│   └── new RoutingAllocation(allocationDeciders, ...)           │
│       └── RoutingAllocation 存储决策器集合                       │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ 【第5步：分配器使用】                                            │
│ BalancedShardsAllocator.allocate(allocation)                   │
│   └── allocation.deciders()  ← 获取决策器集合                   │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ 【第6步：遍历决策器】                                            │
│ AllocationDeciders.canAllocate(shard, node, allocation)         │
│   └── for (AllocationDecider decider : deciders) {             │
│       └── decider.canAllocate(...)  ← 调用每个决策器            │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│ 【第7步：你的决策器执行】                                        │
│ AwarenessAllocationDecider.canAllocate()  ← 你的修改             │
│   └── 检查：主分片是否在禁止区域？                                │
│       ├── 是 → Decision.NO（拒绝）                                │
│       └── 否 → Decision.YES（继续）                             │
└─────────────────────────────────────────────────────────────────┘
```



## 触发位置

## 🚀 触发入口

分片分配/重平衡会在以下情况被触发：

### 1. **集群状态变更事件**

**触发条件**

- 新节点加入集群                             
- 节点离开集群（故障/下线）                          
- 新索引创建                                     
- 索引删除  
- 索引设置变更（副本数等）  
- 分片故障
- 手动调用 reroute API
- 磁盘空间不足
- 定时重平衡检查     

### 2. **具体触发位置（代码）**

| 触发场景 | 触发文件 | 调用方法 |
|---------|---------|---------|
| 节点加入 | `NodeJoinExecutor.java` | `rerouteService.reroute()` |
| 节点离开 | `NodeLeftExecutor.java` | `allocationService.disassociateDeadNodes()` |
| 创建索引 | `MetadataCreateIndexService.java` | `allocationService.reroute()` |
| 分片故障 | `ShardStateAction.java` | `rerouteService.reroute()` |
| 手动reroute | `TransportClusterRerouteAction.java` | `allocationService.reroute()` |
| 磁盘监控 | `DiskThresholdMonitor.java` | `rerouteService.reroute()` |

```
🔄 完整调用链（自顶向下）

【第1层：触发层】
┌─────────────────────────────────────────────────────────────┐
│  各种事件触发                                                │
│  (节点加入/离开、索引创建、分片故障等)                        │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
【第2层：批量服务层】
┌─────────────────────────────────────────────────────────────┐
│  BatchedRerouteService.reroute()                            │
│  • 批量合并多个reroute请求                                     │
│  • 避免频繁触发                                               │
│  • 提交到MasterService的任务队列                               │ 
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
【第3层：集群状态更新层】这里仅仅是提交任务的一个任务调度器，执行任务的逻辑在提交的任务本身中
┌─────────────────────────────────────────────────────────────┐
│  MasterService.submitUnbatchedStateUpdateTask()             │
│  • 主节点执行                                                │
│  • 串行化处理（保证一致性）                                   │
│  • 执行 ClusterStateUpdateTask                               │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
【第4层：分配服务入口】
┌─────────────────────────────────────────────────────────────┐
│  AllocationService.reroute()                                 │
│  • 创建 RoutingAllocation（分配上下文）                      │
│  • 调用 executeWithRoutingAllocation()                       │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
【第5层：核心分配逻辑】
┌─────────────────────────────────────────────────────────────┐
│  AllocationService.reroute(RoutingAllocation, RerouteStrategy)│
│  ┌───────────────────────────────────────────────────────┐  │
│  │  步骤1: allocateExistingUnassignedShards()            │  │
│  │  • 分配未分配的分片（优先分配已有副本的分片）            │  │
│  │  • 先分配主分片，再分配副本                              │  │
│  └───────────────────┬───────────────────────────────────┘  │
│                      │                                       │
│                      ▼                                       │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  步骤2: rerouteStrategy.execute()                      │  │
│  │  • 执行重平衡逻辑                                        │  │
│  │  • 调用 ShardsAllocator.allocate()                     │  │
│  └───────────────────┬───────────────────────────────────┘  │
└───────────────────────┼───────────────────────────────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
【第6层A：未分配分片分配】    【第6层B：重平衡分配】
┌──────────────────────────┐  ┌──────────────────────────┐
│ ExistingShardsAllocator  │  │ ShardsAllocator          │
│ .allocateUnassigned()    │  │ .allocate()               │
│                          │  │                          │
│ 遍历未分配的分片：         │  │ 计算分片移动：            │
│ • 主分片                  │  │ • 找出不平衡的分片         │
│ • 副本                   │  │ • 计算目标节点             │
│                          │  │ • 执行移动                │
└───────────┬─────────────┘  └───────────┬──────────────┘
            │                             │
            └───────────────┬─────────────┘
                            │
                            ▼
【第7层：决策器检查层】
┌─────────────────────────────────────────────────────────────┐
│  。                           │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  遍历所有决策器，逐一检查：                             │  │
│  │                                                         │  │
│  │  1. AwarenessAllocationDecider.canAllocate()│  │
│  │     • 检查区域感知（你的核心逻辑）                       │  │
│  │     • 检查主分片是否在禁止区域                           │  │
│  │                                                         │  │
│  │  2. DiskThresholdDecider.canAllocate()                 │  │
│  │     • 检查磁盘空间                                      │  │
│  │                                                         │  │
│  │  3. ThrottlingAllocationDecider.canAllocate()          │  │
│  │     • 检查限流                                          │  │
│  │                                                         │  │
│  │  4. ... 其他决策器                                      │  │
│  │                                                         │  │
│  │  所有决策器都返回 YES → 允许分配                         │  │
│  │  任何一个返回 NO → 拒绝分配                              │  │
│  └───────────────────────────────────────────────────────┘  │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
【第8层：路由节点操作层】
┌─────────────────────────────────────────────────────────────┐
│  RoutingNodes 操作                                           │
│  • 分配分片到节点                                            │
│  • 移动分片                                                  │
│  • 取消分配                                                  │
│  • 主从切换（你的修改：activePromotableReplicaWithHighest...）│
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
【第9层：构建新集群状态】
┌─────────────────────────────────────────────────────────────┐
│  AllocationService.buildResultAndLogHealthChange()           │
│  • 基于 RoutingAllocation 构建新的 ClusterState              │
│  • 更新路由表                                                │
│  • 发布到所有节点                                             │
└─────────────────────────────────────────────────────────────┘
```

## 详细代码解析

### 任务触发并分配任务处

`rerouteService.reroute()`/`BatchedRerouteService.reroute()`

```java
    public final void reroute(String reason, Priority priority, ActionListener<Void> listener) {
     	 // 上下文包装，异步任务完成后，能够恢复当前请求线程的上下文
        final ActionListener<Void> wrappedListener = ContextPreservingActionListener.wrapPreservingContext(
            listener,
            clusterService.getClusterApplierService().threadPool().getThreadContext()
        );
        final List<ActionListener<Void>> currentListeners;
        synchronized (mutex) {
          	// 如果已经有任务在等待
            if (pendingRerouteListeners != null) {
              	// 新请求的优先级不高于当前任务，直接加到任务列表中，随着当前任务一起被处理
                if (priority.sameOrAfter(pendingTaskPriority)) {
                    logger.trace(
                        "already has pending reroute at priority [{}], adding [{}] with priority [{}] to batch",
                        pendingTaskPriority,
                        reason,
                        priority
                    );
                    pendingRerouteListeners.add(wrappedListener);
                    return;
                } else { // 如果新请求优先级更高，马上处理
                    logger.trace(
                        "already has pending reroute at priority [{}], promoting batch to [{}] and adding [{}]",
                        pendingTaskPriority,
                        priority,
                        reason
                    );
                  	// 包含新旧所有请求的监听器
                    currentListeners = new ArrayList<>(1 + pendingRerouteListeners.size());
                    currentListeners.add(wrappedListener);
                    currentListeners.addAll(pendingRerouteListeners);
                  	// 清空旧任务所有列表
                    pendingRerouteListeners.clear();
                  	// 指向新列表
                    pendingRerouteListeners = currentListeners;
                  	// 提高优先级
                    pendingTaskPriority = priority;
                }
            } else { // 如果没有任务在排队，初始化列表和优先级，提交第一个任务
                logger.trace("no pending reroute, scheduling reroute [{}] at priority [{}]", reason, priority);
                currentListeners = new ArrayList<>(1);
                currentListeners.add(wrappedListener);
                pendingRerouteListeners = currentListeners;
                pendingTaskPriority = priority;
            }
        }
        try {
            var future = new ListenableFuture<Void>();
            final String source = CLUSTER_UPDATE_TASK_SOURCE + "(" + reason + ")";
          	// 将任务提交给集群状态更新线程，真正执行更新的地方
            submitUnbatchedTask(source, new ClusterStateUpdateTask(priority) {

                @Override
              	//	当任务被调度执行时
                public ClusterState execute(ClusterState currentState) {
                    final boolean currentListenersArePending;
                    synchronized (mutex) {
                        assert currentListeners.isEmpty() == (pendingRerouteListeners != currentListeners)
                            : "currentListeners=" + currentListeners + ", pendingRerouteListeners=" + pendingRerouteListeners;
                        currentListenersArePending = pendingRerouteListeners == currentListeners;
                      	// 执行前判断是否还指向当前任务持有的currentListeners
                        if (currentListenersArePending) {
                          	// 说明没有高优先级任务插队，置为null表示这一批次开始处理，后续请求需要开启新的批次
                            pendingRerouteListeners = null;
                        }
                    }
                  	// 有效任务，开始执行
                    if (currentListenersArePending) {
                        logger.trace("performing batched reroute [{}]", reason);
                        return reroute.reroute(currentState, reason, future);
                    } else { // 失效任务，说明发生了优先级升级
                        logger.trace("batched reroute [{}] was promoted", reason);
                        // reroute was batched and completed in other branch
                        future.onResponse(null);
                        return currentState;
                    }
                }

                @Override
              	// 失败处理回调接口
                public void onFailure(Exception e) {
                    synchronized (mutex) {
                        if (pendingRerouteListeners == currentListeners) {
                            pendingRerouteListeners = null;
                        }
                    }
                    final ClusterState state = clusterService.state();
                    if (MasterService.isPublishFailureException(e)) {
                        logger.debug(() -> format("unexpected failure during [%s], current state:\n%s", source, state), e);
                        // no big deal, the new master will reroute again
                    } else if (logger.isTraceEnabled()) {
                        logger.error(() -> format("unexpected failure during [%s], current state:\n%s", source, state), e);
                    } else {
                        logger.error(
                            () -> format("unexpected failure during [%s], current state version [%s]", source, state.version()),
                            e
                        );
                    }
                    ActionListener.onFailure(currentListeners, e);
                }

                @Override
              	// 集群状态更新成功后，触发所有监听器的onResponse，这一批次里所有请求者都会同时收到完成通知
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    future.addListener(ActionListener.running(() -> ActionListener.onResponse(currentListeners, null)));
                }
            });
        } catch (Exception e) {
            synchronized (mutex) {
                assert currentListeners.isEmpty() == (pendingRerouteListeners != currentListeners);
                if (pendingRerouteListeners == currentListeners) {
                    pendingRerouteListeners = null;
                }
            }
            ClusterState state = clusterService.state();
            logger.warn(() -> "failed to reroute routing table, current state:\n" + state, e);
            ActionListener.onFailure(
                currentListeners,
                new ElasticsearchException("delayed reroute [" + reason + "] could not be submitted", e)
            );
        }
    }
```

### 执行重分片的入口处

```java
    // 这里优先处理未分配的分片，然后才重新进行分片的均衡逻辑
		private void reroute(RoutingAllocation allocation, RerouteStrategy rerouteStrategy) {
        assert hasDeadNodes(allocation) == false : "dead nodes should be explicitly cleaned up. See disassociateDeadNodes";
        assert AutoExpandReplicas.getAutoExpandReplicaChanges(allocation.metadata(), () -> allocation).isEmpty()
            : "auto-expand replicas out of sync with number of nodes in the cluster";
        assert assertInitialized();
      	// 1. 处理延迟分配（Delayed Allocation）
   		  // 如果节点重新加入，移除之前的延迟标记
        rerouteStrategy.removeDelayMarkers(allocation);
      	// 2. 分配已存在的未分配分片（GatewayAllocator）
    		// 这一步非常关键：它优先尝试将分片分配回它原本所在的节点（利用磁盘上现有的数据）。
   		  // 这通常由 GatewayAllocator 处理，用于从磁盘恢复数据。
        allocateExistingUnassignedShards(allocation); // try to allocate existing shard copies first
      	// 执行传入的策略（Strategy Hook）
        rerouteStrategy.execute(allocation);
        assert RoutingNodes.assertShardStats(allocation.routingNodes());
    }
```

### BalancedShardsAllocator

- **allocate**

```java
@Override
public void allocate(RoutingAllocation allocation) {
    // ... 前置检查 ...
    
    // 创建 Balancer 对象，它是实际干活的内部类
    final Balancer balancer = new Balancer(writeLoadForecaster, allocation, weightFunction, threshold);
    // 第一步：尝试分配那些还没着落的分片（Unassigned Shards）
    balancer.allocateUnassigned();
    // 第二步：移动分片（处理 Move 指令等）
    balancer.moveShards();
    // 第三步：全局平衡（Rebalance，把分片从忙碌节点移到空闲节点）
    balancer.balance();
}
```

- **allocateUnassigned**

```java
private void allocateUnassigned() {
  	// 获取所有未分配分片
    RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
    assert nodes.isEmpty() == false;
    if (logger.isTraceEnabled()) {
        logger.trace("Start allocating unassigned shards");
    }
    if (unassigned.isEmpty()) {
        return;
    }

    // 排序逻辑 (主分片优先，然后按索引名和ID排序)
    final PriorityComparator secondaryComparator = PriorityComparator.getAllocationComparator(allocation);
    final Comparator<ShardRouting> comparator = (o1, o2) -> {
        if (o1.primary() ^ o2.primary()) {
            return o1.primary() ? -1 : 1;
        }
        if (o1.getIndexName().compareTo(o2.getIndexName()) == 0) {
            return o1.getId() - o2.getId();
        }
        final int secondary = secondaryComparator.compare(o1, o2);
        assert secondary != 0 : "Index names are equal, should be returned early.";
        return secondary;
    };
    ShardRouting[] primary = unassigned.drain();
    ShardRouting[] secondary = new ShardRouting[primary.length];
    int secondaryLength = 0;
    int primaryLength = primary.length;
    ArrayUtil.timSort(primary, comparator);
  
  	// 循环处理每个分片
    do {
        for (int i = 0; i < primaryLength; i++) {
            ShardRouting shard = primary[i];
          	//【关键调用】决定这个分片该去哪个节点
            final AllocateUnassignedDecision allocationDecision = decideAllocateUnassigned(shard);
            final String assignedNodeId = allocationDecision.getTargetNode() != null
                ? allocationDecision.getTargetNode().getId()
                : null;
            final ModelNode minNode = assignedNodeId != null ? nodes.get(assignedNodeId) : null;

          	// 如果决定是 YES，就执行分配
            if (allocationDecision.getAllocationDecision() == AllocationDecision.YES) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Assigned shard [{}] to [{}]", shard, minNode.getNodeId());
                }

                final long shardSize = getExpectedShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE, allocation);
              	// 初始化分片，更新路由表
                shard = routingNodes.initializeShard(shard, minNode.getNodeId(), null, shardSize, allocation.changes());
                minNode.addShard(shard);
                if (shard.primary() == false) {
                    // copy over the same replica shards to the secondary array so they will get allocated
                    // in a subsequent iteration, allowing replicas of other shards to be allocated first
                    while (i < primaryLength - 1 && comparator.compare(primary[i], primary[i + 1]) == 0) {
                        secondary[secondaryLength++] = primary[++i];
                    }
                }
            } else {
              	// 如果没地方去，忽略它（记录原因）
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "No eligible node found to assign shard [{}] allocation_status [{}]",
                        shard,
                        allocationDecision.getAllocationStatus()
                    );
                }

                if (minNode != null) {
                    // throttle decision scenario
                    assert allocationDecision.getAllocationStatus() == AllocationStatus.DECIDERS_THROTTLED;
                    final long shardSize = getExpectedShardSize(shard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE, allocation);
                    minNode.addShard(shard.initialize(minNode.getNodeId(), null, shardSize));
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace("No Node found to assign shard [{}]", shard);
                    }
                }

                unassigned.ignoreShard(shard, allocationDecision.getAllocationStatus(), allocation.changes());
                if (shard.primary() == false) {
                    // we could not allocate it and we are a replica - check if we can ignore the other replicas
                    while (i < primaryLength - 1 && comparator.compare(primary[i], primary[i + 1]) == 0) {
                        unassigned.ignoreShard(primary[++i], allocationDecision.getAllocationStatus(), allocation.changes());
                    }
                }
            }
        }
        primaryLength = secondaryLength;
        ShardRouting[] tmp = primary;
        primary = secondary;
        secondary = tmp;
        secondaryLength = 0;
    } while (primaryLength > 0);
    // clear everything we have either added it or moved to ignoreUnassigned
}
```

- **decideAllocateUnassigned**

```java
private AllocateUnassignedDecision decideAllocateUnassigned(final ShardRouting shard) {
    if (shard.assignedToNode()) {
        // we only make decisions for unassigned shards here
        return AllocateUnassignedDecision.NOT_TAKEN;
    }

    final boolean explain = allocation.debugDecision();
    // 1. 全局预判：问问 Deciders，这个分片是否被允许分配？（不考虑具体节点）
    Decision shardLevelDecision = allocation.deciders().canAllocate(shard, allocation);
    if (shardLevelDecision.type() == Type.NO && explain == false) {
        return AllocateUnassignedDecision.no(AllocationStatus.DECIDERS_NO, null);
    }

    float minWeight = Float.POSITIVE_INFINITY;
    ModelNode minNode = null;
    Decision decision = null;
    /* 不要在这里遍历 identity hashset，因为每次运行时遍历的顺序都不同，这会导致测试变得困难。*/
    Map<String, NodeAllocationResult> nodeExplanationMap = explain ? new HashMap<>() : null;
    List<Tuple<String, Float>> nodeWeights = explain ? new ArrayList<>() : null;
  	// 2. 遍历所有节点，寻找最佳候选者
    for (ModelNode node : nodes.values()) {
        if (node.containsShard(shard) && explain == false) {
            // decision is NO without needing to check anything further, so short circuit
            continue;
        }

        // 计算如果放在这个节点，集群的平衡性得分（权重
        float currentWeight = weight.weight(this, node, shard.getIndexName());
        // moving the shard would not improve the balance, and we are not in explain mode, so short circuit
        if (currentWeight > minWeight && explain == false) {
            continue;
        }

        // 【核心调用点】问问 Deciders：这个特定的节点能放这个分片吗？
        Decision currentDecision = allocation.deciders().canAllocate(shard, node.getRoutingNode(), allocation);
        if (explain) {
            nodeExplanationMap.put(node.getNodeId(), new NodeAllocationResult(node.getRoutingNode().node(), currentDecision, 0));
            nodeWeights.add(Tuple.tuple(node.getNodeId(), currentWeight));
        }
        // 3. 择优录取：
        // 如果 Deciders 说 YES 或 THROTTLE，并且这个节点的权重更优（能让集群更平衡）
        if (currentDecision.type() == Type.YES || currentDecision.type() == Type.THROTTLE) {
            final boolean updateMinNode;
            if (currentWeight == minWeight) {
                /* 我们有一个相等权重的平局打破规则：
                * 1. 如果一个决策是“是”，优先选择它。
                * 2. 优先选择持有该索引的主分片、且在环中下一个 ID 的节点。
                * 对于 3 个分片 2 个副本的情况，我们尝试构建：
                * 1 2 0
                * 2 0 1
                * 0 1 2
                * 当我们需要打破平局时，我们尽量优先选择持有 ID 最小且大于我们需要分配的分片 ID 的节点。
                * 这个方法对于新索引创建时效果很好，因为主分片会首先添加，并且在这个算法中我们只会一次添加一个分片集。
                */
                if (currentDecision.type() == decision.type()) {
                    final int repId = shard.id();
                    final int nodeHigh = node.highestPrimary(shard.index().getName());
                    final int minNodeHigh = minNode.highestPrimary(shard.getIndexName());
                    updateMinNode = ((((nodeHigh > repId && minNodeHigh > repId) || (nodeHigh < repId && minNodeHigh < repId))
                        && (nodeHigh < minNodeHigh)) || (nodeHigh > repId && minNodeHigh < repId));
                } else {
                    updateMinNode = currentDecision.type() == Type.YES;
                }
            } else {
                updateMinNode = currentWeight < minWeight;
            }
            if (updateMinNode) {
                minNode = node;
                minWeight = currentWeight;
                decision = currentDecision;
            }
        }
    }
    if (decision == null) {
        // decision was not set and a node was not assigned, so treat it as a NO decision
        decision = Decision.NO;
    }
    List<NodeAllocationResult> nodeDecisions = null;
    if (explain) {
        nodeDecisions = new ArrayList<>();
        // fill in the correct weight ranking, once we've been through all nodes
        nodeWeights.sort((nodeWeight1, nodeWeight2) -> Float.compare(nodeWeight1.v2(), nodeWeight2.v2()));
        int weightRanking = 0;
        for (Tuple<String, Float> nodeWeight : nodeWeights) {
            NodeAllocationResult current = nodeExplanationMap.get(nodeWeight.v1());
            nodeDecisions.add(new NodeAllocationResult(current.getNode(), current.getCanAllocateDecision(), ++weightRanking));
        }
    }
  	// 返回最终决定
    return AllocateUnassignedDecision.fromDecision(decision, minNode != null ? minNode.routingNode.node() : null, nodeDecisions);
}
```