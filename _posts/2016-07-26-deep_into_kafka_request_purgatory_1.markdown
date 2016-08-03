---
layout: post
title:  "深入理解Kafka设计：RequestPurgatory（1）"
date:   2016-07-27
categories: deep_into_kafka
tags: Kafka
---
* 1
{:toc}

```
版权声明：本文为博主原创文章，未经博主允许不得转载。
```

### 摘要
Kafka Broker有一个关键的数据结构叫做RequestPurgatory（purgatory，炼狱？暂时受苦的地方？从名字上看有点陌生），其实它是Kafka实现Leader与Follower之间数据同步的关键，它保存了那些暂时还没有满足条件和还没有出错的延迟请求，例如：

1. acks=all的Produce请求，只有当in-sync的follower都跟上了这个请求，那么才算满足条件。
2. min.bytes=1的Fetch请求，只有当consumer获得了至少1bytes的消息才算满足条件，这使得consumer不必一直等待新的数据。

本文将探索RequestPurgatory是如何保存成千上万的请求及检测条件的（基于0.8.2版本）。

### 类设计介绍

通过RequestPurgatory的类图我们可以更清楚的看到它的设计结构，见下图：

![RequestPurgatory类图](/public/img/deep_into_kafka_request_purgatory_1_1.png)

从图中我们可以看到，RequestPurgatory的功能实现其实是依托了两个重要的组件：Watchers和ExpiredRequestReaper，并且与DelayedRequest密切相关。

#### DelayedRequest

延迟请求，即那些条件不能立即满足，需要等待至少delayMs时间的请求。

它的成员属性除了时间相关的createdMs和DelayMs而外，还包括satisfied标志位（表示该延迟请求已经条件满足或者已超时），原始的Request对象（保存Request目的是为了根据它构造出相应的Response对象）以及keys（就是topic-partition的数组，这个Request所涉及的所有topic-partition）。DelayedProduce以及DelayFetch是DelayedRequest的子类，各自定义了满足条件和相关属性，我们后面再详细介绍DelayedProduce的代码。

#### Watchers

RequsetPurgatory.watchersForKey其实是一个Map<Any,Wathcers>，key一般就是topic-partition对，value就是Watchers对象，每个新key都会对应新的Watchers。

Watchers作为条件监视器，内部由一个双向链表来保存DelayedRequest（频繁的添加删除，所以考虑链表实现），并监听所有DelayedRequest是否满足条件。Wathcers并非自驱动地做条件检测，而是要由外部驱动的(业务处理线程)。来看看其重要方法：

1. watched：返回监听的DelayedRequest的数目，即监听链表的长度。
2. addIfNotSatisfied：如果DelayedRequest尚未满足条件，将其加入到监听链表。
3. purgeSatisfied：遍历监听链表，删除已经满足条件的DelayedRequest。
4. collectSatisfiedRequests：遍历监听链表，驱动每一个DelayedRequest检查自己是否条件已满足，从监听列表中删除并返回已满足条件的DelayedRequest。


#### ExpiredRequestReaper

如果只依赖Watchers来检查并删除已满足条件的DelayedRequest，那么有可能在下一次检查前会有很多DelayedRequest已满足条件，这批请求如果不及时回收，会有撑爆内存的危险。因此设计了ExpiredRequestReaper线程，定期（默认当已满足条件的请求量超过1000时）执行（purgeSatisfied方法）两个任务：

1. 触发Watchers删除条件满足的请求
2. 清理内部DelayQueue保存的超时请求

ExpiredRequestReaper是一个独立的线程，在RequestPurgatory初始化时就启动了，其内部由一个DelayQueue（按delayMs排序的最小堆）来保存所有的DelayedRequest，经常需要获取最近会过期的请求。
其重要方法如下：

1. enqueue：将DelayedRequest放入DelayQueue，以检查其超时。
2. delayed：获取当前已超时的请求的数目。
3. pollExpired：获取下一条超时请求并设置其已满足条件。
4. purgeSatisfied：两个任务，上面已经说明。

#### RequestPurgatory

经过以上的介绍，那么RequestPurgatory就不这么难理解了，它就是一个帮助类（组合了Watchers和ExpriedRequestReaper），用来处理带超时的异步请求。

RequestPurgatory是一个抽象类，其子类有ProduceRequestPurgatory和FetchRequestPurgatory，需要实现checkSatisfied和exprie两个抽象方法。以下介绍一些重要方法：

1. isSatisfiedByMe(DelayedRequest)：CAS操作将satisfied标志置为true
2. checkAndMaybeWatch：核心逻辑，先检查请求是否满足条件，否则再加入到监听链表并检测超时。
3. update(key)：驱动相应的Watchers检查所有请求是否满足条件并从监听链表删除。除了checkAndMaybeWatch，这就是唯一能驱动DelayedRequest改变状态的入口了，该入口会被ReplicaManager来调用。
4. watchersFor(key)：获取相应key的Watchers对象，如果没有则为key生成新的Wathcers并返回。
5. checkSatisfied(DelayedRequest)：抽象方法，检查请求是否已满足条件。
6. expire(DelayedRequest)：抽象方法，触发DelayedRequest的超时处理。

### 总结

这一节先从总体上先介绍了RequestPurgatory的类结构的设计，及其每个重要组件的功能，下一节再从数据同步这个功能出发，介绍RequestPurgatroy如何处理带超时的异步请求。

### 参考文档
1. [Apache Kafka, Purgatory, and Hierarchical Timing Wheels](http://www.confluent.io/blog/apache-kafka-purgatory-hierarchical-timing-wheels)