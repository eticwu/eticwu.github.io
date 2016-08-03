---
layout: post
title:  "深入理解Kafka设计：RequestPurgatory（2）"
date:   2016-07-29
categories: deep_into_kafka
tags: Kafka
---
* 1
{:toc}

```
版权声明：本文为博主原创文章，未经博主允许不得转载。
```

### 摘要

上一节 [深入理解Kafka设计：RequestPurgatory（1）](/deep_into_kafka/2016/07/27/deep_into_kafka_request_purgatory_1.html)，从类结构设计方面介绍了RequestPurgatory相关的几个主要组件类的功能，这一节将从源码层面来分析RequestPurgatory是如何处理延迟请求的。

### RequestPurgatory工作原理

Kafka的延迟请求分两种类型：一种是Produce延迟请求，需要等待请求涉及到的数据都被其他follower同步；另一种则是Fetch延迟请求，为了避免不断的轮询，Fetch请求会等待有足够的数据量才返回。下面以Produce延迟请求为例来分析RequestPurgatory的工作原理。

#### 处理DelayedRequest

<center>
<img src="/public/img/deep_into_kafka_request_purgatory_2_1.png" style="width:75%" />
图1.处理延迟ProduceRequest的时序图
</center>


KafkaRequestHandler处理请求的过程已经在[深入理解Kafka设计：高性能服务器模型（1）](/deep_into_kafka/2016/07/11/deep_into_kafka_nio_server_1.html)中介绍过。这里直接从KafkaApis处理发送请求的方法入手。

 `KafkaApis.handleProducerOrOffsetCommitRequest`用于接收Producer发送过来的新消息，然后将其持久化，并根据请求参数requiredAcks的值决定是否立即返回响应或将请求延迟处理。

{% highlight java %}
def handleProducerOrOffsetCommitRequest(request: RequestChannel.Request) {
	.....
	val localProduceResults = appendToLocalLog(produceRequest, offsetCommitRequestOpt.nonEmpty)//持久化消息数据
	.....
	if(produceRequest.requiredAcks == 0) {//producer不需要响应
	.....
	} else if (produceRequest.requiredAcks == 1 ||
        produceRequest.numPartitions <= 0 ||
        numPartitionsInError == produceRequest.numPartitions) {//producer只需要leader成功接受到消息即可
	.....
	} else {//producer需要所有follower成功同步到消息
      		//为这个延迟请求生成(topic, parition)列表，Wathcers会将列表中的元素用作key
      		val producerRequestKeys = produceRequest.data.keys.toSeq
      		val statuses = localProduceResults.map(r =>
        r.key -> DelayedProduceResponseStatus(r.end + 1, ProducerResponseStatus(r.errorCode, r.start))).toMap
      		val delayedRequest =  new DelayedProduce(
        		producerRequestKeys,
        		request,
        		produceRequest.ackTimeoutMs.toLong,
        		produceRequest,
        		statuses,
        		offsetCommitRequestOpt)//构建延迟请求
        
        	//延迟请求交给producerRequestPurgatory
      		val satisfiedByMe = producerRequestPurgatory.checkAndMaybeWatch(delayedRequest)
      		if (satisfiedByMe)
        		producerRequestPurgatory.respond(delayedRequest)
 	}
 	.....
 }
{% endhighlight %}

当requiredAcks>1时，会将原请求封装成延迟请求DelayedProduce，并交予producerRequestPurgatory来处理。如果该请求此时已经满足条件（即其他follower都已经成功同步了数据），那么会立即返回响应给producer。

`ProducerRequestPurgatory.checkAndMaybeWatch`是处理DelayedProduce的唯一入口，他会先检查请求是否满足条件，否则再延迟处理请求，并返回false。

{% highlight java %}
def checkAndMaybeWatch(delayedRequest: T): Boolean = {
	if (delayedRequest.keys.size <= 0)//如果请求不包含消息，那么已满足条件
      return isSatisfiedByMe(delayedRequest)

    var isSatisfied = delayedRequest synchronized checkSatisfied(delayedRequest)//首先检查请求是否已满足条件
    if (isSatisfied)
      return isSatisfiedByMe(delayedRequest)//更新isSatisied标志位，reaper线程会使用到

	//由于一个请求可能包含多个topic-partition的数据，每个topic－partition都需要做条件检查
    for(key <- delayedRequest.keys) {
      val lst = watchersFor(key)//为该topic－partition创建Watcher
      if (!lst.addIfNotSatisfied(delayedRequest)) {//如果request还没有满足条件，那么添加到该key对应的Watchers里面
        return false//如果已经满足条件，那么为了节约开销，不再检查余下的key
      }
    }

    isSatisfied = delayedRequest synchronized checkSatisfied(delayedRequest)//再一次检查请求是否满足条件
    if (isSatisfied)
      return isSatisfiedByMe(delayedRequest)
    else {
      //仍然不满足时，需要加入超时控制
      expiredRequestReaper.enqueue(delayedRequest)
      return false
    }
  }
{% endhighlight %}

一般来说，大部分请求很快便会满足条件，所以接连两次调用checkSatisfied，以减轻内存溢出风险。如果两次检查仍然没有满足条件，那么该请求也要控制超时时间了。

#### ExpiredRequestReaper的工作

ExpiredRequestReaper线程随Broker启动而启动，其目的是不断地检查超时请求，触发RequestPurgatory处理超时响应，并帮助其清理超时请求对象以缓解内存压力。

<center>
<img src="/public/img/deep_into_kafka_request_purgatory_2_2.png" style="width:75%" />
图2.ExpiredRequestReaper线程工作时序图
</center>

从`ExpiredRequestReaper.run`中的主循环可以看出该线程的主要逻辑：
{% highlight java%}
def run() {
      while(running.get) {
        try {
          val curr = pollExpired()//获取下一个超时请求
          if (curr != null) {
            curr synchronized {
              expire(curr)//触发RequestPurgatory执行请求超时的动作，具体行为由子类来实现
            }
          }
          //RequestPurgatory监听的请求个数超过1000时
          if (RequestPurgatory.this.watched() >= purgeInterval) {
            val numPurgedFromWatchers = watchersForKey.values.map(_.purgeSatisfied()).sum//清除已满足条件的请求
          }
          //当超时请求列表超过1000时
          if (delayed() >= purgeInterval) {
            val purged = purgeSatisfied()//清除已满足条件的请求
          }
        } catch {
        	.....
        }
      }
      .....
    }
    
    //获取下一个超时请求
    private def pollExpired(): T = {
      while(true) {
        val curr = delayedQueue.poll(200L, TimeUnit.MILLISECONDS)//获取第一个超时的对象
        if (curr == null)
          return null.asInstanceOf[T]
        val updated = curr.satisfied.compareAndSet(false, true)//更新statisfied标志位
        if(updated) {
          return curr
        }
      }
      throw new RuntimeException("This should not happen")
    }
{% endhighlight %}

不论是`ExpiredRequestReaper.purgeSatisfied`还是`Watchers.purgeSatisfied`，都是遍历他们的Request列表（数据结构不一样，ExpiredRequestReaper是DelayedQueue实现，另一个则是LinkedList），然后从中remove掉已经满足条件的Request对象，使得他们可以被GC回收掉，达到最终目的。

#### 释放DelayedRequest

<center>
<img src="/public/img/deep_into_kafka_request_purgatory_2_3.png" style="width:75%" />
图3.ExpiredRequestReaper线程工作时序图
</center>

上面提到DelayedProduce的创建、清理和超时控制，那么他的条件在什么时候会被满足呢？肯定是在数据同步以后了。KafkaApis.handleFetchRequest不仅是处理消费者拉取消息的逻辑，同时也是follower同步leader数据的逻辑入口，所以从这个方法入手来分析。

{% highlight java %}
def handleFetchRequest(request: RequestChannel.Request) {
    val fetchRequest = request.requestObj.asInstanceOf[FetchRequest]
    val dataRead = replicaManager.readMessageSets(fetchRequest)//根据topic,partition,offset,size等参数来读取相应的消息数据

    //如果是从follower过来的请求
    if(fetchRequest.isFromFollower)
      recordFollowerLogEndOffsets(fetchRequest.replicaId, dataRead.mapValues(_.offset))//那么更新内存中replica的相应offset
      
    .....
    if(fetchRequest.maxWait <= 0 ||//请求不需要等待
       fetchRequest.numPartitions <= 0 ||//请求不获取任何数据
       bytesReadable >= fetchRequest.minBytes ||//已经获取到足够数据
       errorReadingData) {//读取消息时出错
      .....
      //立即返回响应
      val response = new FetchResponse(fetchRequest.correlationId, dataRead.mapValues(_.data))
      requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(response)))
    } else {//这里是处理DelayedFetch的逻辑，不再介绍
      .....
    }
  }
  
  private def recordFollowerLogEndOffsets(replicaId: Int, offsets: Map[TopicAndPartition, LogOffsetMetadata]) {
    .....
      case (topicAndPartition, offset) =>
        //更新该replicaId在这个Leader中所有topic-partition的offset
replicaManager.updateReplicaLEOAndPartitionHW(topicAndPartition.topic,
          topicAndPartition.partition, replicaId, offset)
//此时要检查是否可以满足某些produce请求的条件了
replicaManager.unblockDelayedProduceRequests(topicAndPartition)
    }
  }
  
  def unblockDelayedProduceRequests(key: TopicAndPartition) {
    val satisfied = producerRequestPurgatory.update(key)//更新key所对应的DelayedProduce

    //为每一个满足了条件的请求返回响应
    satisfied.foreach(producerRequestPurgatory.respond(_))
  }
{% endhighlight %}

`unblockDelayedProduceRequests `是一个重要的方法，顾名思义，解锁Produce延迟请求。每一次follower成功获取到数据以后，除了要更新对应的offset外，最后还要解锁相应的Produce延迟请求。所以他调用了`RequestPurgatory.update`去更新topic-parition所对应的DelayedRequest状态。

{% highlight java %}
def update(key: Any): Seq[T] = {
    val w = watchersForKey.get(key)
    if(w == null)
      Seq.empty
    else
      w.collectSatisfiedRequests()
  }
{% endhighlight %}

`Watchers.collectSatisfiedRequests`会找到topic-parition所对应的Watchers对象，然后遍历其Request列表并触发条件检查，并返回已满足的请求。

{% highlight java %}
def collectSatisfiedRequests(): Seq[T] = {
      val response = new mutable.ArrayBuffer[T]
      synchronized {
        val iter = requests.iterator()
        while(iter.hasNext) {
          val curr = iter.next
          if(curr.satisfied.get) {
            iter.remove()//这里也会去除已满足条件的请求
          } else {
            val satisfied = curr synchronized checkSatisfied(curr)
            if(satisfied) {
              iter.remove()
              val updated = curr.satisfied.compareAndSet(false, true)
              if(updated == true) {
                response += curr
              }
            }
          }
        }
      }
      response
    }
{% endhighlight %}

由于ExpiredRequestReaper线程也会触发所有Watchers遍历Request列表的动作，所以对每个Watchers对象做了同步控制。另外在RequestPurgatory.checkAndMaybeWatch方法中，业务线程也会执行checkSatisfied操作，跟这里的遍历有极小的可能会操作同一个request对象，所以也加入了同步控制`curr synchronized checkSatisfied(curr)`。

而且对一个老的延迟请求来说，`Watchers.collectSatisfiedRequests`这里是他更新状态的唯一入口了。

`Wathcers.checkSatisfied`最终会调用`DelayedRequest.isSatisfied`，实现由具体子类来决定，看看`DelayedProduce.isSatisfied`：

{% highlight java %}
  def isSatisfied(replicaManager: ReplicaManager) = {
    //检查请求中每一个topic-partition
    partitionStatus.foreach { case (topicAndPartition, fetchPartitionStatus) =>
      // skip those partitions that have already been satisfied
      if (fetchPartitionStatus.acksPending) {//acksPending代表是否还有数据没有同步完
        val partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition)
        val (hasEnough, errorCode) = partitionOpt match {
          case Some(partition) =>
            partition.checkEnoughReplicasReachOffset(
              fetchPartitionStatus.requiredOffset,
              produce.requiredAcks)//检查该topic-partition的所有备份都已经追上了该请求的offset
          case None =>
            (false, ErrorMapping.UnknownTopicOrPartitionCode)
        }
        if (errorCode != ErrorMapping.NoError) {
          fetchPartitionStatus.acksPending = false
          fetchPartitionStatus.responseStatus.error = errorCode
        } else if (hasEnough) {
          fetchPartitionStatus.acksPending = false
          fetchPartitionStatus.responseStatus.error = ErrorMapping.NoError
        }
      }
    }

    //如果该请求的所有topic-partition都已满足条件
    val satisfied = ! partitionStatus.exists(p => p._2.acksPending)
    satisfied
  }
{% endhighlight %}

DelayedProduce经过isSatisfied方法检查，确认条件已经满足，那么isSatisfied标志位被置为true，等待ExpiredRequestReaper定时执行清理，从Watchers和Reaper的列表中移除，最终被GC回收掉。完成Produce延迟请求的生命周期。

### 缺陷

上一节也提到了Reaper线程正是为了缓解内存压力而设计出来的，通过设置工作频率（purgeInterval）可以使其及时的清理已满足条件的延迟请求。但是工作频率设置过高，那么reaper线程会频繁地遍历Wathcers和他的Request列表（同步控制），带来了性能损耗。因此，kafka在0.9.x引入了新的设计来解决这个问题。

### 总结

这一节通过跟踪DelayedProduce请求的处理流程，梳理了其生命周期，也从中明白了RequestPurgatory的工作原理与不足。下一节，将介绍新的设计是如何解决这个问题的。