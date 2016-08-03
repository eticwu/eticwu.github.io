---
layout: post
title:  "深入理解Kafka设计：高性能服务器模型（2）"
date:   2016-07-12
categories: deep_into_kafka
tags: Kafka
---
* 2
{:toc}

```
版权声明：本文为博主原创文章，未经博主允许不得转载。
```

### 摘要
`KafkaServer`作为整个Broker的核心，它管理着所有的功能模块（例如：`ReplicaManager`，`LogManager`，`TopicConfigManager`等），其中`SocketServer`便是NIO服务器模块，负责网络通信。接下来我们会以请求处理的角度来分析`SocketServer`的相关代码。

### 请求处理流程

#### 1. 启动

当我们通过脚本kafka-broker-start.sh启动Broker时，其调用流程是
```
Kafka.main→KafkaServerStartable.startup→KafkaServer.startup
```
，其中`KafkaServer.startup`方法如下：

{% highlight java border %}
val requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests)
.....
def startup() {
   .....
   for(i <- 0 until numProcessorThreads) {
      processors(i) = new Processor(.....)
      Utils.newThread("kafka-network-thread-%d-%d".format(port, i), processors(i), false).start()
    }
	.....
    this.acceptor = new Acceptor(host, port, processors, sendBufferSize, recvBufferSize, quotas)
    Utils.newThread("kafka-socket-acceptor", acceptor, false).start()
    acceptor.awaitStartup
    .....
  }
{% endhighlight %}

可以看到，`SocketServer`不仅创建了`RequetsChannel`，而且创建并启动了1个`Acceptor`线程和N个`Processor`线程。

#### 2. 建立新连接
`Acceptor`启动后主要职责就是负责监听和建立新连接。
{% highlight java %}
private[kafka] class Acceptor(.....) extends AbstractServerThread(connectionQuotas) {
  val serverChannel = openServerSocket(host, port)
  def run() {
    serverChannel.register(selector, SelectionKey.OP_ACCEPT);//关注ACCEPT事件
    .....
    var currentProcessor = 0
    while(isRunning) {
      val ready = selector.select(500)
      if(ready > 0) {
        val keys = selector.selectedKeys()
        val iter = keys.iterator()
        while(iter.hasNext && isRunning) {
            .....
            accept(key, processors(currentProcessor))//指定某一个Processor
            .....
            currentProcessor = (currentProcessor + 1) % processors.length//轮询下一个Processor
          .....
        }
      }
    }
   .....
  }
{% endhighlight %}

建立连接以后，以轮询的方式将新连接均衡的分配给每一个`Processor`。

{% highlight java %}
  def accept(key: SelectionKey, processor: Processor) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()//获取新连接
    try {
      .....
      socketChannel.configureBlocking(false)//设置非阻塞模式
      socketChannel.socket().setTcpNoDelay(true)//打开Nagle's算法，禁止大量小包发送
      socketChannel.socket().setSendBufferSize(sendBufferSize)
      .....
      processor.accept(socketChannel)
    } catch {
      .....
      close(socketChannel)
    }
  }
{% endhighlight %}

实际上`Acceptor`将`SocketChannel`放入`Processor`的新连接处理队列`newConnections`中。

{% highlight java %}
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    wakeup()//唤醒阻塞在select()上的selector
  }
{% endhighlight %}

#### 3. 连接处理
`Processor`线程主循环在不断的处理连接的读写。
	
{% highlight java %}
override def run() {
    .....
    while(isRunning) {//主循环
      // 从newConnections队列获取并处理新连接
      configureNewConnections()
      // 从相应的response队列获取Response并处理
      processNewResponses()
      .....
      val ready = selector.select(300)//获取就绪事件
      .....
      if(ready > 0) {
        val keys = selector.selectedKeys()
        val iter = keys.iterator()
        while(iter.hasNext && isRunning) {
          var key: SelectionKey = null
          try {
            key = iter.next
            iter.remove()
            //相应处理
            if(key.isReadable)
              read(key)
            else if(key.isWritable)
              write(key)
            else if(!key.isValid)
              close(key)
            else
              throw new IllegalStateException("Unrecognized key state for processor thread.")
          } catch {
            ......
            close(key)
          }
        }
      }
      maybeCloseOldestConnection//检查空闲的连接
    }
    .....
    closeAll()
    .....
  }
  
  private val connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000
  private var currentTimeNanos = SystemTime.nanoseconds
  private val lruConnections = new util.LinkedHashMap[SelectionKey, Long]//连接与最近访问的时间戳Map
  private var nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos
  
  private def maybeCloseOldestConnection {
    if(currentTimeNanos > nextIdleCloseCheckTime) {//是否该检查空闲连接了
      if(lruConnections.isEmpty) {
        nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos
      } else {
        val oldestConnectionEntry = lruConnections.entrySet.iterator().next()//获取最早的连接
        val connectionLastActiveTime = oldestConnectionEntry.getValue
        nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos
        if(currentTimeNanos > nextIdleCloseCheckTime) {//检查连接是否空闲
          val key: SelectionKey = oldestConnectionEntry.getKey
          .....
          close(key)
        }
      }
    }
  }
{% endhighlight %}

`Processor`不仅要处理新连接，而且也处理旧连接上的数据读写和关闭，并用一段标准的NIO事件处理代码来处理相应事件。同时还有重要的一步，那就是检查和清除空闲连接（超过10分钟没有读操作的连接），以免浪费带宽和内存。

{% highlight java %}
//获取新连接，并在selector注册OP_READ事件
private def configureNewConnections() {
    while(newConnections.size() > 0) {
      val channel = newConnections.poll()
      .....
      channel.register(selector, SelectionKey.OP_READ)
    }
}

//获取Response，根据结果在selector注册不同的事件
private def processNewResponses() {
    var curr = requestChannel.receiveResponse(id)//获取对应的response队列
    while(curr != null) {//如果有大量的response，一直要清空为止
      val key = curr.request.requestKey.asInstanceOf[SelectionKey]
      try {
        curr.responseAction match {
          case RequestChannel.NoOpAction => {
            //不需要返回响应给客户端，等待可读事件，然后读取后续数据
            ..... 
            key.interestOps(SelectionKey.OP_READ)
            key.attach(null)
          }
          case RequestChannel.SendAction => {
            .....
            //等待可写事件，然后将响应写回客户端
            key.interestOps(SelectionKey.OP_WRITE)
            key.attach(curr)
          }
          case RequestChannel.CloseConnectionAction => {
            .....
            //没有后续操作，直接关闭连接
            close(key)
          }
          case responseCode => throw new KafkaException("No mapping found for response code " + responseCode)
        }
      } catch {
        case e: CancelledKeyException => {
          debug("Ignoring response for closed socket.")
          close(key)
        }
      } finally {
        curr = requestChannel.receiveResponse(id)//获取下一个response
      }
    }
  }
{% endhighlight %}

再来看看read和write事件的处理过程：

{% highlight java %}
def read(key: SelectionKey) {
    lruConnections.put(key, currentTimeNanos)//更新连接最近访问时间戳
    val socketChannel = channelFor(key)
    var receive = key.attachment.asInstanceOf[Receive]
    .....
    val read = receive.readFrom(socketChannel)//根据Kafka消息协议，从中解析出请求数据
    .....
    if(read < 0) {//读取不到数据，可能客户端已经断掉连接
      close(key)
    } else if(receive.complete) {//如果解析完成
      val req = RequestChannel.Request(.....)//根据requestId封装成不同类型的Request对象
      requestChannel.sendRequest(req)//投入RequestQueue中
      key.attach(null)
      //已经收到一个完整的请求了，先处理这个请求，因此不再关心可读，也没必要立即wakeup selector
      key.interestOps(key.interestOps & (~SelectionKey.OP_READ))
    } else {//解析尚未完成，继续等待数据可读
      key.interestOps(SelectionKey.OP_READ)
      wakeup()
    }
  }
  
  def write(key: SelectionKey) {
    val socketChannel = channelFor(key)
    val response = key.attachment().asInstanceOf[RequestChannel.Response]
    val responseSend = response.responseSend
    .....
    val written = responseSend.writeTo(socketChannel)//将响应数据写回
    .....
    if(responseSend.complete) {//响应数据写回完成
      key.attach(null)
      key.interestOps(SelectionKey.OP_READ)//关注下一次数据请求
    } else {
      key.interestOps(SelectionKey.OP_WRITE)//否则继续等待连接可写
      wakeup()//立即唤醒selector
    }
  }
{% endhighlight %}

由于Request和Response作为Network层与API层的交互对象，所以read的一个重要工作就是将请求数据解析出来并封装成Request对象，而write就是将业务返回的Response对象写回。

以上便是网络层处理数据读写的过程。接下来介绍API层处理Request和返回Response的过程。

#### 4. 业务逻辑处理
为了不影响网络层的吞吐量，Kafka将繁重的逻辑处理独立出来作为API层，交给一组`KafkaRequestHandler`线程来完成的。这种设计与Netty是一致的。
{% highlight java %}
class KafkaRequestHandlerPool(val brokerId: Int,
                              val requestChannel: RequestChannel,
                              val apis: KafkaApis,
                              numThreads: Int) extends Logging with KafkaMetricsGroup {
  .....	
  val threads = new Array[Thread](numThreads)
  val runnables = new Array[KafkaRequestHandler](numThreads)
  for(i <- 0 until numThreads) {
    runnables(i) = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, apis)
    threads(i) = Utils.daemonThread("kafka-request-handler-" + i, runnables(i))
    threads(i).start()//启动所有handler线程
  }
  
  def shutdown() {
    info("shutting down")
    for(handler <- runnables)
      handler.shutdown//发送一个AllDone的Request，通知handler线程结束
    for(thread <- threads)
      thread.join//等待所有的handler线程都执行完毕才结束
    info("shut down completely")
  }
}
{% endhighlight %}

`KafkaRequestHandlerPool`管理所有handler线程的创建和关闭，同时也保证了正常情况下业务逻辑执行完成后才结束handler线程，不会导致不完整的业务。
所有的`KafkaRequestHandler`线程都从唯一的`RequetsChannel.RequestQueue`中争抢Request，并交给`KafakApis`中相应的逻辑来处理。

{% highlight java %}
def run() {
    while(true) {//handler主循环
      try {
        var req : RequestChannel.Request = null
        while (req == null) {
          .....
          req = requestChannel.receiveRequest(300)//从RquestQueue中获取request
          .....
        }
        if(req eq RequestChannel.AllDone) {//KafkaRequestHandlerPool的关闭通知
        	......
          return
        }
        .....
        apis.handle(req)//交给KafkaApis对应的逻辑去处理
      } catch {
        .....
      }
    }
 }
{% endhighlight %}

可以看到`KafkaRequestHandler`只有当收到关闭通知后，才会结束线程，否则一直执行下去。

`KafkaApis.handle`作为所有业务逻辑的入口，会根据requestId将Request分发给相应的逻辑代码来处理。
{% highlight java %}
def handle(request: RequestChannel.Request) {
    try{
      .....
      request.requestId match {
        case RequestKeys.ProduceKey => handleProducerOrOffsetCommitRequest(request)
        case RequestKeys.FetchKey => handleFetchRequest(request)
        case RequestKeys.OffsetsKey => handleOffsetRequest(request)
        case RequestKeys.MetadataKey => handleTopicMetadataRequest(request)
        case RequestKeys.LeaderAndIsrKey => handleLeaderAndIsrRequest(request)
        case RequestKeys.StopReplicaKey => handleStopReplicaRequest(request)
        case RequestKeys.UpdateMetadataKey => handleUpdateMetadataRequest(request)
        case RequestKeys.ControlledShutdownKey => handleControlledShutdownRequest(request)
        case RequestKeys.OffsetCommitKey => handleOffsetCommitRequest(request)
        case RequestKeys.OffsetFetchKey => handleOffsetFetchRequest(request)
        case RequestKeys.ConsumerMetadataKey => handleConsumerMetadataRequest(request)
        case requestId => throw new KafkaException("Unknown api code " + requestId)
      }
    } catch {
      case e: Throwable =>
        request.requestObj.handleError(e, requestChannel, request)
      .....
    } finally
      .....
  }
{% endhighlight %}

每个业务逻辑处理完成以后，会根据具体情况判断是否需要返回Response并放入ResponseQueue中，由相应的`Processor`继续处理。

### 总结

至此，我们通过请求处理的角度分析了Kafka SocketServer的相关代码，并了解到其网络层与API层的设计和工作原理，从中也学习到如何利用Java Nio实现服务器的方法和一些细节。

### 参考文档
1. [http://ju.outofmemory.cn/entry/124628](http://ju.outofmemory.cn/entry/124628)