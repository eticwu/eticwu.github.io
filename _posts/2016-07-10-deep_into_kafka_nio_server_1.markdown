---
layout: post
title:  "深入理解Kafka设计：高性能服务器模型（1）"
date:   2016-07-10
categories: deep_into_kafka
tags: Kafka
---
* 1
{:toc}

```
版权声明：本文为博主原创文章，未经博主允许不得转载。
```

### 摘要
Kafka作为一个高性能的消息中间件，其高效的原因可以归纳为以下这几个方面：

* 高性能服务器模型
* PageCache
* Zero－Copy

本文将从源码层面（基于0.8.2.X）介绍Broker的高性能服务器模型是如何实现的。

### 高性能服务器模型
Kafka并没有采用netty或mina等第三方网络应用框架，而是直接了当的使用了NIO来实现服务器，并在使用了IO多路复用以及多线程Reactor模式，这种设计的优势是很容易实现，同时也很快。

官方并没有在服务器设计上详细展开，因此本文将从逻辑结构和源码方面来分析这个方面的设计，借此了解一下NIO服务器设计的方法及一些细节。

SocketServer作为Kafka的NIO服务器实现，其逻辑结构图如下：
![SocketServer逻辑架构图](/public/img/deep_into_kafka_nio_server_1_1.png)

### 重要组件

1. Acceptor线程：主要负责监听并接受客户端（包括但不限于Producer，Consumer，Broker，Controller，AdminTool）的连接请求，新连接建立以后指定某个Processor去处理。
2. Processor线程：负责数据读写，连接关闭的处理线程，其数目由配置num.network.threads决定，默认是3个。每个Processor内部都有自己的newConnections队列和selector。
	* newConnections：一个无界的SocketChannel队列，存放新建立的连接，将Acceptor与Processor的功能解藕。
	* selector：只使用一个selector来支撑大量连接的事件管理很容易遇到瓶颈，而多个selector并存的结构可以均衡的管理大量连接。
 	
3. RequestChannel：包含一个RequestQueue和多个ResponseQueue。它是网络层与API层交换数据的地方，同时也使得两者逻辑解藕和异步化。
	* RequestQueue：所有的请求都会被封装成Request并放入RequestQueue中，队列大小默认500。
	* ResponseQueue：每个Processor都会有对应ResponseQueue，KafkaApis业务逻辑处理完成后，会将返回结果封装成Response，接着由相应的Processor来处理该response。而且设计上必须保证一对Request和Response都要由同一个Processor来处理，因为只有这个Processor拥有该通信连接。

4. KafkaRequestHandler线程：这是真正的业务逻辑处理线程，其数目由配置num.io.threads决定，默认是8个。每个Handler线程都在不断的从RequestChannel.RequestQueue中获取新的请求，那些负载轻的线程才有可能抢到新的请求，因为负载重的线程（也许正在进行IO）还没有空闲来接受下一个新的请求，所以这也算一个潜在的负载均衡策略吧。
5. KafkaApis：Broker的所有业务逻辑都定义在这里，其handle方法会根据Request对象的requestId（对应各种业务逻辑，其定义可以在类RequestKeys中看到），将请求分发给对应的业务逻辑处理方法。当处理完成以后，可能会将处理结果封装成Response返回给对应的RequestChannel.ResponseQueue。	

### 总结

这一节主要是从总体上介绍了Broker服务器模型的各种重要的组件，下一节我们将结合源码分析一下请求处理的流程:
[深入理解Kafka设计:高性能服务器模型（2）](/deep_into_kafka/2016/07/12/deep_into_kafka_nio_server_2.html)