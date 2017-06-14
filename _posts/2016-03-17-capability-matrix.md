---
layout: post
cover: true
title: 清晰&形式化Runner的能力
date:  2016-02-16 22:00:00
tags: beam
subclass: 'post tag-beam'
categories: 'beam'
author: 相国
nickname: xiangguo
uyan: true
---
翻译自：[https://beam.apache.org/beam/capability/2016/03/17/capability-matrix.html)

翻译：相国

随着初始代码的完成([Dataflow SDK and Runner](https://github.com/apache/beam/pull/1), [Flink Runner](https://github.com/apache/beam/pull/12), [Spark Runner](https://github.com/apache/beam/pull/42))和
[Storm](https://issues.apache.org/jira/browse/BEAM-9)、[Hadoop](https://issues.apache.org/jira/browse/BEAM-19)和[Gearpump](https://issues.apache.org/jira/browse/BEAM-79)等大多数runner的实现，我们想要在Apache Beam(孵化)社区开始解决一个大问题：每一个runner能支持什么功能?
虽然我们很想有一个世界，所有的runner都支持Beam Model的全套语义(以前称为[Dataflow Model](!http://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf))，实际上，总是会有一些某个runner不能支持的特性。例如,基于Hadoop的runner必定是基于batch的，可能无法(轻易)实现支持无限集合。然而，这并不妨碍它被大规模使用。另一个方面，一个runner提供的实现在语义上可能和另外一个runner有些许的不一样（比如，虽然当前的runner都支持exactly-once交付担保， 但是[Apache Samza](http://samza.apache.org/) runner当前仅支持at-least-once）。

为了阐明这件事，我们在已经存在的runner的[能力矩阵](https://beam.apache.org/documentation/runners/capability-matrix/)中枚举了几个Beam model的关键特性，对应模型中的4个关键的问题：What / Where / When / How (如果你不熟悉这些问题，可以概览一下[Streaming 102](http://oreilly.com/ideas/the-world-beyond-batch-streaming-102))。这个表将保持模型的发展,我们的理解的增长,和runner的创建或特性的添加。

下面是对已经存在的runner的能力解读的汇总快照（可以查看[live version](https://beam.apache.org/documentation/runners/capability-matrix/)获取明细、描述和Jira链接）；集成仍在进行,整个系统还没有完全在一个稳定、可用的状态。但是这个情况应该很快就会改变，当第一个Beam的1.0版本发布时，我们会在这个博客上大声地，清晰地公布。

与此同时,这些表有助于明确接下来我们期望什么，并且指导对现有的runner能力的期望以及接下来runner要实现什么特性。

![1](http://orj5v9ukk.bkt.clouddn.com/image/post/capability-matrix-1.png)

![2](http://orj5v9ukk.bkt.clouddn.com/image/post/capability-matrix-2.png)

![3](http://orj5v9ukk.bkt.clouddn.com/image/post/capability-matrix-3.png)

![4](http://orj5v9ukk.bkt.clouddn.com/image/post/capability-matrix-4.png)