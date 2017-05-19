---
layout: post
cover: true
title: 使用Apache Beam进行有状态的数据处理
date:  2017-02-13 10:18:00
tags: Beam
subclass: 'post tag-beam'
categories: 'beam'
---

翻译自：[https://beam.apache.org/blog/2017/02/13/stateful-processing.html](https://beam.apache.org/blog/2017/02/13/stateful-processing.html)

翻译：郭亚峰（默岭）



我们可以使用Beam构建高度抽象的可移植的数据处理作业来处理无边界，乱序的大规模数据。有状态处理操作是Beam模型的新功能，它扩展了Beam模型的能力，能够支持新的用例，提供新的效能。在这篇文章中，我会介绍Beam的有状态处理：它如何工作，和Beam模型中的其它功能如何配合，可以用来干什么，代码应该怎么写。

``
警告：新功能！：这是Beam模型中非常新的一部分。运行引擎正在增加对它的支持。目前已经有多个引擎支持这个功能，但是在你使用前请确保查阅新的[运行引擎支持能力表文档](https://beam.apache.org/documentation/runners/capability-matrix/)，了解目前的支持状态。
``
首先我们快速回顾一下：Beam中一个数据处理作业是一个有向无环图。图中包含了称作PTransforms的并行数据处理操作。数据则来自于PCollections。接下来我们以下图为例展开描述。

![A Beam Pipeline - PTransforms are boxes - PCollections are arrows](https://beam.apache.org/images/blog/stateful-processing/pipeline.png)




长方形的盒子代表了PTransform。PTransform中间的箭头代表PCollections中的数据数据从一个PTransform流向下一个PTransform。PCollection可以是有边界的（意味着数据是有限的并且你知道它是有限的），也可以是无边界的（意味着你不知道它是否是有限的 - 大概可以认为就像是一个数据输入流，你不知道它是否会终止，何时会终止）。图边缘的圆柱体代表了数据源或者数据宿，比如说有边界的一组日志文件或者无边界的数据流如kafka主题。这篇文章主要不会涉及数据源和数据宿，而是发生在两者之间的数据处理。

在Beam中处理数据需要的最主要的两块是ParDo，用来对数据进行并行处理，和GroupByKey(它和CombinePerKey紧密关联，后面会讲到这一点。)用来按同样的键汇总处理数据。下图当中（在Beam的PPT中经常可见）图形的颜色代表了它的键。如右图所示，GroupByKey/CombinePerKey操作把所有绿色的数据收集到了一起，产生了一个唯一的输出。

![ParDo and GroupByKey/CombinePerKey:          Elementwise versus aggregating computations](https://beam.apache.org/images/blog/stateful-processing/pardo-and-gbk.png)

但并非所有的用例都可以用简单的ParDo/Map和GroupByKey/CombinePerKey操作来进行描述表达。这篇文章讲述的就是Beam编程模型的一个新的扩展：**处理元素时可以关联可变的状态**。

![Stateful ParDo - sequential per-key processing with persistent state](https://beam.apache.org/images/blog/stateful-processing/stateful-pardo.png)

上图中ParDo可以访问旁边的持久化的一致性的状态，在处理每条数据的时候可以读写状态。状态是按key进行分区的，因此状态用一个平行的不同颜色的色带组成的。它同时也按不同的窗口分区。不过我觉得如果用格子图案可能有点点过了;-)。后面我在第一个例子里会讲述为什么状态要按这样的方式进行分区。

这篇文章接下来的内容中，我会描述这项新功能的更多细节 - 它如何工作，和其它现有的功能有什么区别，如何确保它仍然是可以大规模扩展的。在模型层面介绍完了之后，我们看一下如何在Beam Java SDK中使用这个功能。

## 在Beam中有状态数据处理是如何工作的？
ParDo数据转换的逻辑是通过DoFn来表述的。DoFn作用于每一个输入的数据记录来加工数据。如果没有状态接入，那么DoFn就是一个单纯的从一条输入记录到一条或者多条的映射，如同MapReduce中的Mapper一样。有了状态以后，DoFn在处理每个输入元素的时候，可以访问持久化的可变的状态。如下图所示一样。
![Stateful DoFn -          the runner controls input but the DoFn controls storage and output](https://beam.apache.org/images/blog/stateful-processing/stateful-dofn.png)

首先要注意这张图里面，所有的数据 - 小正方形，圆形，三角形，是红色的。意味着有状态处理是基于同一个key的前提下的。所有的数据元素是 key-value 对并且key相同。Beam Runner对DoFn的调用用黄色表示，DoFn对Runner的调用用紫色表示:

Runner调用DoFn的 @ProcessElement方法。每个key+window调用一遍。
DoFn读写状态 - 弧形箭头代表从旁边的存储读写状态
DoFn调用ProcessContext.output （或ProcessContext.sideOutput）输出元素到输出流（或者到副输出）
在概括层面上，它是很直观的：在编程过程中，我们经常有编写循环的经历。在循环中，除了一些别的操作，我们经常修改一些可变变量。这些和有状态处理是非常类似的。那么在Beam模型中，它是如何运作的：和其他功能的关系是什么？它的可扩展性如何，因为状态就意味着同步？啥时候应该使用这个功能？

## Beam模型如何支持有状态数据处理?
在讨论有状态数据处理的细节之前，我们先来看一下另外一种带“状态”的对多条数据进行处理的方式：CombineFn。在Beam中，你可以使用Combine.perKey(CombineFn)来进行满足结合律和交换律的累加型计算。累加计算针对同一键（和窗口）的多条数据给出一条结果。

下图演示了CombineFn，一种Runner调用CombineFn的最简单的方式来构建累加器并且从最终的累加器当中提取结果。

![CombineFn - the runner controls input, storage, and output](https://beam.apache.org/images/blog/stateful-processing/combinefn.png)

和有状态的DoFn的示意图一样，所有的数据的颜色都是红色的，因为这里展示的是Combine针对某个具体的key的数据的操作。方法调用全部是黄色，因为所有调用都由Runner控制。Runner调用 addInput 把每条数据对追加到当前的累加器里面去。

- Runner在有必要时会持久化累加器。
- Runner在可以吐出数据时调用extractOutput吐出数据。

现在看起来 CombineFn和 DoFn非常相似。事实上数据的流动路径确实非常相似，但两者本质上有重要的区别

- 在CombineFn中所有的对方法的调用和和状态存储是由Runner控制的。我们无法决定状态何时被持久化，如何被持久化，何时累加器被抛弃（取决于窗口的触发器）或者何时从累加器中提取输出。
- 在CombineFn中状态只能存放于唯一的累加器中。而在有状态DoFn中你可以只访问你需要读取的数据，只写入那些已经变动的数据。
- CombineFn中支持的功能没有DoFn那么丰富。比如说DoFn支持一个输入多个输出，支持副输出。（当然你可以通过一个足够复杂的累加器来进行模拟，但是这种实现极不自然也不高效。而一些DoFn的功能比如说支持副输入，可以访问窗口，感觉上对CombineFn也是非常有提供支持的必要性的。

不过CombineFn的一个主要的功能是提供了mergeAccumulators给Runner。它也是CombineFn的结合律的具体体现。它为CombineFn的执行提供了很大的优化。Runner可以实例化多个CombineFn来用经典的分而治之的架构模式进行处理，然后调用mergeAccumulators将中间结果合并。如下图所示。

![Divide-and-conquer aggregation with a CombineFn](https://beam.apache.org/images/blog/stateful-processing/combiner-lifting.png)

CombineFn的要求是：无论Runner是否做了并行化调用，或者甚至做了更复杂的热键防倾斜处理操作，结果必须是一致的。（译注：即用CombineFn实现的运算逻辑必须满足交换律和结合律）

而有状态的DoFn中并没有（也无必要）提供这种合并操作。Runner无法随意地将执行并行化并且随后合并状态。不过需要注意接收输入元素的顺序仍然是随机的，因此DoFn需要对数据输入顺序不敏感，对数据结合顺序不敏感。不过同样，对于输出并不要求具有一致性。（一个有意思也很简单的事实：如果输出永远是一致相等的，那么DoFn必然满足结合律和交换律。）

现在我们可以理解有状态的DoFn和CombineFn有哪些区别了。不过在这里我想退一步来进行一个高度的概括。Beam中的有状态处理，和其他相关的功能一起，使得我们能有机会突破Beam高度的确定性的函数式编程模式，来做一些非确定性的指令式编程表述，因为有些逻辑本身更适合用指令模式来进行表达。

## 例子：任意但一致的序号分配
假设对于输入的元素我们需要分配一个在key+window范围内唯一的序号。对某条记录，我们不关心给它分配了什么序号，我们只想保证需要是唯一的并且连续的（中间不断号）。在跳进去研究如何用Beam SDK编写代码之前，我们先从模型层面过一下。我们想象一下，我们可能要把输入做一个像下图一样的处理：

 
 <img class="center-block" src="https://beam.apache.org/images/blog/stateful-processing/assign-indices.png" alt="Assigning arbitrary but unique indices to each element" width="180">


A,B,C,D,E这些元素的顺序是随机的。因此他们分配得到的序号也是随机的。而下游的数据转换需要理解并且接受这一点。对于这个DoFn输出的值而言，它不满足交换律或者结合律。这个数据转换操作的输入数据顺序无关性仅仅体现在，无论数据输入的顺序如何变化，输出都满足如下的属性：没有重复的序号，序号之间没有断开，并且每个输入元素都获得了一个序号。

从概念上来看，用有状态的循环来表达这种处理逻辑是非常简单的：这里我们需要存储的是下一个序号。

- 接收到输入的数据元素，把它和序号关联并且输出。
- 把数据序号增加并存入状态。

这是一个探讨大数据并行处理的很好的例子。因为上述的算法根本无法并行化。如果我们要用同样的方式对PCollection编序号的话，我们一次只能处理一个元素。很明显这是非常糟糕的。 Beam中的状态会被限定到特定的范围内，从而使得大部分时候有状态的ParDo仍然是可以并行执行的。不过即便SDK做了这样的隔离分区，大部分时候我们仍然需要仔细考虑计算可并行化的问题。

目前有状态ParDo的状态设计是按key+window的方式组织隔离分区的。当DoFn读写一个名为index的状态的时候，它实际上是按照当前的key+window读取名为index的可变状态。为了便于理解期间，你可以把整个Transform的状态想象成一张表格。这张表格的行是程序中状态变量的名字，列是key+window对。如同下表：

xx | (key, window)1 | (key, window)2 |(key, window)3 | …
----|------|----|----|---
"index" | 3 | 7 | 15 | …
"fizzOrBuzz?"|"fizz"|"7"|"fizzbuzz"|…
…|…|…|…|…		
				
				
（如果你的空间感觉比较好，也可以想象成一个有三个不同维度（状态变量名，key, window）的立方体）。

我们可以通过以下两种方式保证执行的并行度：

保证在较少的窗口内有足够多的键 - 比如说在全局窗口中按用户id作为key。
或者在很多的窗口里处理比较少的键 - 比如说用固定窗口处理一个全局唯一的key。
**警告**： 目前所有的Beam Runner都只支持按键进行并行处理。

大多数情况下，对于每个key+window，你可以只聚焦于如何和一个单一的状态变量进行操作。Beam不允许跨列（即同一变量在不同窗口/key里的值）的状态访问。

## Beam状态的Java SDK操作
我们已经用一个比较抽象的例子讨论了在Beam模型中如何进行有状态的处理。接下来我们来看一下如何用Java SDK编写有状态数据处理。下面是通过有状态的DoFn 给输入数据分配一个随机但同一个key+window内一致连续的序号的具体实现。
```
new DoFn<KV<MyKey, MyValue>, KV<Integer, KV<MyKey, MyValue>>>() {

  // A state cell holding a single Integer per key+window
  @StateId("index")
  private final StateSpec<Object, ValueState<Integer>> indexSpec = 
      StateSpecs.value(VarIntCoder.of());

  @ProcessElement
  public void processElement(
      ProcessContext context,
      @StateId("index") ValueState<Integer> index) {
    int current = firstNonNull(index.read(), 0);
    context.output(KV.of(current, context.element()));
    index.write(current+1);
  }
}
```
让我们解密下上面的代码:

- 首先我们发现上述的代码中多次出现了注解@StateId("index")。这表明你在DoFn中使用了一个名为“index” 的可变状态。Beam SDK和你所选择的Runner，将会处理这些注解，把DoFn中的状态进行正确地处理。

- 第一次出现的@StateId("index")是对一个类型为StateSpec 的变量的注解。它声明和配置我们需要的状态。类型参数 ValueState描述了我们状态的类型。 ValueState用来存储一个单个值。注意，我们这里声明的StateSpec 变量本身是final的，不能用来存储状态。Runner在运行期会按照StateSpec的定义 提供状态变量的具体实现。

- 在声明状态的时候，我们还需要提供对状态进行序列化反序列化的Coder。这个是通过调用StateSpecs.value(VarIntCoder.of())来完成的。

- 第二次出现的注解@StateId("index")是对方法@ProcessElement 中的参数的注解。这里表示把之前定义的状态作为参数传给了processElement方法。

- 状态的访问非常简单， read()读取状态，write(newvalue)把新值写入状态。

- DoFn 的其他功能和之前一样，比如使用context.output(...)输出结果等。我们仍然可以使用side input, side output，对窗口进行访问等。

下面是对于SDK和Runner对于状态处理的总结：

- 状态需要明确地声明，从而SDK和Runner能够了解并管理这些状态。例如当窗口超期时清理掉状态。
- 如果我们声明了一个状态类型但是转换到了一个错误的类型，SDK能够发现并捕捉这个错误。
- 如果使用了重复的状态ID， SDK同样能够发现。
- Runner发现一个DoFn 是有状态的时候，运行期的行为可能会有非常大的差别。比如说会增加数据的shuffle，同步过程来避免对状态存储的并发访问。
- 接下来我们来看一个更贴近现实业务场景的例子。

## 例子：异常探查
假设我们处理的数据源头是用户的动作记录流。这些数据流入一个复杂的模型，来对用户的行为进行定量表达分析，判断行为的性质，比如说，探查出诈骗行为。我们会从输入的事件中建立模型，然后拿新流入的事件和最近的模型进行比较，探查是否有异常发生。

如果我们是使用 CombineFn来构造模型，那么我们可能会在实现mergeAccumulators的时候陷入困境。假设下面是我们使用 CombineFn来构造模型的可能的代码：
```
class ModelFromEventsFn extends CombineFn<Event, Model, Model> {
    @Override
    public abstract Model createAccumulator() {
      return Model.empty();
    }

    @Override
    public abstract Model addInput(Model accumulator, Event input) {
      return accumulator.update(input); // this is encouraged to mutate, for efficiency
    }

    @Override
    public abstract Model mergeAccumulators(Iterable<Model> accumulators) {
      // ?? can you write this ??
    }

    @Override
    public abstract Model extractOutput(Model accumulator) {
      return accumulator; }
}
```
我们先把实现mergeAccumulators的难题放在一边，假设我们实现了上述的CombineFn，命名为ModelFromEventsFn。 那么我们就可以为某个特定用户通过调用Combine.perKey(new ModelFromEventsFn())计算出我们需要的模型。接下来的问题是如何把计算出来的模型重新代入到同一个用户动作记录流当中？一个标准的解决方案是把Combine转换的输出作为一个ParDo转换的副输入，然后在处理PCollection中的数据的时候将其读入，使用该模型的产出来对流入的数据进行判断，输出预测结果。代码如下：
```
PCollection<KV<UserId, Event>> events = ...

final PCollectionView<Map<UserId, Model>> userModels = events
    .apply(Combine.perKey(new ModelFromEventsFn()))
    .apply(View.asMap());

PCollection<KV<UserId, Prediction>> predictions = events
    .apply(ParDo.of(new DoFn<KV<UserId, Event>>() {

      @ProcessElement
      public void processElement(ProcessContext ctx) {
        UserId userId = ctx.element().getKey();
        Event event = ctx.element().getValue();

        Model model = ctx.sideinput(userModels).get(userId);

        // Perhaps some logic around when to output a new prediction
        … c.output(KV.of(userId, model.prediction(event))) … 
      }
    }));
```
在上面的代码中，每一个用户在一个窗口期内，数据转换 Combine.perKey(...) 都只会输出一个模型。这个模型是由View.asMap() 转换负责生成并作为 ParDo的副输入。 ParDo的执行处理会一直等待副输入完备之后才会处理输入的主数据流，把每一个事件和模型进行比较。这是一个高延迟，高完整性（数据完整性）的方案。模型考虑了用户在窗口期内所有的行为，不过当窗口完成之前整个数据处理管道不会有任何的输出。

上述的方案可能我们无法接受。我们需要更快得到模型的输出，或者甚至我们不需要定义任何意义上的窗口，我们只是需要持续的用最新的模型进行分析，即便是生成模型的数据不是非常完整也没有关系。那么如何才能控制正在使用的模型的更新？触发器是Beam管理平衡数据完整性和延迟程度之间的工具。下面我们使用了触发器来控制模型在输入数据到达一秒后触发窗口计算并把模型输出。
```
PCollection<KV<UserId, Event>> events = ...

PCollectionView<Map<UserId, Model>> userModels = events

    // A tradeoff between latency and cost
    .apply(Window.triggering(
        AfterProcessingTime.pastFirstElementInPane(Duration.standardSeconds(1)))

    .apply(Combine.perKey(new ModelFromEventsFn()))
    .apply(View.asMap());
```
使用触发器通常也是一种非常好的平衡数据延迟和处理成本的方式：如果源头生产的事件数量非常大，那么不管怎么样一秒钟内系统只会计算一次，产生一个新模型。这样下游收到的数据量就会减少，避免产生非常多的模型，造成下游拥堵，一个模型还没有使用，新的模型又产生了。而另一方面，如果触发器触发的周期过长，考虑到缓存，处理延迟等，新的模型可能要花费很多秒才能在副通道中准备好。在此之间，很多的事件（有可能是一整批活动记录）已经流过了 ParDo ，使用了比较旧的模型。如果runner对cache的过期时间控制的比较紧的话，提高触发器的频率就可以通过增加计算成本来减少数据的延迟程度（译注：或者说，模型的新旧程度）

这里还有另外一个关于成本的因素需要考虑。对于异常探查场景来说，ParDo 可以通过减少我们并不感兴趣的正常数据的输出量来降低下游需要处理的数据量，从而降低下游的计算成本。 Filter转换可以处理大部分的数据过滤场景，但是如果我们过滤与否的判断条件是基于数据流本身的上次计算结果的话， Filter本身就不能够处理了。
我们可以使用有状态数据处理来同时解决使用副输入引入的延迟问题，以及大量我们不感兴趣输出引起的处理成本增加的问题。下面的代码使用了本文介绍的有状态ParDo进行了实现。
```
new DoFn<KV<UserId, Event>, KV<UserId, Prediction>>() {

  @StateId("model")
  private final StateSpec<Object, ValueState<Model>> modelSpec =
      StateSpecs.value(Model.coder());

  @StateId("previousPrediction")
  private final StateSpec<Object, ValueState<Prediction>> previousPredictionSpec =
      StateSpecs.value(Prediction.coder());

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @StateId("previousPrediction") ValueState<Prediction> previousPredictionState,
      @StateId("model") ValueState<Model> modelState) {
    UserId userId = c.element().getKey();
    Event event = c.element().getValue()

    Model model = modelState.read();
    Prediction previousPrediction = previousPredictionState.read();
    Prediction newPrediction = model.prediction(event);
    model.add(event);
    modelState.write(model);
    if (previousPrediction == null 
        || shouldOutputNewPrediction(previousPrediction, newPrediction)) {
      c.output(KV.of(userId, newPrediction));
      previousPredictionState.write(newPrediction);
    }
  }
};
```
我们解读一下上面的代码：

- 我们定义了两个状态变量，@StateId("model")用来保存用户当前的模型，而@StateId("previousPrediction")用来保存上一次模型的预测输出。
- @ProcessElement使用了注解增加了两个参数来获得对状态的访问。
- 通过modelState.read()获得了当前的模型。因为状态本身是按key+窗口组织的，因此获得的模型是属于当前用户的模型。
- 我们可以使用model.prediction(event)得到一个对流入的新事件的预测，然后用previousPredicationState.read()获得上次的预测结果，并且把两者之间进行比较。
- 我们可以通过model.update() 来更改模型，然后使用modelState.write(...)来把更改过的模型回写。从状态中读取出来的值是可以修改的，唯一注意就是要把修改过的值写回到状态中去。这个约定和CombineFn 中的累加器是一致的。
- 如果预测值和上次的输出相比已经有比较显著的变化，那么我们可以通过context.output(...)输出这次的预测，并且使用previousPredictionState.write(...)保存预测结果。这里是否输出本次的预测结果，是和上次的输出相关的，而和上次的计算不一定相关。现实中这里的判断逻辑有可能会更加复杂。 在正式开始在生产中使用有状态数据处理之前，我们下面来看看它是否真的适用于你的场景，有哪些方面需要考虑。

## 性能考虑
判断是否是用按key+window组织的状态，我们需要理解它是如何工作的。尽管每个Runner如何管理状态可能会有不同，不过还是有一些通用的需要考虑的点：

- 按key+window分区。也许最重要的事情就是考虑runner可能需要在网络上交换数据从而把每个key+window的数据分布到一起。如果数据之前已经是按key分布的，那么runner有可能会利用好这一点，不会再次分发。
- 同步开销：API的设计上，runner承担了并行控制的职责。这个设计在便利应用开发者的同时也使得对于同一组key+window的数据处理无法并行化。即便是这些处理能够利用并行化。
- 状态的存储和容错性：因为状态是按key+window组织的，需要并行处理的key+window越多，状态占据的存储也会越多。因为Beam所有状态数据(含CombineFn的状态)的容错性，一致性处理是同一套机制，因此此类状态过大过多会增加状态管理的总体成本。
- ​状态的过期：因为状态是按窗口组织的，runner在窗口过期后（当水位线越过允许的延迟后）会回收状态占用的资源。这也意味着runner会增加一个计时器来定时调度状态的回收。
 
## 试试看!
如果你对Beam还不熟悉的话，希望对有状态处理的介绍会引起你对Beam的兴趣，可以评估下它是否能解决你的问题。如果已经在使用Beam的话，我希望这个新增的功能能帮助你支持更多的新的场景。别忘了检查Runner的功能表 capability matrix看看你使用的Runner对这一新功能的支持程度。
另外你可以订阅邮件列表 user@beam.apache.org加入社区。我们希望听到你的声音。