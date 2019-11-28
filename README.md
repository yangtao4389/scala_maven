### Spark项目实战:大数据实时流处理日志

### 项目来源：
https://blog.csdn.net/qq_41955099/article/details/88959996

### 代码复制：
https://github.com/spmygithub/ImoocLog_SparkStreaming

### 提示
* ip更换：全局搜索：199.199.199.199更换成服务器的ip


### 数据流设计
1. 编写python脚本，源源不断产生用户访问日志。   
-- 与项目平级的`generate_log.py`来模拟产生数据，可以直接用`python generate_log.py`运行，产生的日志会在该项目下新建`nginx`目录，部署到服务器上的全路径为：`/home/code/dev/scala_log/nginx/access.log`
2. kafka与flume配置  
-- flume通过配置收集日志信息，再传入kafka  
-- kafka接收flume的数据流  
-- 具体模拟可以参考：HOW_TO_START  
4. 本地编写程序，使用 Spark Streaming 消费 Kafka 的用户日志。
5. Spark Streaming将数据清洗过滤非法数据，然后分析日志中用户的访问课程，统计课程数。
6. 将 Spark Streaming 处理的结果写入 HBase 中。  
-- 由于项目过程中用到了hbase表，所以需要先创建，具体参考：HOW_TO_STAR/003.hbase创建需要的表.md  
7. 前端使用 Spring MVC、 Spring、 MyBatis 整合作为数据展示平台。
8. 使用Ajax异步传输数据到jsp页面，并使用 Echarts 框架展示数据。


### 后端数据构建流程
1.通过IntelliJ IDEA中的maven创建scala项目  
* File---> new project --->Maven ---> Create from archetype(选择 scala-archetype-simple) ---> 一直next
* jdk 1.8

2.向pom.xml加入具体的引用包

3.构建src/main/scala/spark1/project/utils  
* 新增 DateUtils.scala 来处理log日志里面的时间，可以直接运行看处理效果
* 新增HbaseUtils.java 来连接Hbase，可以直接运行进行测试，HbaseExample是官方教程  
    参考官方：https://hbase.apache.org/2.0/devapidocs/index.html 这里选择org.apache.hadoop.hbase.client,点击后最下面有个示例，本项目用HbaseExample来实现了。可以直接运行，来判断是否连接到服务器的hbase  
    参考教程：https://github.com/heibaiying/BigData-Notes/blob/master/code/Hbase/hbase-java-api-2.x/src/main/java/com/heibaiying/HBaseUtils.java  

4.domain里面创建scala case来规则化数据写入
* ClickLog.scala  点击日志
* CourseClickCount.scala 当天课程点击统计
* CourseSearchClickCount.scala 当天课程搜索点击统计


5.dao里面创建数据连接相关做数据存储
* 先在hbase创建表：  
(1). 进入Hbase shell：$HBASE_HOME/bin/./hbase shell  
在Hbase shell中执行以下命令:  
(2). 创建名字空间 ns1：create_namespace 'ns1'  
(3). 创建课程访问统计表，列族为info：create 'ns1:courses_clickcoun', 'info'  
(4). 创建搜索引擎统计表，列族为info：create 'ns1:courses_search_clickcount', 'info'  

* CourseClickCountDao.scala  跟hbase交互存储，可以直接运行测试（必须在hbase里面先创建表）
* CourseSearchClickCountDao.scala  



6.application创建 CountByStreaming.scala 的object类，作为项目的主入口
* 直接定义main运行程序  
* StreamingGuide.scala 是来源于http://spark.apache.org/docs/latest/streaming-programming-guide.html的案例
* RecoverableNetworkWordCount.scala同上https://github.com/apache/spark/blob/v2.4.4/examples/src/main/scala/org/apache/spark/examples/streaming/RecoverableNetworkWordCount.scala
* SparkKafkaExample.scala 参考  http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
* KafkaWordCoun.scala  来源https://www.cnblogs.com/zhaojinyan/p/9360873.html

7.日志生成python文件
* generate_log.py  与src平级，过后在服务器用定时器访问时用  

8.服务器启动flume及kafka--HOW_TO_START
9.本地启动主程序：CountByStreaming.scala



### 打包教程：
https://weilu2.github.io/2018/11/16/%E9%85%8D%E7%BD%AE-Intellij-Idea-%E5%92%8C-Sbt-%E5%BC%80%E5%8F%91%E3%80%81%E6%89%93%E5%8C%85%E3%80%81%E8%BF%90%E8%A1%8C-Spark-%E7%A8%8B%E5%BA%8F/
https://blog.csdn.net/freecrystal_alex/article/details/78296851
#### 直接生成java -jar 执行的jar包
编写pom.xml，新增
```sbtshell
    
<build>
  <plugins>
  。。。。
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-assembly-plugin</artifactId>
        <version>2.5.5</version>
        <configuration>
          <archive>
            <manifest>
              <mainClass>spark1.project.application.CountByStreaming</mainClass>
            </manifest>
          </archive>
          <descriptorRefs>
            <descriptorRef>jar-with-dependencies</descriptorRef>
          </descriptorRefs>
        </configuration>
        <executions>
          <execution>
            <id>make-assembly</id>
            <phase>package</phase>
            <goals>
              <goal>single</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
。。。。
    </plugins>
  </build>

```
其中写入要打包的主类（mainClass）：`spark1.project.application.CountByStreaming`
执行maven中的package打包程序
