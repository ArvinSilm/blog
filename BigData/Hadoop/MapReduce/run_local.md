## Windows本地运行MapReduce

--------

Windows本地无HDFS调试MapReduce程序。

### Maven

依赖

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.5.1</version>
</dependency>
```

### 代码

需要修改`org.apache.hadoop.io.nativeio.NativeIO`和`org.apache.hadoop.mapred.YARNRunner`的代码。
新建`org.apache.hadoop.io.nativeio`和`org.apache.hadoop.mapred`包，将代码放入其中。

### 环境

本地解压一份所需版本的Hadoop源码，在系统环境变量中添加**HADOOP_HOME**，指向源码路径。
将[winutils.exe](https://drive.google.com/open?id=1RS4IuApabSg-rm9E5_mAPymOEpPDUm3g)拷贝到HADOOP_HOME/bin目录下。