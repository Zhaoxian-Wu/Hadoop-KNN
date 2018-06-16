# Hadoop Mapreduce实现KNN算法
## 运行环境
Hadoop 2.9.0  
java version "10" 2018-03-20  
Java(TM) SE Runtime Environment 18.3 (build 10+46)  
Java HotSpot(TM) 64-Bit Server VM 18.3 (build 10+46, mixed mode)  
## 用法
首先把仓库中的`KNN.jar`和`KnnSampleGenarator.jar`放入当前工作目录
### 测试数据生成
> ```$HADOOP_HOME/bin/hadoop jar KnnSampleGenarator.jar <SampleSize> <VectorSize> <K of KNN>```
- SampleSize: 样本大小
- VectorSize: 向量维数
- K of KNN: KNN的k


如`$HADOOP_HOME/bin/hadoop jar KnnSampleGenarator.jar 1000 80 50`将会生成一个大小为1000的样本以及一个目标搜索向量，样本中每个向量的维数为80。最后一个50指的是样本中有50/2=25个向量与目标向量“接近”的向量，当使用K=50的时候，这25个向量必然在KNN的结果中。这样生成的数据可以用来检查算法的正确性。  
生成的数据位于HDFS上的/KNN/sample/target和/KNN/sample/dataSet文件

### 数据集的预处理
> ```hadoop jar PreProcess.jar <dataSet> <bucketSize>```
- dataSet: 数据集
- bucketSize: 桶的长度，与给定数据集数据之间的长度有关  

如处理上面生成的数据可以使用```$HADOOP_HOME/bin/hadoop jar PreProcess.jar /KNN/sample/dataSet 0.9```


### KNN计算
> ```HADOOP_HOME/bin/hadoop jar KNN.jar <targetFile> <k of KNN>```
- targetFile: 待寻找KNN的目标向量
- DataSet: 寻找KNN的数据集
- k of KNN: KNN的k
如使用`KnnSampleGenarator.jar`生成的测试文件：  
> ```HADOOP_HOME/bin/hadoop jar KNN.jar /KNN/sample/target 50```

输出存放在/KNN/result文件夹内，存放的是计算出来的K的最近邻向量，查看命令为  
> ```HADOOP_HOME/bin/hadoop dfs -cat /KNN/result/*```
## 数据格式
输入及输出均采用数据+”, "（逗号+空格）分隔的形式，如下面是一个目标向量(10维)在文件中的格式
> 0.91319805, 0.8839588, 0.7871948, 0.19662851, 0.34938443, 0.21287525, 0.30541795, 0.6484879, 0.084944546, 0.43906987