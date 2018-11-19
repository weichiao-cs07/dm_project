# dm_project
---
tags: NCTU, Cloud Computing, 2018/11/8, 雲端計算, Hadoop, Spark
---
:::info
Strongly recommend for creating a new virtual machine with ram over 4GB
:::
## Start Spark for machine learning

:::info
Step 1 ~ 21 is launch in hadoop-3.1.1 @master
:::
1.**~$ sudo apt-get install python-software-properties**

2.**~$ sudo apt-get install software-properties-common**

3.**~$ sudo add-apt-repository ppa:webupd8team/java**

4.**~$echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list**

5.**~$ sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823**

6.**~$ sudo apt-get update**

7.**~$ sudo apt install oracle-java8-installer ssh python3-pip sbt**

8.**~$ pip3 --no-cache-dir install numpy pandas**

9.**~$ wget http://ftp.tc.edu.tw/pub/Apache/hadoop/common/hadoop-3.1.1/hadoop-3.1.1.tar.gz**

10.**~$ wget http://ftp.twaren.net/Unix/Web/apache/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz**

11.**~$ tar -zxvf hadoop-3.1.1.tar.gz**

12.**~$ tar -zxvf spark-2.3.2-bin-hadoop2.7.tgz**

13.**~$ ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa**

14.**~$  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys**

15.**~$ chmod 0600 ~/.ssh/authorized_keys**

16.**~$ sudo mv hadoop-3.1.1 /usr/local/hadoop**

17.**~$ sudo mv spark-2.3.2-bin-hadoop2.7 /usr/local/spark**

18.**~$ sudo vim ~/.bashrc**
```
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/usr/local/hadoop
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$HADOOP_HOME/bin
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib"
export JAVA_LIBRARY_PATH=$HADOOP_HOME/lib/native:$JAVA_LIBRARY_PATH
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=python3
```

19.**~$cd /usr/local/hadoop/etc/hadoop**

20.**/usr/local/hadoop/etc/hadoop$ sudo vim hadoop-env.sh**
```
# The java implementation to use. By default, this environment
# variable is REQUIRED on ALL platforms except OS X!
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

# Location of Hadoop.  By default, Hadoop will attempt to determine
# this location based upon its execution path.
```

21.~**$source ~/.bashrc**
:::info
Step1 -Step6(Optional)
Download Anaconda and ipython notebook for excuting the python file more easily
:::
1. **Go to the provided link https://repo.continuum.io/archive/index.html and choose the Anaconda2-2.5.0-Linux-x86_64.sh to click mouse and choose copy link location**
2. **$wget  https://repo.continuum.io/archive/Anaconda2-2.5.0-Linux-x86_64.sh**
3. **~$ bash Anaconda2-2.5.0-Linux-x86_64.sh -b**
4. **~$ sudo gedit ~/.bashrc**
```
export PATH=/home/hduser/anaconda2/bin:$PATH
export ANACONDA_PATH=/home/hduser/anaconda2
export PYSPARK_DRIVER_PYTHON=$ANACONDA_PATH/bin/ipython
export PYSPARK_PYTHON=$ANACONDA_PATH/bin/python
#export PYSPARK_PYTHON=python3
(這行不用加只是和前面裝python3作為參考)
```
5.**~$ source ~/.bashrc**
6.**~$ mkdir ipython_notebook**
7.**~/ipython_notebook$ PYSPARK_DRIVER_PYTHON=ipython PYSPARK_DRIVER_PYTHON_OPTS='notebook' pyspark**
8.**After Step 7,the ipython notebook should be initiated successfully**


:::info
Step1 -Step6(Optional)
Download visual studio code in order to write and edit python files
:::
1. **Goto  https://code.visualstudio.com/ > click .deb file** 
2. **$cd ~/Downloads**
3. **~/Downloads$ sudo dpkg -i code_1.28.2-1539735992_amd64.deb**
```
Selecting previously unselected package code.
(Reading database ... 178482 files and directories currently installed.)
Preparing to unpack code_1.28.2-1539735992_amd64.deb ...
Unpacking code (1.28.2-1539735992) ...
dpkg: dependency problems prevent configuration of code:
 code depends on libgconf-2-4; however:
  Package libgconf-2-4 is not installed.

dpkg: error processing package code (--install):
 dependency problems - leaving unconfigured
Processing triggers for gnome-menus (3.13.3-11ubuntu1) ...
Processing triggers for desktop-file-utils (0.23-1ubuntu3.18.04.1) ...
Processing triggers for mime-support (3.60ubuntu1) ...
Errors were encountered while processing:
 code
```
4.**~/Downloads$ sudo apt-get install -f**
5.**redo step6 again**
6.**install the python extension within the visual studio code**



:::info
Step 1 - Step 7
download the dataset and code for running the python file
:::

1. **Go to the below link and download code  and dataset directory https://drive.google.com/drive/folders/1nQziZbsB4tWPEG93dBjcoKBEg7J_z_9K**
2. **~/Downloads$ mv dataset-20181109T060244Z-001 dataset**
3. **~/Downloads$ mv code-20181109T060152Z-001 code**
4. **~/Downloads$ cp -a code /home/hadoop**
5. **~/Downloads$ cp -a dataset /home/hadoop**
6. **~/code$ sudo vim PL_Bin.py**
(change Path variable and Path for the row_df variable)
```python=
global Path
Path="file:/home/hadoop/"
def CreateSparkContext():
    def SetLogger( sc ):
        logger = sc._jvm.org.apache.log4j
        logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
        logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
        logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

    sparkConf = SparkConf().setAppName("RunDecisionTreeBinary").set("spark.ui.showConsoleProgress", "false")
    sc = SparkContext(conf = sparkConf)
    print(("master="+sc.master))
    SetLogger(sc)
    return (sc)
sc = CreateSparkContext()
print("read data")
sqlContext = SQLContext(sc)
row_df = sqlContext.read.format("csv").option("header", "true").option("delimiter", "\t").load(Path+"dataset/train.csv")
df= row_df.select(['url','alchemy_category' ]+[col(column).cast("double").alias(column) for column in row_df.columns[4:] ] )
```

7.**~/code$ spark-submit --driver-memory 2g --master local[4] PL_Bin.py**
8.**After step 7, the program should be run successfully as shown in TA's ppt



:::info
Decision Tree - Regression programs for Dataframe one
:::
```python=
import pyspark.sql.types 
from pyspark import SparkConf, SparkContext
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf,col
from pyspark.sql import SQLContext
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorIndexer,VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator


global Path    
Path="file:/home/hadoop/" --> change to your own home directory, 
eg: if your home path is /home/timothy --> Path="file:/home/timothy/"

)
    def SetLogger( sc ):
        logger = sc._jvm.org.apache.log4j
        logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
        logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
        logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)
    sparkConf = SparkConf().setAppName("RunDecisionTreeBinary").set("spark.ui.showConsoleProgress", "false") 
    sc = SparkContext(conf = sparkConf)
    SetLogger(sc)
    return (sc)

sc=CreateSparkContext()
sqlContext = SQLContext(sc)
row_df = sqlContext.read.format("csv").option("header", "true").load(Path+"dataset/hour.csv")

print("read data")
new_row_df=row_df.drop("instant").drop("dteday").drop('yr').drop("casual").drop("registered")
new_df= new_row_df.select([ col(column).cast("double").alias(column) for column in new_row_df.columns])
train_df, test_df = new_df.randomSplit([0.7, 0.3])
train_df.cache()
test_df.cache()

assemblerInputs = new_df.columns[:-1]
print("setup pipeline")
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="tmp_features")
indexer = VectorIndexer(inputCol="tmp_features", outputCol="features", maxCategories=24)
dt = DecisionTreeRegressor(labelCol="cnt",featuresCol= "features",maxDepth=10, maxBins=100,impurity="variance")
dt_pipeline = Pipeline(stages=[assembler,indexer ,dt])

print("train model")
dt_pipelineModel = dt_pipeline.fit(train_df)

print("predict")
predicted=dt_pipelineModel.transform(test_df).select("season","mnth","hr","holiday","weekday","workingday","weathersit","temp","atemp","hum","windspeed","cnt","prediction").show(10)
print(predicted)

print("eval model")
evaluator = RegressionEvaluator(labelCol='cnt',predictionCol='prediction',metricName="rmse")
predicted_df=dt_pipelineModel.transform(test_df)
rmse = evaluator.evaluate(predicted_df)
rmse
```

:::info
Decision Tree - Regression programs for RDD one
:::

```
RDD 模板
-*- coding: UTF-8 -*-

import sys

from time import time

import pandas as pd

import matplotlib.pyplot as plt

from pyspark import SparkConf, SparkContext

from pyspark.mllib.tree import DecisionTree

from pyspark.mllib.regression import LabeledPoint

import numpy as np

from pyspark.mllib.evaluation import RegressionMetrics

import math 

def SetLogger( sc ):

    logger = sc._jvm.org.apache.log4j

    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )

    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)



def SetPath(sc):

    global Path

    if sc.master[0:5]=="local" :

        Path="file:/home/hduser/pythonsparkexample/PythonProject/"

    else:   

        Path="hdfs://master:9000/user/hduser/"

#如果您要在cluster模式執行(hadoop yarn 或Spark Stand alone)，請依照書上說明，先上傳檔案至HDFS目錄



def extract_label(record):

    label=(record[-1])

    return float(label)



def convert_float(x):

    return (0 if x=="?" else float(x))



def extract_features(record,featureEnd):

    featureSeason=[convert_float(field)  for  field in record[2]] 

    features=[convert_float(field)  for  field in record[4: featureEnd-2]]

    return  np.concatenate( (featureSeason, features))



def PrepareData(sc): 

    #----------------------1.匯入並轉換資料-------------

    print("開始匯入資料...")

    rawDataWithHeader = sc.textFile(Path+"data/hour.csv")

    header = rawDataWithHeader.first() 

    rawData = rawDataWithHeader.filter(lambda x:x !=header)    

    lines = rawData.map(lambda x: x.split(","))

    print (lines.first())

    print("共計：" + str(lines.count()) + "筆")

    #----------------------2.建立訓練評估所需資料 RDD[LabeledPoint]-------------

    labelpointRDD = lines.map(lambda r:LabeledPoint(

                                                    extract_label(r), 

                                                    extract_features(r,len(r) - 1)))



    print labelpointRDD.first()

    #----------------------3.以隨機方式將資料分為3部份並且回傳-------------

    (trainData, validationData, testData) = labelpointRDD.randomSplit([8, 1, 1])

    print("將資料分trainData:" + str(trainData.count()) + 

             "   validationData:" + str(validationData.count()) +

             "   testData:" + str(testData.count()))

    #print labelpointRDD.first()

    return (trainData, validationData, testData) #回傳資料



def PredictData(sc,model): 

    #----------------------1.匯入並轉換資料-------------

    print("開始匯入資料...")

    rawDataWithHeader = sc.textFile(Path+"data/hour.csv")

    header = rawDataWithHeader.first() 

    rawData = rawDataWithHeader.filter(lambda x:x !=header)    

    lines = rawData.map(lambda x: x.split(","))

    #print (lines.first())

    print("共計：" + str(lines.count()) + "筆")

    #----------------------2.建立訓練評估所需資料 LabeledPoint RDD-------------

    labelpointRDD = lines.map(lambda r: LabeledPoint(

                                                     extract_label(r), 

                                                     extract_features(r,len(r) - 1)))

    #----------------------3.定義字典----------------

    SeasonDict = { 1 : "春",  2 : "夏",  3 :"秋",  4 : "冬"   }

    HoildayDict={  0 : "非假日", 1 : "假日"  }  

    WeekDict = {0:"一",1:"二",2:"三",3:"四",4 :"五",5:"六",6:"日"}

    WorkDayDict={ 1 : "工作日",  0 : "非工作日"  }

    WeatherDict={ 1 : "晴",  2 : "陰",  3 : "小雨", 4 : "大雨" }

    #----------------------4.進行預測並顯示結果--------------

    for lp in labelpointRDD.take(100):

        predict = int(model.predict(lp.features))

        label=lp.label

        features=lp.features

        result = ("正確" if  (label == predict) else "錯誤")

        error = math.fabs(label - predict)

        dataDesc="  特徵: "+SeasonDict[features[0]] +"季,"+\

                            str(features[1]) + "月," +\

                            str(features[2]) +  "時,"+ \

                            HoildayDict[features[3]] +","+\

                            "星期"+WeekDict[features[4]]+","+ \

                            WorkDayDict[features[5]]+","+\

                            WeatherDict[features[6]]+","+\

                            str(features[7] * 41)+ "度,"+\

                            "體感" + str(features[8] * 50) + "度," +\

                            "溼度" + str(features[9] * 100) + ","+\

                            "風速" + str(features[10] * 67) +\

                            " ==> 預測結果:" + str(predict )+\

                            "  , 實際:" + str(label) + result +",  誤差:" + str(error)

        print dataDesc

        



    



def evaluateModel(model, validationData):

    score = model.predict(validationData.map(lambda p: p.features))

    scoreAndLabels=score.zip(validationData.map(lambda p: p.label))

    metrics = RegressionMetrics(scoreAndLabels)

    RMSE=metrics.rootMeanSquaredError

    return( RMSE)

 



def trainEvaluateModel(trainData,validationData,

                                           impurityParm, maxDepthParm, maxBinsParm):

    startTime = time()

    model = DecisionTree.trainRegressor(trainData, 

                                  categoricalFeaturesInfo={}, \

                                  impurity=impurityParm, 

                                  maxDepth=maxDepthParm, 

                                  maxBins=maxBinsParm)

    RMSE = evaluateModel(model, validationData)

    duration = time() - startTime

    print    "訓練評估：使用參數" + \

                " impurityParm= %s"%impurityParm+ \

                "  maxDepthParm= %s"%maxDepthParm+ \

                "  maxBinsParm = %d."%maxBinsParm + \

                 "  所需時間=%d"%duration + \

                 "  結果RMSE = %f " % RMSE 

    return (RMSE,duration, impurityParm, maxDepthParm, maxBinsParm,model)





def evalParameter(trainData, validationData, evaparm,impurityList, maxDepthList, maxBinsList):

    metrics = [trainEvaluateModel(trainData, validationData,  impurity,maxdepth,  maxBins  ) 

                            for impurity in impurityList 

                            for maxdepth in maxDepthList  

                            for maxBins in maxBinsList ]

    if evaparm=="impurity":

        IndexList=impurityList[:]

    elif evaparm=="maxDepth":

        IndexList=maxDepthList[:]

    elif evaparm=="maxBins":

        IndexList=maxBinsList[:]



    df = pd.DataFrame(metrics,index=IndexList,

                      columns=['RMSE', 'duration','impurityParm', 'maxDepthParm', 'maxBinsParm','model'])

    

    showchart(df,evaparm,'RMSE','duration',0,200 )

    

    

def showchart(df,evalparm ,barData,lineData,yMin,yMax):

    ax = df[barData].plot(kind='bar', title =evalparm,figsize=(10,6),legend=True, fontsize=12)

    ax.set_xlabel(evalparm,fontsize=12)

    ax.set_ylim([yMin,yMax])

    ax.set_ylabel(barData,fontsize=12)

    ax2 = ax.twinx()

    ax2.plot(df[[lineData ]].values, linestyle='-', marker='o', linewidth=2.0,color='r')

    plt.show()

    

def evalAllParameter(training_RDD, validation_RDD, impurityList, maxDepthList, maxBinsList):    

    metrics = [trainEvaluateModel(trainData, validationData,  impurity,maxdepth,  maxBins  ) 

                        for impurity in impurityList 

                        for maxdepth in maxDepthList  

                        for maxBins in maxBinsList ]

    Smetrics = sorted(metrics, key=lambda k: k[0])

    bestParameter=Smetrics[0]

    

    print("調校後最佳參數：impurity:" + str(bestParameter[2]) + 

            "  ,maxDepth:" + str(bestParameter[3]) + 

            "  ,maxBins:" + str(bestParameter[4])   + 

            "  ,結果RMSE = " + str(bestParameter[0]))

    

    return bestParameter[5]



def  parametersEval(training_RDD, validation_RDD):

    

    print("----- 評估maxDepth參數使用 ---------")

    evalParameter(training_RDD, validation_RDD,"maxDepth", 

                              impurityList=["variance"],       

                              maxDepthList =[3, 5, 10, 15, 20, 25]  ,

                              maxBinsList=[10])

    print("----- 評估maxBins參數使用 ---------")

    evalParameter(training_RDD, validation_RDD,"maxBins", 

                              impurityList=["variance"],        

                              maxDepthList=[10],                   

                              maxBinsList=[3, 5, 10, 50, 100, 200 ])  







def CreateSparkContext():

    sparkConf = SparkConf()                                            \

                         .setAppName("RunDecisionTreeRegression")           \

                         .set("spark.ui.showConsoleProgress", "false") 

    sc = SparkContext(conf = sparkConf)

    print ("master="+sc.master)    

    SetLogger(sc)

    SetPath(sc)

    return (sc)



if __name__ == "__main__":

    print("RunDecisionTreeRegression")

    sc=CreateSparkContext()

    print("==========資料準備階段===============")

    (trainData, validationData, testData) =PrepareData(sc)

    trainData.persist(); validationData.persist(); testData.persist()

    print("==========訓練評估階段===============")

    (AUC,duration, impurityParm, maxDepthParm, maxBinsParm,model)= \

             trainEvaluateModel(trainData, validationData, "variance", 10, 100)

    if (len(sys.argv) == 2) and (sys.argv[1]=="-e"):

        parametersEval(trainData, validationData)

    elif   (len(sys.argv) == 2) and (sys.argv[1]=="-a"): 

        print("-----所有參數訓練評估找出最好的參數組合---------")  

        model=evalAllParameter(trainData, validationData,

                          ["variance"],

                          [3, 5, 10, 15, 20, 25], 

                          [3, 5, 10, 50, 100, 200 ])

    print("==========測試階段===============")

    RMSE = evaluateModel(model, testData)

    print("使用testata測試最佳模型,結果 RMSE:" + str(RMSE))

    print("==========預測資料===============")

    PredictData(sc, model)

    #print   model.toDebugString()
```
```
https://gist.github.com/jonathan0405/852bdc4ea3e2bd912031504f2c2f178b


https://gist.github.com/timmychen1996/ffb0dbc1d416e9ebd9ce09866c5a1f06

```

:::info
Scala
https://github.com/dyingapple/Fog_hw4/blob/master/code/DS_Bike/src/main/scala/DS_Bike.scala
// change url
:::
1.**~/code/DS_Bin$ sbt package(Step 1)**
2.**After Step 1, there are three main directory that you can see. Project, src, target and the build.sbt file**
3.**edit the scale file in src/main/scala/<<your scala file>>**
4.**After editing the scala file, neeeded to do the step 1 again**
5.**~/code/DS_Bin$ spark-submit target/scala-2.11/simple-project_2.11-1.0.jar(Step 3)**
5.**After step 5 , you can see the results successfully**

```
Problem answer:
From the time for executing the RDD version and DataFrame version, 
it can tell that RDD version takes less time compared to the DataFrame version. 
And the reason is that DataFrame’s code were running as SQL database; 
however, the RDD were the main features about how the Spark
architecture deal with the data. It has two main parts which are
transformations and actions, only when the actions were called will 
the RDD dataframe start computing. This helps reducing unneeded 
computation when performing the certain task. 
Besides, the data form of RDD does not contain the column name like 
DataFrame version. Because of this feature, the data within the RDD 
can be spilt across different machines which meets the parallelizing requirement.  
The reason that we use regression instead of the classification were mainly depends on what we want to predict 
from the dataset. For the example provided by the TA whose dataset 
were asking whether the given url were evergreen or not. 
Since the outcome were categorical whose value were neither 1 nor 0, 
it perfectly suits for the classification problem. But for our biking dataset, most of the features were numbers so did the outcome which 
is the rental number that we try to predict. This kind of prediction 
were mainly numerical and it is more appropriate to use regression 
as a model. When using the regression model, it is much more 
clearly seeing how the features may change the prediction since 
regression model using rmse as a standard to minimize the error. 
And the rmse was considering all the data points, so how the 
data distributed may affect the outcome.
```












```

