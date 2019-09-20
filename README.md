# Source Code for Chapter 9 - Distributed Big Data Computing Platforms for Edge Computing

This repository contains the code corresponding to the examples in Chapter 9 *"Distributed Big Data Computing Platforms for Edge Computing"* of IET book *"Edge Computing: Models, Technologies and Applications"*. More details about this book can be found [here](https://sites.google.com/site/ietedgebookproposal/chapters).

## Hadoop

We use `hadoop-3.2.0`. Please download it first ([hadoop.apache.org/release/3.2.0.html](https://hadoop.apache.org/release/3.2.0.html)) and unzip it in a `hadoop-3.2.0` folder. We suppose this folder is in the root of this repository, at the same level as `hadoop` folder.

The code is implemented in:

- [hadoop/src/main/java/org/apache/hadoop/examples/temperature/Temperature.java](hadoop/src/main/java/org/apache/hadoop/examples/temperature/Temperature.java)

- [hadoop/src/main/java/org/apache/hadoop/examples/temperature/TemperaturePhase1.java](hadoop/src/main/java/org/apache/hadoop/examples/temperature/TemperaturePhase1.java)

- [hadoop/src/main/java/org/apache/hadoop/examples/temperature/TemperaturePhase2.java](hadoop/src/main/java/org/apache/hadoop/examples/temperature/TemperaturePhase2.java)

To compile the code, go to the `hadoop` folder in this repository:

```
cd hadoop
mvn clean package
```

The jar file is in `target/hadoop-mapreduce-temperature-3.2.0.jar`.

You can run the code in stand-alone Hadoop mode. If you want to setup a Hadoop cluster with HDFS, please follow the tutorial at [hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html).

### Run in One Phase

To run the code:

```
cd hadoop-3.2.0
mkdir input
cp ../data/temp-sg.txt input/
./bin/hadoop jar ../hadoop-temp/target/hadoop-mapreduce-temperature-3.2.0.jar temperature input output
cat output/part-r-00000
```

you should get:

```
S100	28.213852813852785
S104	27.87857142857144
S106	27.79544349939253
S107	28.31384615384636
S108	28.660815047021938
S109	27.962962962963015
S111	27.573355629877394
S115	28.39571836346337
S116	28.54836065573762
S117	28.358946149391905
S121	28.08334317779083
S122	27.59476635514016
S24	28.5209480122324
S43	28.55350206003531
S44	27.83325917686318
S50	27.626717112922
S60	28.229866356769268
```

### Run in Two Phases (Edge-Cloud)

To run the code, make sure you have the input folder, as above, and output to an intermediate folder:

```
cd hadoop-3.2.0
mkdir input
cp ../data/temp-sg.txt input/
./bin/hadoop jar ../hadoop/target/hadoop-mapreduce-temperature-3.2.0.jar temperature-phase1 input inter
```

Partial (intermediate) results are in `inter` folder. To run the second phase, make sure `inter` folder contains only the `part-r-00000`. You should also delete the existing `output` folder (do this before every new run):

```
rm -r output
rm inter/_SUCCESS
./bin/hadoop jar ../hadoop/target/hadoop-mapreduce-temperature-3.2.0.jar temperature-phase2 inter output
cat output/part-r-00000
```

you should get:

```
S100	28.213852813852785
S104	27.87857142857144
S106	27.79544349939253
S107	28.31384615384636
S108	28.660815047021938
S109	27.962962962963015
S111	27.573355629877394
S115	28.39571836346337
S116	28.54836065573762
S117	28.358946149391905
S121	28.08334317779083
S122	27.59476635514016
S24	28.5209480122324
S43	28.55350206003531
S44	27.83325917686318
S50	27.626717112922
S60	28.229866356769268
```

which is the same as the result of the single-phase MapReduce job above.


## Spark

To run Spark, please download it ([spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)) and unzip it in a folder. Let's suppose the folder is called `spark-platform` and it's on the same level as spark folder in this repository.

Run Scala Spark Shell and load our Scala script from this repository, `/spark` folder. Don't forget to modify the path to input and output files in [temperature.scala](spark/temperature.scala) file.

```
cd spark-platform/bin
./spark-shell
> :load ../../spark/temperature.scala
```

The results are in `data/spark-out`. If you run:

```
cat part-0000* | tr -d "()" | tr ',' '\t'
```

you should get:

```
S115	28.398764258555136
S43	28.555647058823524
S104	27.88029670839129
S44	27.835297387437464
S116	28.55051194539249
S106	27.79836065573771
S117	28.360995370370365
S60	28.231939605110345
S121	28.08606847697755
S24	28.520948012232413
S107	28.31537585421413
S108	28.66081504702193
S50	27.62867946480511
S111	27.575097493036196
S122	27.596870621205042
S100	28.217243510506798
S109	27.96545342381245
```


## Beam

The example code is implemented in:

- [beam/src/main/java/org/apache/beam/examples/Temperature.java](beam/src/main/java/org/apache/beam/examples/Temperature.java)

- [beam/src/main/java/org/apache/beam/examples/TemperatureWindowed.java](beam/src/main/java/org/apache/beam/examples/TemperatureWindowed.java)

To run our example on Beam's direct runner, use the following Maven commands. You should delete the existing `output` folder.

```
cd beam
rm -rf output
mvn clean package
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.Temperature -Dexec.args="--inputFile=../data/temp-sg.txt --output=output/avg" -Pdirect-runner
cat output/avg-0000*
```

You should get:

```
S115: 28.537462
S50: 27.628918
S109: 27.994415
S43: 28.736534
S106: 27.859465
S44: 28.14845
S108: 28.964952
S107: 28.3132
S60: 28.258558
S100: 28.239708
S24: 28.44321
S111: 27.625013
S104: 28.225807
S121: 28.223034
S117: 28.459747
S116: 28.462505
S122: 27.948292
```

For the windowed example:

```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.TemperatureWindowed -Dexec.args="--inputFile=../data/temp-sg.txt --output=output-windowed/avg" -Pdirect-runner
cat output-windowed/avg-0000*
```

You should get:

```
S117: 28.383629
S106: 27.911184
S108: 28.972845
S111: 27.5561
S122: 27.853048
S104: 28.136549
S109: 27.992222
S116: 28.324923
S121: 28.184568
S115: 28.461754
S43: 28.721031
S24: 28.345
S44: 28.034462
S50: 27.584888
S100: 28.220465
S60: 28.193674
S107: 28.185816
```

## Apache Edgent

We added our sources in `incubator-edgent-samples`. We added these sources:

- [edgent/topology/src/main/java/org/apache/edgent/samples/topology/FileBasedTemperatureSensors.java](edgent/topology/src/main/java/org/apache/edgent/samples/topology/FileBasedTemperatureSensors.java) in `org.apache.edgent.samples.topology` 

- [edgent/utils/src/main/java/org/apache/edgent/samples/utils/sensor/SimpleSensor.java](edgent/utils/src/main/java/org/apache/edgent/samples/utils/sensor/SimpleSensor.java) in `org.apache.edgent.samples.utils.sensor`.

To run the topology:

```
cd edgent
./mvnw clean package
cd topology
./run-sample.sh FileBasedTemperatureSensors
```

# Authors

Dumitrel Loghin, Lavanya Ramapantulu, Yong Meng Teo



