/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples.temperature;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TemperaturePhase1 {

	public static class TokenizerMapperPhase1
	extends Mapper<Object, Text, Text, DoubleWritable> {

		private Text sensorId = new Text();
		private DoubleWritable sensorVal = new DoubleWritable();

		public void map(Object key, Text value, 
				Context context) 
				throws IOException, InterruptedException {				
			List<Pair<String,Double>> sensors = Temperature.processSensorRecord(value.toString());
			for (Pair<String,Double> sensor : sensors) {
				sensorId.set(sensor.getKey());
				sensorVal.set(sensor.getValue());
				context.write(sensorId, sensorVal);
			}    	
		}
	}

	public static class AvgReducerPhase1
	extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
		private DoubleWritable resultSum = new DoubleWritable();
		private DoubleWritable resultNo = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values, 
				Context context)
				throws IOException, InterruptedException {
			int n = 0;
			double sum = 0.0;
			for (DoubleWritable sensors : values) {
				sum += sensors.get();
				n++;
			}
			if (n > 0) {
				Text keySum = new Text(key.toString() + "_sum");
				Text keyNo = new Text(key.toString() + "_no");
				resultSum.set(sum);
				resultNo.set(n);
				context.write(keySum, resultSum);
				context.write(keyNo, resultNo);				
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: temperature-phase1 <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "temperature-phase1");
		job.setJarByClass(TemperaturePhase1.class);
		job.setMapperClass(TokenizerMapperPhase1.class);
		job.setReducerClass(AvgReducerPhase1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
