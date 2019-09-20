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
import java.util.StringTokenizer;

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

public class TemperaturePhase2 {

	public static class TokenizerMapperPhase2 
	extends Mapper<Object, Text, Text, Text> {

		private Text sensorId = new Text();		

		public void map(Object key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			
			StringTokenizer st = new StringTokenizer(value.toString(), "_");
			if (st.countTokens() >= 2) {
				sensorId.set(st.nextToken());
				context.write(sensorId, value);
			}			
		}
	}

	public static class AvgReducerPhase2
	extends Reducer<Text,Text,Text,DoubleWritable> {
		
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<Text> values,  
				Context context)
				throws IOException, InterruptedException {
			
			double n = 0.0;
			double sum = 0.0;
			for (Text val : values) {
				StringTokenizer st = new StringTokenizer(val.toString());
				try {
					st.nextToken();
					double x = Double.parseDouble(st.nextToken());				
					if (val.toString().contains("_sum")) {
						sum += x;
					}
					else
						if (val.toString().contains("_no")) {
							n += x;
						}	
				}
				catch (Exception e) {

				}
			}

			if (n > 0.0)
				sum = sum / n;
			result.set(sum);
			// output.collect(key, result);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: temperature-phase2 <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "temperature-phase2");
		job.setJarByClass(TemperaturePhase2.class);
		job.setMapperClass(TokenizerMapperPhase2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(AvgReducerPhase2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
