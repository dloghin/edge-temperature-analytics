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
import java.util.LinkedList;
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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Temperature {

	public static List<Pair<String,Double>> processSensorRecord(String record) {
		List<Pair<String,Double>> sensors = new LinkedList<>();
		try {
			JsonObject jobject = new JsonParser().parse(record).getAsJsonObject();
			if (jobject.has("items")) {
				jobject = jobject.get("items").getAsJsonArray().get(0).getAsJsonObject();					
				if (jobject.has("readings")) {
					JsonArray jarray = jobject.getAsJsonArray("readings");
					for (JsonElement reading : jarray) {
						sensors.add(Pair.of(reading.getAsJsonObject().get("station_id").getAsString(),
								reading.getAsJsonObject().get("value").getAsDouble()));						
					}
				}
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
		}
		return sensors;
	}

	public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, DoubleWritable> {

		private Text sensorId = new Text();
		private DoubleWritable sensorVal = new DoubleWritable();

		public void map(Object key, Text value, 
				Context context) 
				throws IOException, InterruptedException {				
			List<Pair<String,Double>> sensors = processSensorRecord(value.toString());
			for (Pair<String,Double> sensor : sensors) {
				sensorId.set(sensor.getKey());
				sensorVal.set(sensor.getValue());
				context.write(sensorId, sensorVal);
			}    	
		}
	}

	public static class AvgReducer 
	extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
		
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values, 
				Context context) 
				throws IOException, InterruptedException {
			int n = 0;
			double avg = 0.0;
			for (DoubleWritable sensors : values) {
				avg += sensors.get();
				n++;
			}
			if (n > 0)
				avg = avg / n;
			result.set(avg);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: temperature <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "temperature");
		job.setJarByClass(Temperature.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);		
		job.setReducerClass(AvgReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,	new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
