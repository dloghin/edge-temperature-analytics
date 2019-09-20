/*
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
package org.apache.beam.examples;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TemperatureWindowed {
	
	public static Instant getTimeStamp(String strTimeStamp) {
		String zoneOffset = strTimeStamp.substring(strTimeStamp.indexOf('+'));
		strTimeStamp = strTimeStamp.substring(0, strTimeStamp.indexOf('+'));		
		strTimeStamp = strTimeStamp.replace('T', ' ');
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");						
		LocalDateTime dateTime = LocalDateTime.parse(strTimeStamp, formatter);
		return new Instant(dateTime.toEpochSecond(ZoneOffset.of(zoneOffset)));
	}

	static class ExtractSensorsValuesFn extends DoFn<String, KV<String,Float>> {    

		@ProcessElement
		public void processElement(@Element String element, OutputReceiver<KV<String,Float>> receiver) {
			try {
				JsonObject jobject = new JsonParser().parse(element).getAsJsonObject();
				if (jobject.has("items")) {
					jobject = jobject.get("items").getAsJsonArray().get(0).getAsJsonObject();					
					if (jobject.has("readings")) {
						JsonArray jarray = jobject.getAsJsonArray("readings");
						String strTimeStamp = jobject.get("timestamp").getAsString();						
						Instant timeStamp = getTimeStamp(strTimeStamp);
						for (JsonElement reading : jarray) {						
							receiver.outputWithTimestamp(KV.of(reading.getAsJsonObject().get("station_id").getAsString(),
									reading.getAsJsonObject().get("value").getAsFloat()), timeStamp);
									
						}
					}
				}
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
			}    	   
		}
	}

	public static class FormatAsTextFn extends SimpleFunction<KV<String, Float>, String> {
		@Override
		public String apply(KV<String, Float> input) {
			return input.getKey() + ": " + input.getValue();
		}
	}	
	
	public static class AverageFloats implements SerializableFunction<Iterable<Float>, Float> {
		@Override
		public Float apply(Iterable<Float> input) {			
			float sum = 0;
			int n = 0;
			for (float item : input) {
				sum += item;
				n++;
			}
			if (n == 0)
				return sum;
			return sum / n;
		}
	}


	public static class ComputeAverage
	extends PTransform<PCollection<String>, PCollection<KV<String, Float>>> {
		@Override
		public PCollection<KV<String, Float>> expand(PCollection<String> sensors) {
			PCollection<KV<String, Float>> pc1 = sensors.apply(ParDo.of(new ExtractSensorsValuesFn()));
			PCollection<KV<String, Float>> pc2 = pc1.apply(Window.into(FixedWindows.of(Duration.standardDays(1))));		
			PCollection<KV<String, Float>> pc3 = pc2.apply(Combine.<String,Float>perKey(new AverageFloats()));		
			return pc3;
		}
	}

	public interface TemperatureOptions extends PipelineOptions {

		@Description("Path of the file to read from")
		@Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
		String getInputFile();

		void setInputFile(String value);

		@Description("Path of the file to write to")
		@Required
		String getOutput();

		void setOutput(String value);
	}

	static void run(TemperatureOptions options) {
		Pipeline p = Pipeline.create(options);
		
		// Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
		// static FormatAsTextFn() to the ParDo transform.
		p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
		.apply(new ComputeAverage())
		.apply(MapElements.via(new FormatAsTextFn()))
		.apply("WriteCounts", TextIO.write().to(options.getOutput()));

		p.run().waitUntilFinish();
	}

	public static void main(String[] args) {
		TemperatureOptions options =
				PipelineOptionsFactory.fromArgs(args).withValidation().as(TemperatureOptions.class);
		run(options);
	}

}
