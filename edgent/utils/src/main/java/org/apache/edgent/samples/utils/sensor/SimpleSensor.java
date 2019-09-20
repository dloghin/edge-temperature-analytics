/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 */

package org.apache.edgent.samples.utils.sensor;

import java.util.concurrent.TimeUnit;

import org.apache.edgent.function.Supplier;
import org.apache.edgent.topology.TStream;
import org.apache.edgent.topology.Topology;

import com.google.gson.JsonObject;

public class SimpleSensor implements Supplier<JsonObject> {
	private static final long serialVersionUID = 1L;    
	private double value;
	private String name;	

	public SimpleSensor(String name) {
		this.name = name;
	}
	
	public void set(double value) {
		this.value = value;
	}

	/** Get the next sensor value as described in the class documentation. */
	@Override
	public JsonObject get() {		
		JsonObject obj = new JsonObject();
		obj.addProperty("name", name);
		obj.addProperty("value", value);
		return obj;
	}

	public TStream<JsonObject> getStream(Topology topology) {
		return topology.poll(this, 1, TimeUnit.SECONDS);
	}
}
