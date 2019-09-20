package org.apache.edgent.samples.topology;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.edgent.analytics.math3.stat.Statistic.MAX;
import static org.apache.edgent.analytics.math3.stat.Statistic.MEAN;
import static org.apache.edgent.analytics.math3.stat.Statistic.MIN;
import static org.apache.edgent.analytics.math3.stat.Statistic.STDDEV;

import org.apache.edgent.analytics.math3.json.JsonAnalytics;
import org.apache.edgent.providers.development.DevelopmentProvider;
import org.apache.edgent.providers.direct.DirectProvider;
import org.apache.edgent.samples.utils.sensor.SimpleSensor;
import org.apache.edgent.topology.TStream;
import org.apache.edgent.topology.TWindow;
import org.apache.edgent.topology.Topology;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class FileBasedTemperatureSensors implements Runnable {
	
	private BufferedReader fileReader = null;

	private Topology topology;

	private HashMap<String, SimpleSensor> sensorMap;

	private TStream<JsonObject> sensors = null;

	public FileBasedTemperatureSensors(String file, Topology topology) {
		try {
			fileReader = new BufferedReader(new FileReader(file));    		
		}
		catch (Exception e) {			
		}
		this.topology = topology;
		sensorMap = new HashMap<String, SimpleSensor>();
	}

	public synchronized TStream<JsonObject> getEdgeSensors() {
		return sensors;
	}	

	@Override
	public void run() {
		try {
			String line = fileReader.readLine();
			while (line != null) {
				JsonObject jobject = new JsonParser().parse(line).getAsJsonObject();
				if (jobject.has("items")) {
					jobject = jobject.get("items").getAsJsonArray().get(0).getAsJsonObject();					
					if (jobject.has("readings")) {
						JsonArray jarray = jobject.getAsJsonArray("readings");
						for (JsonElement reading : jarray) {	
							String id = reading.getAsJsonObject().get("station_id").getAsString();
							Double value = reading.getAsJsonObject().get("value").getAsDouble();
							SimpleSensor sensor = sensorMap.get(id);
							if (sensor == null) {
								sensor = new SimpleSensor(id);
								sensorMap.put(id, sensor);
								if (sensors == null)
									sensors = sensor.getStream(topology);
								else
									sensors = sensors.union(sensor.getStream(topology));
							}							
							sensor.set(value);
						}
					}
				}
				Thread.sleep(1000);
				line = fileReader.readLine();
			}
		}
		catch (Exception e) {

		}
	}

	public static void main(String[] args) throws Exception {

		System.out.println("SensorsAggregates: Output will be randomly intermittent, be patient!");

		DirectProvider tp = new DevelopmentProvider();

		Topology topology = tp.newTopology("TemperatureAnalytics");

		FileBasedTemperatureSensors fbts = new FileBasedTemperatureSensors("/home/dumi/git/edge/iet-edge-book/data/temp-sg.txt", topology);
				
		Thread readThread = new Thread(fbts);
		readThread.start();
		
		try {
			Thread.sleep(5000);
		}
		catch (Exception e) {			
		}
				
		// TODO uncomment to run sequentially
		// fbts.run();

		TWindow<JsonObject,JsonElement> sensorWindow = fbts.getEdgeSensors().last(10, TimeUnit.SECONDS, j -> j.get("name"));        

		// Aggregate the windows calculating the min, max, mean and standard deviation across each window independently.
		TStream<JsonObject> sensors = JsonAnalytics.aggregate(sensorWindow, "name", "value", MIN, MAX, MEAN, STDDEV);

		sensors.print();       

		tp.submit(topology);
	}

}
