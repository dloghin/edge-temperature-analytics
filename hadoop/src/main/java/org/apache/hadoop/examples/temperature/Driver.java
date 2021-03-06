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

import org.apache.hadoop.util.ProgramDriver;

/**
 * A description of an example program based on its class and a 
 * human-readable description.
 */
public class Driver {
  
  public static void main(String argv[]){
	System.out.println("Here");
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {    	
      pgd.addClass("temperature", Temperature.class, "Get Average Temperature per Sensor");
      pgd.addClass("temperature-phase1", TemperaturePhase1.class, "Get Average Temperature per Sensor - Phase 1");
      pgd.addClass("temperature-phase2", TemperaturePhase2.class, "Get Average Temperature per Sensor - Phase 2");
      exitCode = pgd.run(argv);
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
	
