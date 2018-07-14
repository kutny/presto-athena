/**
 * 
 */
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.athena;

import com.facebook.presto.plugin.jdbc.JdbcPlugin;

/**
 * Initial class injected into PrestoDB via SPI.
 * 
 * @author Jiri Koutny
 *
 */
public class AthenaPlugin extends JdbcPlugin {

	/**
	 * Athena Plugin Constructor
	 */
	public AthenaPlugin() {
		//name of the connector and the module implementation
		super("athena", new AthenaClientModule());
	}
}
