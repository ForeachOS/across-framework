/*
 * Copyright 2014 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.foreach.across.core.database;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * Class encapsulating general database properties to be used when creating/updating the schema.
 * With specific functionality for table names and renaming tables.
 */
public class SchemaConfiguration
{
	private String defaultSchema = "";
	private Collection<SchemaObject> tables = new ArrayList<>();
	private Map<String, String> properties = new HashMap<>();

	public SchemaConfiguration() {
	}

	public SchemaConfiguration( String defaultSchema ) {
		this.defaultSchema = defaultSchema;
	}

	public SchemaConfiguration( Collection<SchemaObject> tables ) {
		this.tables = new ArrayList<>( tables );
	}

	/**
	 * Configures the default schema to be used
	 *
	 * @param defaultSchema
	 */
	public void setDefaultSchema( String defaultSchema ) {
		this.defaultSchema = defaultSchema;
	}

	public String getDefaultSchema() {
		return defaultSchema;
	}

	public Collection<SchemaObject> getTables() {
		return Collections.unmodifiableCollection( tables );
	}

	public void renameTable( String original, String name ) {
		boolean found = false;
		for ( SchemaObject databaseObject : tables ) {
			if ( StringUtils.equals( original, databaseObject.getOriginalName() ) ) {
				databaseObject.setCurrentName( name );
				found = true;
			}
		}

		if ( !found ) {
			SchemaObject object = new SchemaObject( StringUtils.lowerCase( "table." + original ), original );
			object.setCurrentName( name );

			tables.add( object );
		}
	}

	public String getCurrentTableName( String original ) {
		for ( SchemaObject databaseObject : tables ) {
			if ( StringUtils.equals( original, databaseObject.getOriginalName() ) ) {
				return databaseObject.getCurrentName();
			}
		}

		return original;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperty( String name, String value ) {
		properties.put( name, value );
	}

	public String getProperty( String name ) {
		return properties.get( name );
	}
}
