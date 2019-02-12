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
package com.foreach.across.config;

import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility loader for retrieving data from <strong>META-INF/across.configuration</strong> file.
 * Data should be in {@link Properties} format and the values can be fetched as comma-separated
 * strings using {@link #loadValues(String, ClassLoader)}.
 * <p/>
 * All files on the classpath matching the resource will be read.
 *
 * @author Arne Vandamme
 * @see org.springframework.core.io.support.SpringFactoriesLoader
 * @since 3.0.0
 */
public abstract class AcrossConfigurationLoader
{
	/**
	 * Location to look for configuration file (in multiple jars).
	 */
	public static final String CONFIGURATION_RESOURCE_LOCATION = "META-INF/across.configuration";

	/**
	 * Retrieve a single value from the across.configuration.
	 *
	 * @param key         key for the value
	 * @param classLoader to fetch the sources
	 * @return single value or null if none available
	 */
	public static String loadSingleValue( String key, ClassLoader classLoader ) {
		String value = null;

		try {
			Enumeration<URL> urls = ( classLoader != null ? classLoader.getResources( CONFIGURATION_RESOURCE_LOCATION ) :
					ClassLoader.getSystemResources( CONFIGURATION_RESOURCE_LOCATION ) );
			while ( urls.hasMoreElements() ) {
				URL url = urls.nextElement();
				Properties properties = PropertiesLoaderUtils.loadProperties( new UrlResource( url ) );
				if ( properties.containsKey( key ) ) {
					value = properties.getProperty( key );
				}
			}

			return value;
		}
		catch ( IOException ex ) {
			throw new IllegalArgumentException( "Unable to load [" + key + "] from location [" + CONFIGURATION_RESOURCE_LOCATION + "]", ex );
		}
	}

	/**
	 * Read comma-separated values from the {@link #CONFIGURATION_RESOURCE_LOCATION} for the given key.
	 *
	 * @param key         property for which to read the values
	 * @param classLoader to fetch the resources
	 * @return list of values
	 */
	public static List<String> loadValues( String key, ClassLoader classLoader ) {
		try {
			Enumeration<URL> urls = ( classLoader != null ? classLoader.getResources( CONFIGURATION_RESOURCE_LOCATION ) :
					ClassLoader.getSystemResources( CONFIGURATION_RESOURCE_LOCATION ) );
			List<String> result = new ArrayList<>();
			while ( urls.hasMoreElements() ) {
				URL url = urls.nextElement();
				Properties properties = PropertiesLoaderUtils.loadProperties( new UrlResource( url ) );
				String factoryClassNames = properties.getProperty( key );
				result.addAll( Arrays.asList( StringUtils.commaDelimitedListToStringArray( factoryClassNames ) ) );
			}
			return result.stream()
			             .map( org.apache.commons.lang3.StringUtils::trim )
			             .collect( Collectors.toList() );
		}
		catch ( IOException ex ) {
			throw new IllegalArgumentException( "Unable to load [" + key + "] from location [" + CONFIGURATION_RESOURCE_LOCATION + "]", ex );
		}
	}
}