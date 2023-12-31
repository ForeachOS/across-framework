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
package com.foreach.across.test;

import lombok.NonNull;

import javax.servlet.Registration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Arne Vandamme
 * @since 1.1.2
 */
public abstract class AbstractMockRegistration implements Registration.Dynamic
{
	private final String name;
	private final String className;
	private final Map<String, String> initParameters = new HashMap<>();
	private final MockAcrossServletContext servletContext;

	private boolean asyncSupported;

	protected AbstractMockRegistration( MockAcrossServletContext servletContext, @NonNull String name, String className ) {
		this.servletContext = servletContext;
		this.name = name;
		this.className = className;
	}

	public MockAcrossServletContext getServletContext() {
		return servletContext;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getClassName() {
		return className;
	}

	@Override
	public void setAsyncSupported( boolean isAsyncSupported ) {
		this.asyncSupported = isAsyncSupported;
	}

	public boolean isAsyncSupported() {
		return asyncSupported;
	}

	@Override
	public boolean setInitParameter( String name, String value ) {
		if ( !initParameters.containsKey( name ) ) {
			initParameters.put( name, value );
			return true;
		}

		return false;
	}

	@Override
	public String getInitParameter( String name ) {
		return initParameters.get( name );
	}

	@Override
	public Set<String> setInitParameters( Map<String, String> initParameters ) {
		return initParameters.entrySet()
		                     .stream()
		                     .filter( entry -> !setInitParameter( entry.getKey(), entry.getValue() ) )
		                     .map( Map.Entry::getKey )
		                     .collect( Collectors.toSet() );
	}

	@Override
	public Map<String, String> getInitParameters() {
		return Collections.unmodifiableMap( initParameters );
	}

	public Enumeration<String> getInitParameterNames() {
		return Collections.enumeration( initParameters.keySet() );
	}
}
