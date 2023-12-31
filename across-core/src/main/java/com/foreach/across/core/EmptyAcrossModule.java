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

package com.foreach.across.core;

import com.foreach.across.core.context.configurer.ApplicationContextConfigurer;

import java.util.Set;

/**
 * Default empty AcrossModule with a configurable name. This module does not do anything by default,
 * but can be used to inject in an AcrossContext to satisfy dependencies.
 * <p/>
 * Optionally a set of annotated classes can be added that should be loaded in this module.
 *
 * @author Arne Vandamme
 * @since 2.0.0
 */
public final class EmptyAcrossModule extends AcrossModule
{
	private final String name;

	public EmptyAcrossModule( String name, Class<?>... annotatedClasses ) {
		this( name, false, annotatedClasses );
	}

	protected EmptyAcrossModule( String name, boolean optional, Class<?>[] annotatedClasses ) {
		this.name = name;

		if ( annotatedClasses.length > 0 ) {
			addApplicationContextConfigurer( annotatedClasses );
		}
		else if ( !optional ) {
			addApplicationContextConfigurer( Object.class );
		}
	}

	/**
	 * @return Name of this module.  Should be unique within a configured AcrossContext.
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * @return Description of the content of this module.
	 */
	@Override
	public String getDescription() {
		return "Standard empty module implementation - configurable by name to allow for faking dependencies.";
	}

	@Override
	protected void registerDefaultApplicationContextConfigurers( Set<ApplicationContextConfigurer> contextConfigurers ) {
		// No context configurers should be loaded by default
	}

	@Override
	protected void registerDefaultInstallerContextConfigurers( Set<ApplicationContextConfigurer> installerContextConfigurers ) {
		// No context configurers should be loaded by default
	}

	@Override
	public String[] getInstallerScanPackages() {
		return new String[0];
	}

	/**
	 * Creates an empty module that will not be bootstrapped unless any actual configuration with components is added.
	 *
	 * @param moduleName name of the module
	 * @return module
	 */
	public static EmptyAcrossModule optional( String moduleName ) {
		return new EmptyAcrossModule( moduleName, true, new Class[0] );
	}
}
