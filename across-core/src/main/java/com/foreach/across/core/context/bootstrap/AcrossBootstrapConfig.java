/*
 * Copyright 2019 the original author or authors
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

package com.foreach.across.core.context.bootstrap;

import com.foreach.across.core.AcrossContext;
import com.foreach.across.core.context.ModuleConfigurationSet;
import com.foreach.across.core.context.configurer.ApplicationContextConfigurer;
import com.foreach.across.core.context.module.ModuleConfigurationExtension;
import com.foreach.across.core.filters.BeanFilter;
import com.foreach.across.core.installers.InstallerSettings;
import com.foreach.across.core.transformers.ExposedBeanDefinitionTransformer;
import com.foreach.across.core.util.ClassLoadingUtils;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Represents the global bootstrap configuration for the AcrossContext.
 */
public class AcrossBootstrapConfig
{
	private final AcrossContext context;
	private final Map<String, ModuleBootstrapConfig> modules;
	private final ModuleConfigurationSet moduleConfigurationSet;

	private InstallerSettings installerSettings;

	private ExposedBeanDefinitionTransformer exposeTransformer;

	public AcrossBootstrapConfig( AcrossContext context,
	                              Map<String, ModuleBootstrapConfig> modules,
	                              ModuleConfigurationSet moduleConfigurationSet ) {
		this.context = context;
		this.moduleConfigurationSet = moduleConfigurationSet;
		setInstallerSettings( context.getInstallerSettings() );

		// Modifying the module collection itself is no longer allowed in bootstrap phase
		this.modules = Collections.unmodifiableMap( modules );
	}

	/**
	 * @return The AcrossContext this configuration is attached to.
	 */
	public AcrossContext getContext() {
		return context;
	}

	/**
	 * @return Collection of the module configurations that will be bootstrapped.
	 */
	public Collection<ModuleBootstrapConfig> getModules() {
		return modules.values();
	}

	/**
	 * @return Set of additional configurations that will be added to modules.
	 */
	public ModuleConfigurationSet getModuleConfigurationSet() {
		return moduleConfigurationSet;
	}

	public InstallerSettings getInstallerSettings() {
		return installerSettings;
	}

	public void setInstallerSettings( InstallerSettings installerSettings ) {
		Assert.notNull( installerSettings, "InstallerSettings for the AcrossContext can never be null." );
		this.installerSettings = installerSettings;
	}

	/**
	 * @param moduleName Unique name of the module.
	 * @return True if the module with that name is configured on the context.
	 */
	public boolean hasModule( String moduleName ) {
		return getModule( moduleName ) != null;
	}

	/**
	 * @param moduleName Unique name of the module.
	 * @return Bootstrap configuration instance for that module.
	 */
	public ModuleBootstrapConfig getModule( String moduleName ) {
		return modules.get( moduleName );
	}

	public ExposedBeanDefinitionTransformer getExposeTransformer() {
		return exposeTransformer;
	}

	public void setExposeTransformer( ExposedBeanDefinitionTransformer exposeTransformer ) {
		this.exposeTransformer = exposeTransformer;
	}

	/**
	 * Method to exclude one or more annotated classes from a module bootstrap configuration.
	 * If the configuration class has been added previously at any point during bootstrap configuration,
	 * it will be removed again. This method is safe to use even if the classes are not present on the classpath.
	 *
	 * @param moduleName Unique name of the module in the context.
	 * @param classNames Annotated class names.
	 * @return True if the module was present.
	 */
	@SuppressWarnings("RedundantCast")
	public boolean excludeFromModule( String moduleName, String... classNames ) {
		return excludeFromModule(
				moduleName,
				(Class[]) Stream.of( classNames )
				                .map( ClassLoadingUtils::resolveClass )
				                .filter( Objects::nonNull )
				                .toArray( Class[]::new )
		);
	}

	/**
	 * Method to exclude one or more annotated classes from a module bootstrap configuration.
	 * If the configuration class has been added previously at any point during bootstrap configuration,
	 * it will be removed again.
	 *
	 * @param moduleName       Unique name of the module in the context.
	 * @param annotatedClasses Annotated classes.
	 * @return True if the module was present.
	 */
	public boolean excludeFromModule( String moduleName, Class... annotatedClasses ) {
		for ( Class configurationClass : annotatedClasses ) {
			moduleConfigurationSet.exclude( configurationClass, moduleName );
		}
		return hasModule( moduleName );
	}

	/**
	 * Method to add one or more configuration classes to a module bootstrap configuration.
	 * The module is identified by its name.  This method is safe to use in all circumstances:
	 * if the module is not configured in the context only the return value will be false but no
	 * exception will occur.  If the class with the name is not present on the classpath, it will be skipped.
	 *
	 * @param moduleName Unique name of the module in the context.
	 * @param classNames Annotated class names.
	 * @return True if the module was present.
	 */
	@SuppressWarnings("RedundantCast")
	public boolean extendModule( String moduleName, String... classNames ) {
		return extendModule( moduleName, true, false, classNames );
	}

	/**
	 * Method to add one or more configuration classes to a module bootstrap configuration.
	 * The module is identified by its name.  This method is safe to use in all circumstances:
	 * if the module is not configured in the context only the return value will be false but no
	 * exception will occur.  If the class with the name is not present on the classpath, it will be skipped.
	 *
	 * @param moduleName Unique name of the module in the context.
	 * @param deferred   false if the configuration classes should be added before the initial configuration
	 * @param optional   true if the configuration classes should <strong>not</strong> force the module ApplicationContext to start
	 * @param classNames Annotated class names.
	 * @return True if the module was present.
	 */
	@SuppressWarnings("RedundantCast")
	public boolean extendModule( String moduleName, boolean deferred, boolean optional, String... classNames ) {
		return extendModule(
				moduleName,
				deferred,
				optional,
				(Class[]) Stream.of( classNames )
				                .map( ClassLoadingUtils::resolveClass )
				                .filter( Objects::nonNull )
				                .toArray( Class[]::new )
		);
	}

	/**
	 * Method to add one or more configuration classes to a module bootstrap configuration.
	 * The module is identified by its name.  This method is safe to use in all circumstances:
	 * if the module is not configured in the context only the return value will be false but no
	 * exception will occur.
	 *
	 * @param moduleName           Unique name of the module in the context.
	 * @param configurationClasses Annotated class instances.
	 * @return True if the module was present.
	 */
	public boolean extendModule( String moduleName, Class... configurationClasses ) {
		return extendModule( moduleName, true, false, configurationClasses );
	}

	/**
	 * Method to add one or more configuration classes to a module bootstrap configuration.
	 * The module is identified by its name.  This method is safe to use in all circumstances:
	 * if the module is not configured in the context only the return value will be false but no
	 * exception will occur.
	 *
	 * @param moduleName           Unique name of the module in the context.
	 * @param deferred             false if the configuration classes should be added before the initial configuration
	 * @param optional             true if the configuration classes should <strong>not</strong> force the module ApplicationContext to start
	 * @param configurationClasses Annotated class instances.
	 * @return True if the module was present.
	 */
	public boolean extendModule( String moduleName, boolean deferred, boolean optional, Class... configurationClasses ) {
		return extendModule(
				moduleName,
				Stream.of( configurationClasses )
				      .map( configurationClass -> ModuleConfigurationExtension.of( configurationClass, deferred, optional ) )
				      .toArray( ModuleConfigurationExtension[]::new )
		);
	}

	/**
	 * Method to add one or more configuration extensions to a module bootstrap configuration.
	 * The module is identified by its name.  This method is safe to use in all circumstances:
	 * if the module is not configured in the context only the return value will be false but no
	 * exception will occur.
	 *
	 * @param moduleName Unique name of the module in the context.
	 * @param extensions Configuration extensions
	 * @return True if the module was present.
	 */
	public boolean extendModule( String moduleName, ModuleConfigurationExtension... extensions ) {
		Stream.of( extensions ).forEach( e -> moduleConfigurationSet.register( e, moduleName ) );
		return hasModule( moduleName );
	}

	/**
	 * Method to add one or more configurers to a module bootstrap configuration.
	 * The module is identified by its name.  This method is safe to use in all circumstances:
	 * if the module is not configured in the context only the return value will be false but no
	 * exception will occur.
	 *
	 * @param moduleName  Unique name of the module in the context.
	 * @param configurers Configurers to add to the bootstrap.
	 * @return True if the module was present.
	 */
	public boolean extendModule( String moduleName, ApplicationContextConfigurer... configurers ) {
		if ( hasModule( moduleName ) ) {
			getModule( moduleName ).addApplicationContextConfigurer( configurers );
			return true;
		}

		return false;
	}

	/**
	 * Method to add one or more expose filter to a module bootstrap configuration.
	 *
	 * @param moduleName    Unique name of the module in the context.
	 * @param exposeFilters BeanFilter instances to add as additional expose filters.
	 * @return True if the module was present.
	 */
	public boolean extendModule( String moduleName, BeanFilter... exposeFilters ) {
		if ( hasModule( moduleName ) ) {
			getModule( moduleName ).addExposeFilter( exposeFilters );
			return true;
		}

		return false;
	}
}
