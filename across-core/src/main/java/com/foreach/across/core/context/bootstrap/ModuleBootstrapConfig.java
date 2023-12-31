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

import com.foreach.across.core.AcrossModule;
import com.foreach.across.core.context.ExposedModuleBeanRegistry;
import com.foreach.across.core.context.configurer.AnnotatedClassConfigurer;
import com.foreach.across.core.context.configurer.ApplicationContextConfigurer;
import com.foreach.across.core.context.info.AcrossModuleInfo;
import com.foreach.across.core.context.module.ModuleConfigurationExtension;
import com.foreach.across.core.filters.BeanFilter;
import com.foreach.across.core.installers.InstallerSettings;
import com.foreach.across.core.transformers.ExposedBeanDefinitionTransformer;
import com.foreach.across.core.util.ClassLoadingUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.lang3.ArrayUtils;

import java.util.*;
import java.util.stream.Stream;

/**
 * Represents the actual bootstrap configuration of an AcrossModule.
 * This is the configuration that can be modified during the bootstrap process,
 * without changing the initial AcrossModule configuration.
 * <p>
 * Should be deprecated in the future, replace by {@link com.foreach.across.core.context.module.AcrossModuleBootstrapConfiguration}
 *
 * @see com.foreach.across.core.context.module.AcrossModuleBootstrapConfiguration
 */
public class ModuleBootstrapConfig
{
	private final AcrossModuleInfo moduleInfo;

	private BeanFilter exposeFilter;
	private ExposedBeanDefinitionTransformer exposeTransformer;
	private Set<ApplicationContextConfigurer> applicationContextConfigurers = new LinkedHashSet<>();
	private Set<ApplicationContextConfigurer> installerContextConfigurers = new LinkedHashSet<>();
	private Set<String> excludedAnnotatedClasses = new LinkedHashSet<>();

	/**
	 * List of module configuration extensions that should be added when bootstrapping the module.
	 */
	@Getter
	@Setter
	@NonNull
	private Set<ModuleConfigurationExtension> configurationExtensions = new LinkedHashSet<>();

	private Collection<Object> installers = new LinkedList<>();
	private InstallerSettings installerSettings;
	private Collection<ExposedModuleBeanRegistry> previouslyExposedBeans = new ArrayList<>();

	private boolean hasComponents = false;

	public ModuleBootstrapConfig( AcrossModuleInfo moduleInfo ) {
		this.moduleInfo = moduleInfo;
	}

	public AcrossModule getModule() {
		return moduleInfo.getModule();
	}

	public String getModuleName() {
		return moduleInfo.getName();
	}

	/**
	 * @return module name as well as possible aliases
	 */
	public String[] getAllModuleNames() {
		return ArrayUtils.addAll( new String[] { getModuleName() }, moduleInfo.getAliases() );
	}

	public BeanFilter getExposeFilter() {
		return exposeFilter;
	}

	public void setExposeFilter( BeanFilter exposeFilter ) {
		this.exposeFilter = exposeFilter;
	}

	public int getBootstrapIndex() {
		return moduleInfo.getIndex();
	}

	/**
	 * Adds filters that will be used after module bootstrap to copy beans to the parent context.
	 * This adds the filters to the already configured expose filter.
	 *
	 * @param exposeFilters One or more filters that beans should match to be exposed to other modules.
	 */
	public void addExposeFilter( BeanFilter... exposeFilters ) {
		BeanFilter[] members = exposeFilters;
		BeanFilter current = getExposeFilter();

		if ( current != null ) {
			members = new BeanFilter[members.length + 1];
			members[0] = current;
			System.arraycopy( exposeFilters, 0, members, 1, exposeFilters.length );
		}

		setExposeFilter( BeanFilter.composite( members ) );
	}

	/**
	 * Expose beans matching any of the classes or annotations.
	 * If the class specified is not present on the classpath, it will be ignored.
	 *
	 * @param classNames to add
	 */
	public void exposeClass( String... classNames ) {
		expose(
				(Class<?>[]) Stream.of( classNames )
				                   .map( ClassLoadingUtils::resolveClass )
				                   .filter( Objects::nonNull )
				                   .toArray( Class[]::new )
		);
	}

	/**
	 * Expose beans matching any of the classes.  If the class is an annotation, beans having the annotation
	 * will be matched, otherwise beans assignable to the target class will match.
	 *
	 * @param classOrAnnotations to match
	 */
	public void expose( Class<?>... classOrAnnotations ) {
		setExposeFilter( BeanFilter.composite(
				getExposeFilter(),
				BeanFilter.instances( classOrAnnotations ),
				BeanFilter.annotations( classOrAnnotations )
		) );
	}

	/**
	 * Exposed all beans with the given names.
	 *
	 * @param beanNames that need to be exposed
	 */
	public void expose( String... beanNames ) {
		setExposeFilter( BeanFilter.composite(
				getExposeFilter(),
				BeanFilter.beanNames( beanNames )
		) );
	}

	public ExposedBeanDefinitionTransformer getExposeTransformer() {
		return exposeTransformer;
	}

	public void setExposeTransformer( ExposedBeanDefinitionTransformer exposeTransformer ) {
		this.exposeTransformer = exposeTransformer;
	}

	public Set<ApplicationContextConfigurer> getApplicationContextConfigurers() {
		return Collections.unmodifiableSet( applicationContextConfigurers );
	}

	public void setApplicationContextConfigurers( Set<ApplicationContextConfigurer> applicationContextConfigurers ) {
		this.applicationContextConfigurers.clear();
		this.hasComponents = false;
		addApplicationContextConfigurers( applicationContextConfigurers );
	}

	public void addApplicationContextConfigurer( Class<?>... annotatedClasses ) {
		addApplicationContextConfigurer( new AnnotatedClassConfigurer( annotatedClasses ) );
	}

	public void addApplicationContextConfigurer( boolean optional, Class<?>... annotatedClasses ) {
		addApplicationContextConfigurer( optional, new AnnotatedClassConfigurer( annotatedClasses ) );
	}

	public void addApplicationContextConfigurer( ApplicationContextConfigurer... configurers ) {
		addApplicationContextConfigurers( Arrays.asList( configurers ) );
	}

	public void addApplicationContextConfigurer( boolean optional, ApplicationContextConfigurer... configurers ) {
		addApplicationContextConfigurers( optional, Arrays.asList( configurers ) );
	}

	public void addApplicationContextConfigurers( Collection<ApplicationContextConfigurer> configurers ) {
		addApplicationContextConfigurers( false, configurers );
	}

	public void addApplicationContextConfigurers( boolean optional, Collection<ApplicationContextConfigurer> configurers ) {
		applicationContextConfigurers.addAll( configurers );

		if ( !optional && configurers.stream().anyMatch( ApplicationContextConfigurer::hasComponents ) ) {
			hasComponents = true;
		}
	}

	/**
	 * Add a number of configurations that should be imported when bootstrapping this module.
	 * By default you usually want to import deferred configuration, in order to override bean definitions from the original module.
	 *
	 * @param deferred             true if the configuration should be added after the initial module configuration and extensions
	 * @param optional             true if the configuration should <strong>not</strong> force the module ApplicationContext to be started
	 * @param configurationClasses to import
	 */
	public void extendModule( boolean deferred, boolean optional, Class... configurationClasses ) {
		Stream.of( configurationClasses )
		      .map( clazz -> ModuleConfigurationExtension.of( clazz.getName(), deferred, optional ) )
		      .forEach( configurationExtensions::add );
	}

	/**
	 * Add a number of configurations that should be imported when bootstrapping this module.
	 * By default you usually want to import deferred configuration, in order to override bean definitions from the original module.
	 *
	 * @param deferred             true if the configuration should be added after the initial module configuration and extensions
	 * @param optional             true if the configuration should <strong>not</strong> force the module ApplicationContext to be started
	 * @param configurationClasses to import
	 */
	public void extendModule( boolean deferred, boolean optional, String... configurationClasses ) {
		Stream.of( configurationClasses )
		      .filter( className -> ClassLoadingUtils.resolveClass( className ) != null )
		      .map( className -> ModuleConfigurationExtension.of( className, deferred, optional ) )
		      .forEach( configurationExtensions::add );
	}

	/**
	 * Add a number of configuration extensions that should be added when bootstrapping this module.
	 *
	 * @param extensions to add
	 */
	public void extendModule( ModuleConfigurationExtension... extensions ) {
		configurationExtensions.addAll( Arrays.asList( extensions ) );
	}

	public Set<ApplicationContextConfigurer> getInstallerContextConfigurers() {
		return installerContextConfigurers;
	}

	public void setInstallerContextConfigurers( Set<ApplicationContextConfigurer> installerContextConfigurers ) {
		this.installerContextConfigurers = installerContextConfigurers;
	}

	public void addInstallerContextConfigurer( Class<?>... annotatedClasses ) {
		addInstallerContextConfigurer( new AnnotatedClassConfigurer( annotatedClasses ) );
	}

	public void addInstallerContextConfigurer( ApplicationContextConfigurer... configurers ) {
		addInstallerContextConfigurers( Arrays.asList( configurers ) );
	}

	public void addInstallerContextConfigurers( Collection<ApplicationContextConfigurer> configurers ) {
		installerContextConfigurers.addAll( configurers );
	}

	public InstallerSettings getInstallerSettings() {
		return installerSettings;
	}

	public void setInstallerSettings( InstallerSettings installerSettings ) {
		this.installerSettings = installerSettings;
	}

	public Collection<Object> getInstallers() {
		return installers;
	}

	public void setInstallers( Collection<Object> installers ) {
		this.installers = installers;
	}

	public boolean isEmpty() {
		return installers.isEmpty() && !hasComponents && configurationExtensions.stream().allMatch( ModuleConfigurationExtension::isOptional );
	}

	public Collection<ExposedModuleBeanRegistry> getPreviouslyExposedBeans() {
		return previouslyExposedBeans;
	}

	public void setPreviouslyExposedBeans( Collection<ExposedModuleBeanRegistry> previouslyExposedBeans ) {
		this.previouslyExposedBeans = previouslyExposedBeans;
	}

	public void addPreviouslyExposedBeans( ExposedModuleBeanRegistry moduleBeanRegistry ) {
		this.previouslyExposedBeans.add( moduleBeanRegistry );
	}

	public Set<String> getExcludedAnnotatedClasses() {
		return excludedAnnotatedClasses;
	}

	/**
	 * Explicitly exclude an annotated class from being added. If it has been configured previously, it will still be ignored.
	 * <p>
	 * <p>NOTE: this only applies to the application context, not the installer context.</p>
	 *
	 * @param classNames to exclude
	 */
	public void exclude( String... classNames ) {
		excludedAnnotatedClasses.addAll( Arrays.asList( classNames ) );
	}

	/**
	 * Explicitly exclude an annotated class from being added. If it has been configured previously, it will still be ignored.
	 * <p>
	 * <p>NOTE: this only applies to the application context, not the installer context.</p>
	 *
	 * @param annotatedClasses to exclude
	 */
	public void exclude( Class<?>... annotatedClasses ) {
		Stream.of( annotatedClasses )
		      .map( Class::getName )
		      .forEach( excludedAnnotatedClasses::add );
	}
}
