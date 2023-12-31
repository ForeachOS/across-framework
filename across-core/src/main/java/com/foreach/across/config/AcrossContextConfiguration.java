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
package com.foreach.across.config;

import com.foreach.across.core.AcrossContext;
import com.foreach.across.core.AcrossModule;
import com.foreach.across.core.context.*;
import com.foreach.across.core.support.AcrossContextBuilder;
import com.foreach.across.core.util.ClassLoadingUtils;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.SpringApplicationEvent;
import org.springframework.boot.type.classreading.ConcurrentReferenceCachingMetadataReaderFactory;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.*;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.classreading.MetadataReaderFactory;

import javax.sql.DataSource;
import java.util.*;

/**
 * <p>Creates an AcrossContext bean and will apply all AcrossContextConfigurer instances
 * before bootstrapping.  Depending on the settings imported from {@link EnableAcrossContext},
 * modules will be auto-configured and further configuration of the context delegated to the configurer beans.</p>
 * <p>A single {@link DataSource} bean or one named <b>acrossDataSource</b> is required for installers to work.</p>
 */
@Configuration
@Import(AcrossContextWebConfiguration.class)
public class AcrossContextConfiguration implements ImportAware, EnvironmentAware, BeanFactoryAware, BeanClassLoaderAware
{
	private static final Logger LOG = LoggerFactory.getLogger( AcrossContextConfiguration.class );

	static final String ANNOTATION_TYPE = EnableAcrossContext.class.getName();

	@Autowired(required = false)
	private Collection<AcrossContextConfigurer> configurers = Collections.emptyList();

	@Autowired(required = false)
	private Collection<AcrossModule> moduleBeans = Collections.emptyList();

	private AnnotationMetadata importMetadata;
	private Environment environment;
	private BeanFactory beanFactory;
	private ClassLoader beanClassLoader;
	private AcrossContext acrossContext;

	@Override
	public void setImportMetadata( AnnotationMetadata importMetadata ) {
		this.importMetadata = importMetadata;
	}

	@Override
	public void setEnvironment( Environment environment ) {
		this.environment = environment;
	}

	@Override
	public void setBeanFactory( BeanFactory beanFactory ) {
		this.beanFactory = beanFactory;
	}

	@Override
	public void setBeanClassLoader( ClassLoader classLoader ) {
		this.beanClassLoader = classLoader;
	}

	@Bean
	public AcrossContext acrossContext( ConfigurableApplicationContext applicationContext,
	                                    @Qualifier(AcrossContext.DATASOURCE) Optional<DataSource> acrossDataSource,
	                                    @Qualifier(AcrossContext.INSTALLER_DATASOURCE) Optional<DataSource> installerDataSource
	                                    ) {
		Map<String, Object> configuration = importMetadata.getAnnotationAttributes( ANNOTATION_TYPE );

		AcrossContextBuilder contextBuilder = new AcrossContextBuilder()
				.applicationContext( applicationContext )
				.dataSource( selectAcrossDataSource( acrossDataSource ) )
				.installerDataSource( installerDataSource.orElse( null ) )
				.developmentMode( isDevelopmentMode() )
				.moduleConfigurationPackages( determineModuleConfigurationPackages( configuration ) )
				.configurer( configurers.toArray( new AcrossContextConfigurer[0] ) );

		autoConfigureContextBuilder( contextBuilder, importMetadata.getAnnotationAttributes( ANNOTATION_TYPE ) );

		AcrossContext context = contextBuilder.build();
		context.bootstrap();

		this.acrossContext = context;

		return context;
	}

	@Order(Ordered.HIGHEST_PRECEDENCE)
	@EventListener
	public void forwardApplicationEvent( ApplicationEvent event ) {
		if ( event instanceof SpringApplicationEvent || event instanceof WebServerInitializedEvent ) {
			// todo: extend across-configuration to allow specifying which events should be forwarded
			if ( acrossContext != null ) {
				val multicaster = AcrossContextUtils.getContextInfo( acrossContext )
				                                    .getApplicationContext()
				                                    .getBean( ApplicationEventMulticaster.class );

				multicaster.multicastEvent( event );
			}
		}
	}

	private DataSource selectAcrossDataSource( Optional<DataSource> acrossDataSource ) {
		if ( acrossDataSource.isPresent() ) {
			return acrossDataSource.get();
		}

		Collection<DataSource> dataSources = BeanFactoryUtils.beansOfTypeIncludingAncestors( (ListableBeanFactory) beanFactory, DataSource.class, false, false )
		                                                     .values();
		if ( !dataSources.isEmpty() ) {
			if ( dataSources.size() == 1 ) {
				LOG.info( "Single datasource bean found - registering it as the AcrossContext datasource." );
				return dataSources.iterator().next();
			}
			else {
				LOG.warn(
						"Unable to select AcrossContext datasource - multiple beans but none named 'acrossDataSource', " +
								"please put an explicit qualifier on the correct datasource instance." );
			}
		}
		else {
			LOG.trace( "No datasource bean found - not autoconfiguring an across datasource." );
		}
		return null;
	}

	private void autoConfigureContextBuilder( AcrossContextBuilder contextBuilder,
	                                          Map<String, Object> configuration ) {
		if ( configuration != null ) {
			if ( Boolean.TRUE.equals( configuration.get( "autoConfigure" ) ) ) {
				contextBuilder.moduleDependencyResolver( beanFactory.getBean( ModuleDependencyResolver.class ) )
				              .modules( namedModulesToConfigure( configuration ) )
				              .modules( moduleBeans.toArray( new AcrossModule[0] ) );
			}
		}
	}

	private boolean isDevelopmentMode() {
		if ( environment.containsProperty( "across.development.active" ) ) {
			return environment.getProperty( "across.development.active", Boolean.class, false );
		}
		else if ( environment.acceptsProfiles( Profiles.of( "dev" ) ) ) {
			LOG.info( "Activating development mode for Across because of 'dev' Spring profile" );
			return true;
		}
		return false;
	}

	private String[] namedModulesToConfigure( Map<String, Object> configuration ) {
		String[] valueModuleNames = (String[]) configuration.get( "value" );
		String[] moduleNames = (String[]) configuration.get( "modules" );

		Set<String> modules = new LinkedHashSet<>();
		modules.addAll( Arrays.asList( valueModuleNames ) );
		modules.addAll( Arrays.asList( moduleNames ) );

		return modules.toArray( new String[0] );
	}

	@Bean(SharedMetadataReaderFactory.BEAN_NAME)
	public ConcurrentReferenceCachingMetadataReaderFactory sharedMetadataReaderFactory() {
		return new ConcurrentReferenceCachingMetadataReaderFactory( beanClassLoader );
	}

	@Bean
	@Lazy
	public ModuleDependencyResolver moduleDependencyResolver( ApplicationContext applicationContext ) {
		final ClassPathScanningCandidateModuleProvider candidateModuleProvider = new ClassPathScanningCandidateModuleProvider(
				applicationContext, applicationContext.getBean( SharedMetadataReaderFactory.BEAN_NAME, MetadataReaderFactory.class )
		);

		ClassPathScanningModuleDependencyResolver moduleDependencyResolver = new ClassPathScanningModuleDependencyResolver( candidateModuleProvider );

		Map<String, Object> configuration = importMetadata.getAnnotationAttributes( ANNOTATION_TYPE );
		if ( configuration != null ) {
			moduleDependencyResolver.setResolveRequired(
					Boolean.TRUE.equals( configuration.get( "scanForRequiredModules" ) )
			);
			moduleDependencyResolver.setResolveOptional(
					Boolean.TRUE.equals( configuration.get( "scanForOptionalModules" ) )
			);
			moduleDependencyResolver.setExcludedModules(
					Arrays.asList( (String[]) configuration.get( "excludeFromScanning" ) )
			);
			moduleDependencyResolver.setBasePackages( determineModulePackages( configuration ) );
		}

		return moduleDependencyResolver;
	}

	private String[] determineModuleConfigurationPackages( Map<String, Object> configuration ) {
		Set<String> configurationPackageSet = new LinkedHashSet<>();

		String[] configurationPackages = (String[]) configuration.get( "moduleConfigurationPackages" );
		Collections.addAll( configurationPackageSet, configurationPackages );

		Class<?>[] configurationPackageClasses = (Class<?>[]) configuration.get( "moduleConfigurationPackageClasses" );

		for ( Class<?> modulePackageClass : configurationPackageClasses ) {
			configurationPackageSet.add( modulePackageClass.getPackage().getName() );
		}

		if ( configurationPackageSet.isEmpty() ) {
			try {
				Class<?> importingClass = ClassLoadingUtils.loadClass( importMetadata.getClassName() );
				Package importingClassPackage = importingClass.getPackage();

				String base = "";

				if ( importingClassPackage != null ) {
					base = importingClass.getPackage().getName();
				}

				configurationPackageSet.add( base + ".config" );
				configurationPackageSet.add( base + ".extensions" );
			}
			catch ( ClassNotFoundException ignore ) {
			}
		}

		return configurationPackageSet.toArray( new String[0] );
	}

	private String[] determineModulePackages( Map<String, Object> configuration ) {
		Set<String> modulePackageSet = new LinkedHashSet<>();

		String[] modulePackages = (String[]) configuration.get( "modulePackages" );
		Collections.addAll( modulePackageSet, modulePackages );

		Class<?>[] modulePackageClasses = (Class<?>[]) configuration.get( "modulePackageClasses" );

		for ( Class<?> modulePackageClass : modulePackageClasses ) {
			modulePackageSet.add( modulePackageClass.getPackage().getName() );
		}

		if ( !modulePackageSet.contains( "." ) ) {
			modulePackageSet.add( AcrossContextBuilder.STANDARD_MODULES_PACKAGE );

			try {
				Class<?> importingClass = ClassLoadingUtils.loadClass( importMetadata.getClassName() );
				Package importingClassPackage = importingClass.getPackage();

				if ( importingClassPackage != null ) {
					modulePackageSet.add( importingClass.getPackage().getName() );
				}
				else {
					modulePackageSet.add( "modules" );
				}
			}
			catch ( ClassNotFoundException ignore ) {
			}
		}
		else {
			LOG.info( "Not registering default packages for Across module scanning" );
			modulePackageSet.remove( "." );
		}

		return modulePackageSet.toArray( new String[0] );
	}
}
