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

import com.foreach.across.config.AcrossConfiguration;
import com.foreach.across.core.*;
import com.foreach.across.core.annotations.Module;
import com.foreach.across.core.config.ModuleConfigurationImportSelector;
import com.foreach.across.core.context.*;
import com.foreach.across.core.context.beans.PrimarySingletonBean;
import com.foreach.across.core.context.beans.ProvidedBeansMap;
import com.foreach.across.core.context.beans.SingletonBean;
import com.foreach.across.core.context.configurer.ApplicationContextConfigurer;
import com.foreach.across.core.context.configurer.ApplicationContextConfigurerAdapter;
import com.foreach.across.core.context.configurer.ConfigurerScope;
import com.foreach.across.core.context.configurer.ProvidedBeansConfigurer;
import com.foreach.across.core.context.info.*;
import com.foreach.across.core.context.installers.ClassPathScanningInstallerProvider;
import com.foreach.across.core.context.installers.InstallerSetBuilder;
import com.foreach.across.core.context.module.ModuleConfigurationExtension;
import com.foreach.across.core.context.registry.AcrossContextBeanRegistry;
import com.foreach.across.core.context.registry.DefaultAcrossContextBeanRegistry;
import com.foreach.across.core.events.AcrossContextBootstrappedEvent;
import com.foreach.across.core.events.AcrossModuleBeforeBootstrapEvent;
import com.foreach.across.core.events.AcrossModuleBootstrappedEvent;
import com.foreach.across.core.filters.BeanFilter;
import com.foreach.across.core.filters.BeanFilterComposite;
import com.foreach.across.core.filters.NamedBeanFilter;
import com.foreach.across.core.installers.AcrossBootstrapInstallerRegistry;
import com.foreach.across.core.installers.InstallerPhase;
import com.foreach.across.core.transformers.ExposedBeanDefinitionTransformer;
import com.foreach.across.core.util.ClassLoadingUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.CachedIntrospectionResults;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionEvaluationReport;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.PropertySources;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.TypeFilter;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.WebApplicationContext;

import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Stream;

import static com.foreach.across.core.context.bootstrap.AcrossBootstrapConfigurer.CONTEXT_INFRASTRUCTURE_MODULE;
import static com.foreach.across.core.context.bootstrap.AcrossBootstrapConfigurer.CONTEXT_POSTPROCESSOR_MODULE;

/**
 * Takes care of bootstrapping an entire across context.
 * To be replaced by {@link AcrossLifecycleBootstrapHandler} in the future.
 *
 * @see AcrossLifecycleBootstrapHandler
 */
public class AcrossBootstrapper
{
	static final String EXPOSE_SUPPORTING_APPLICATION_CONTEXT = "ax:expose-parent";

	private static final String AUTO_CONFIGURATION_REPORT_BEAN_NAME = "autoConfigurationReport";

	private static final Logger LOG = LoggerFactory.getLogger( AcrossBootstrapper.class );

	private final AcrossContext context;
	private final Deque<ConfigurableApplicationContext> createdApplicationContexts = new ArrayDeque<>();
	private BootstrapApplicationContextFactory applicationContextFactory;
	private List<AcrossBootstrapConfigurer> bootstrapConfigurers;

	public AcrossBootstrapper( AcrossContext context ) {
		this.context = context;

		applicationContextFactory = new AnnotationConfigBootstrapApplicationContextFactory();
	}

	public BootstrapApplicationContextFactory getApplicationContextFactory() {
		return applicationContextFactory;
	}

	public void setApplicationContextFactory( BootstrapApplicationContextFactory applicationContextFactory ) {
		this.applicationContextFactory = applicationContextFactory;
	}

	/**
	 * Bootstraps all modules in the context.
	 */
	public void bootstrap() {
		String moduleBeingProcessed = null;

		AcrossBootstrapTimer bootstrapTimer = new AcrossBootstrapTimer();
		bootstrapTimer.start();

		try {
			bootstrapTimer.startConfigurationPhase();

			checkBootstrapIsPossible();

			ConfigurableAcrossContextInfo contextInfo = buildContextAndModuleInfo();
			Collection<AcrossModuleInfo> modulesInOrder = contextInfo.getModules();

			LOG.info( "---" );
			LOG.info( "AcrossContext: {} ({})", context.getDisplayName(), context.getId() );
			LOG.info( "Bootstrapping {} modules in the following order:", modulesInOrder.size() );
			for ( AcrossModuleInfo moduleInfo : modulesInOrder ) {
				LOG.info( "{} - {} {} [resources: {}]", moduleInfo.getIndex(), moduleInfo.getName(),
				          moduleInfo.getVersionInfo().getVersion(), moduleInfo.getResourcesKey() );
			}
			LOG.info( "---" );

			runModuleBootstrapperCustomizations( modulesInOrder, context.getParentApplicationContext() );

			LazyCompositeAutowireCandidateResolver.clearAdditionalResolvers();
			AcrossApplicationContextHolder root = createRootContext( contextInfo );
			AcrossConfigurableApplicationContext rootContext = root.getApplicationContext();

			createdApplicationContexts.push( rootContext );

			AcrossBootstrapConfig contextBootstrapConfig = createBootstrapConfiguration( contextInfo );
			prepareForBootstrap( contextInfo );

			BootstrapLockManager bootstrapLockManager = new BootstrapLockManager( contextInfo );

			ModuleConfigurationSet moduleConfigurationSet = contextBootstrapConfig.getModuleConfigurationSet();

			try {
				AcrossBootstrapInstallerRegistry installerRegistry =
						new AcrossBootstrapInstallerRegistry(
								contextInfo.getBootstrapConfiguration(),
								bootstrapLockManager,
								applicationContextFactory
						);

				bootstrapTimer.finishConfigurationPhase();

				// Run installers that don't need anything bootstrapped
				installerRegistry.runInstallers( InstallerPhase.BeforeContextBootstrap );

				boolean pushExposedToParentContext = shouldPushExposedBeansToParent( contextInfo );
				ExposedContextBeanRegistry contextExposedBeans = new ExposedContextBeanRegistry(
						AcrossContextUtils.getBeanRegistry( contextInfo ),
						rootContext.getBeanFactory(),
						contextInfo.getBootstrapConfiguration().getExposeTransformer()
				);

				contextExposedBeans.add( AcrossContextInfo.BEAN );

				LOG.info( "" );
				LOG.info( "--- Starting module bootstrap" );

				List<ConfigurableAcrossModuleInfo> bootstrappedModules = new ArrayList<>();

				for ( AcrossModuleInfo moduleInfo : contextInfo.getModules() ) {
					bootstrapTimer.startModuleBootstrap( moduleInfo );

					moduleBeingProcessed = moduleInfo.getName();

					ConfigurableAcrossModuleInfo configurableAcrossModuleInfo = (ConfigurableAcrossModuleInfo) moduleInfo;
					ModuleBootstrapConfig config = moduleInfo.getBootstrapConfiguration();
					bootstrappedModules.forEach( previous -> config.addPreviouslyExposedBeans( previous.getExposedBeanRegistry() ) );

					// Add scanned (or edited) module configurations - first registered on the context, then on module itself
					config.extendModule( moduleConfigurationSet.getConfigurations( moduleInfo.getName(), moduleInfo.getAliases() ) );
					bootstrapConfigurers.forEach( configurer -> configurer.configureModule( config ) );

					LOG.info( "" );
					LOG.info( "{} - {} {} [resources: {}]", String.format( "%2s", moduleInfo.getIndex() ), moduleInfo.getName(),
					          moduleInfo.getVersionInfo().getVersion(), moduleInfo.getResourcesKey() );
					LOG.info( "     {}", moduleInfo.getModule().getClass() );
					LOG.info( "" );

					configurableAcrossModuleInfo.setBootstrapStatus( ModuleBootstrapStatus.BootstrapBusy );

					rootContext.publishEvent( new AcrossModuleBeforeBootstrapEvent( contextInfo, moduleInfo ) );

					if ( config.isEmpty() ) {
						LOG.info( "     Nothing to be done - skipping module bootstrap" );
						configurableAcrossModuleInfo.setBootstrapStatus( ModuleBootstrapStatus.Skipped );
						bootstrapTimer.finishModuleBootstrap( moduleInfo );
						continue;
					}

					filterApplicationContextConfigurers( moduleInfo, config, moduleConfigurationSet );

					// Run installers before bootstrapping this particular module
					installerRegistry.runInstallersForModule( moduleInfo.getName(), InstallerPhase.BeforeModuleBootstrap );

					// Create the module context
					AcrossConfigurableApplicationContext child = applicationContextFactory.createApplicationContext( context, config, root );

					AcrossApplicationContextHolder moduleApplicationContext = new AcrossApplicationContextHolder( child, root );
					AcrossContextUtils.setAcrossApplicationContextHolder( config.getModule(), moduleApplicationContext );

					applicationContextFactory.loadApplicationContext( context, config, moduleApplicationContext );

					// Bootstrap the module
					configurableAcrossModuleInfo.setBootstrapStatus( ModuleBootstrapStatus.Bootstrapped );

					// Send event that this module has bootstrapped
					rootContext.publishEvent( new AcrossModuleBootstrappedEvent( moduleInfo ) );

					// Run installers after module itself has bootstrapped
					installerRegistry.runInstallersForModule( moduleInfo.getName(), InstallerPhase.AfterModuleBootstrap );

					// Copy the beans to the parent context
					exposeBeans( configurableAcrossModuleInfo, config.getExposeFilter(), config.getExposeTransformer(),
					             rootContext );

					if ( pushExposedToParentContext ) {
						contextExposedBeans.addAll( configurableAcrossModuleInfo.getExposedBeanDefinitions() );
					}

					// Push the currently exposed beans to the previously bootstrapped modules
					ExposedModuleBeanRegistry moduleExposedBeans = configurableAcrossModuleInfo.getExposedBeanRegistry();
					bootstrappedModules.stream()
					                   .map( ConfigurableAcrossModuleInfo::getBeanFactory )
					                   .forEach( bf -> moduleExposedBeans.copyTo( bf, false ) );

					bootstrappedModules.add( configurableAcrossModuleInfo );

					bootstrapTimer.finishModuleBootstrap( moduleInfo );
				}

				moduleBeingProcessed = null;

				LOG.info( "" );
				LOG.info( "--- Module bootstrap finished: {} modules started", contextInfo.getModules().size() );
				LOG.info( "" );

				if ( pushExposedToParentContext ) {
					pushExposedBeansToParent( contextExposedBeans, rootContext );
				}

				// Refresh beans
				bootstrapTimer.startRefreshBeansPhase();
				AcrossContextUtils.refreshBeans( context );
				bootstrapTimer.finishRefreshBeansPhase();

				contextInfo.setBootstrapped( true );

				// Bootstrapping done, run installers that require context bootstrap finished
				installerRegistry.runInstallers( InstallerPhase.AfterContextBootstrap );

				bootstrapTimer.addInstallerTimeReports( installerRegistry.getInstallerTimeReports() );

				// Destroy the installer contexts
				installerRegistry.destroy();
			}
			finally {
				// Safe guard - ensure bootstrap released
				bootstrapLockManager.ensureUnlocked();

				SharedMetadataReaderFactory.clearCachedMetadata( rootContext );
			}

			// Bootstrap finished - publish the event
			bootstrapTimer.startContextBootstrappedEventHandling();
			rootContext.publishEvent( new AcrossContextBootstrappedEvent( contextInfo ) );
			bootstrapTimer.finishContextBootstrappedEventHandling();

			createdApplicationContexts.clear();

			resetCommonCaches();
		}
		catch ( RuntimeException e ) {
			LOG.debug( "Exception during bootstrapping, destroying all created ApplicationContext instances" );

			destroyAllCreatedApplicationContexts();

			AcrossException ae = e instanceof AcrossException ? (AcrossException) e : new AcrossBootstrapException( e );

			if ( ae.getModuleBeingProcessed() == null ) {
				ae.setModuleBeingProcessed( moduleBeingProcessed );
			}

			throw ae;
		}

		bootstrapTimer.finish();
		bootstrapTimer.printReport();
	}

	private void resetCommonCaches() {
		ReflectionUtils.clearCache();
		AnnotationUtils.clearCache();
		ResolvableType.clearCache();
		CachedIntrospectionResults.clearClassLoader( AcrossContextUtils.getApplicationContext( context ).getClassLoader() );
	}

	/**
	 * Create a new set of {@link com.foreach.across.core.context.configurer.ApplicationContextConfigurer} that no longer
	 * contains any explicitly excluded annotated classes.
	 */
	private void filterApplicationContextConfigurers( AcrossModuleInfo moduleInfo,
	                                                  ModuleBootstrapConfig config,
	                                                  ModuleConfigurationSet moduleConfigurationSet ) {
		Set<String> notAllowedAnnotatedClasses = new LinkedHashSet<>(
				Arrays.asList( moduleConfigurationSet.getExcludedConfigurations( moduleInfo.getName(), moduleInfo.getAliases() ) )
		);
		notAllowedAnnotatedClasses.addAll( config.getExcludedAnnotatedClasses() );

		Set<ApplicationContextConfigurer> filtered = new LinkedHashSet<>();

		config.getApplicationContextConfigurers()
		      .forEach( configurer -> {
			      if ( ArrayUtils.isEmpty( configurer.annotatedClasses() ) ) {
				      filtered.add( configurer );
			      }
			      else {
				      List<Class> filteredClasses = new ArrayList<>();
				      Stream.of( configurer.annotatedClasses() )
				            .filter( c -> !notAllowedAnnotatedClasses.contains( c.getName() ) )
				            .forEach( filteredClasses::add );

				      filtered.add(
						      new ApplicationContextConfigurerAdapter()
						      {
							      @Override
							      public ProvidedBeansMap providedBeans() {
								      return configurer.providedBeans();
							      }

							      @Override
							      public Class[] annotatedClasses() {
								      return filteredClasses.toArray( new Class[0] );
							      }

							      @Override
							      public String[] componentScanPackages() {
								      return configurer.componentScanPackages();
							      }

							      @Override
							      public BeanFactoryPostProcessor[] postProcessors() {
								      return configurer.postProcessors();
							      }

							      @Override
							      public PropertySources propertySources() {
								      return configurer.propertySources();
							      }

							      @Override
							      public TypeFilter[] excludedTypeFilters() {
								      return configurer.excludedTypeFilters();
							      }
						      }
				      );
			      }
		      } );

		// filter classes to import
		Set<ModuleConfigurationExtension> configurationExtensions = new LinkedHashSet<>();
		config.getConfigurationExtensions()
		      .stream()
		      .filter( e -> !notAllowedAnnotatedClasses.contains( e.getAnnotatedClass() ) )
		      .forEach( configurationExtensions::add );

		config.setConfigurationExtensions( configurationExtensions );
		config.setApplicationContextConfigurers( filtered );
	}

	private void destroyAllCreatedApplicationContexts() {
		while ( !createdApplicationContexts.isEmpty() ) {
			try {
				createdApplicationContexts.pop().close();
			}
			catch ( Exception e ) {
				/*Ignore exception*/
				LOG.trace( "Exception destroying ApplicationContext", e );
			}
		}
	}

	private boolean shouldPushExposedBeansToParent( AcrossContextInfo contextInfo ) {
		ApplicationContext applicationContext = contextInfo.getApplicationContext();

		if ( applicationContext.getParent() != null && !( applicationContext.getParent() instanceof ConfigurableApplicationContext ) ) {
			LOG.warn(
					"Unable to push the exposed beans to the parent ApplicationContext - requires a ConfigurableApplicationContext" );
		}

		return applicationContext.getParent() != null;
	}

	private void pushExposedBeansToParent( ExposedContextBeanRegistry exposedContextBeanRegistry, ApplicationContext rootContext ) {
		if ( !exposedContextBeanRegistry.isEmpty() ) {
			ApplicationContext parentContext = rootContext.getParent();
			ConfigurableApplicationContext currentApplicationContext = (ConfigurableApplicationContext) parentContext;
			ConfigurableListableBeanFactory currentBeanFactory = currentApplicationContext.getBeanFactory();

			ConfigurableListableBeanFactory beanFactory = currentBeanFactory;

			// If the direct parent does not handle exposed beans, check if another context already introduced
			// a supporting context higher up
			if ( !( beanFactory instanceof AcrossListableBeanFactory ) && currentApplicationContext.getParent() != null ) {
				ApplicationContext parent = currentApplicationContext.getParent();

				if ( parent instanceof ConfigurableApplicationContext ) {
					beanFactory = ( (ConfigurableApplicationContext) parent ).getBeanFactory();
				}
			}

			// Make sure the parent can handle exposed beans - if not, introduce a supporting BeanFactory in the hierarchy
			if ( !( beanFactory instanceof AcrossListableBeanFactory ) ) {
				AcrossConfigurableApplicationContext parentApplicationContext = applicationContextFactory.createApplicationContext();
				parentApplicationContext.setId( EXPOSE_SUPPORTING_APPLICATION_CONTEXT );

				if ( parentApplicationContext instanceof WebApplicationContext && rootContext instanceof WebApplicationContext ) {
					( (ConfigurableWebApplicationContext) parentApplicationContext )
							.setServletContext( ( (WebApplicationContext) rootContext ).getServletContext() );
				}

				ProvidedBeansMap providedBeansMap = new ProvidedBeansMap();
				providedBeansMap.put(
						SharedMetadataReaderFactory.BEAN_NAME,
						rootContext.getBean( SharedMetadataReaderFactory.BEAN_NAME )
				);
				parentApplicationContext.provide( providedBeansMap );
				parentApplicationContext.refresh();
				parentApplicationContext.start();

				ConfigurableListableBeanFactory parentBeanFactory = parentApplicationContext.getBeanFactory();

				BeanFactory parentBf = currentApplicationContext.getParentBeanFactory();
				if ( parentBf == null ) {
					currentApplicationContext.setParent( parentApplicationContext );
					currentBeanFactory.setParentBeanFactory( parentBeanFactory );
				}
				else {
					ConfigurableApplicationContext parent = (ConfigurableApplicationContext) currentApplicationContext.getParent();
					parent.setParent( parentApplicationContext );
					parent.getBeanFactory().setParentBeanFactory( parentBeanFactory );
				}
				beanFactory = parentApplicationContext.getBeanFactory();
			}

			exposedContextBeanRegistry.copyTo( beanFactory );
		}
	}

	private void exposeBeans( ConfigurableAcrossModuleInfo acrossModuleInfo,
	                          BeanFilter exposeFilter,
	                          ExposedBeanDefinitionTransformer exposeTransformer,
	                          AcrossConfigurableApplicationContext parentContext ) {
		BeanFilter exposeFilterToApply = exposeFilter;

		AcrossListableBeanFactory moduleBeanFactory = AcrossContextUtils.getBeanFactory(
				acrossModuleInfo );

		String[] exposedBeanNames = moduleBeanFactory.getExposedBeanNames();

		if ( exposedBeanNames.length > 0 ) {
			exposeFilterToApply = new BeanFilterComposite(
					exposeFilter,
					new NamedBeanFilter( exposedBeanNames )
			);
		}

		ExposedModuleBeanRegistry exposedBeanRegistry = new ExposedModuleBeanRegistry(
				AcrossContextUtils.getBeanRegistry( acrossModuleInfo.getContextInfo() ),
				acrossModuleInfo,
				(AbstractApplicationContext) acrossModuleInfo.getApplicationContext(),
				exposeFilterToApply,
				exposeTransformer
		);

		exposedBeanRegistry.copyTo( parentContext.getBeanFactory() );

		acrossModuleInfo.setExposedBeanRegistry( exposedBeanRegistry );
	}

	private ConfigurableAcrossContextInfo buildContextAndModuleInfo() {
		ConfigurableAcrossContextInfo contextInfo = new ConfigurableAcrossContextInfo( context );
		ModuleBootstrapOrderBuilder moduleBootstrapOrderBuilder = new ModuleBootstrapOrderBuilder();
		moduleBootstrapOrderBuilder.setDependencyResolver( context.getModuleDependencyResolver() );
		moduleBootstrapOrderBuilder.setSourceModules( context.getModules() );

		Collection<AcrossModuleInfo> configured = new LinkedList<>();

		int row = 1;

		AcrossContextConfigurationModule infrastructureModule = new AcrossContextConfigurationModule( CONTEXT_INFRASTRUCTURE_MODULE );
		infrastructureModule.setOrder( Ordered.HIGHEST_PRECEDENCE + 1000 );
		ConfigurableAcrossModuleInfo moduleInfo = new ConfigurableAcrossModuleInfo( contextInfo, infrastructureModule, row++ );
		moduleInfo.setModuleRole( AcrossModuleRole.INFRASTRUCTURE );
		moduleInfo.setOrderInModuleRole( infrastructureModule.getOrder() );

		configured.add( moduleInfo );

		for ( AcrossModule module : moduleBootstrapOrderBuilder.getOrderedModules() ) {
			configured.add( new ConfigurableAcrossModuleInfo( contextInfo, module, row++ ) );
		}

		AcrossContextConfigurationModule postProcessorModule = new AcrossContextConfigurationModule( CONTEXT_POSTPROCESSOR_MODULE );
		postProcessorModule.setOrder( Ordered.LOWEST_PRECEDENCE - 1000 );
		moduleInfo = new ConfigurableAcrossModuleInfo( contextInfo, postProcessorModule, row );
		moduleInfo.setModuleRole( AcrossModuleRole.POSTPROCESSOR );
		moduleInfo.setOrderInModuleRole( postProcessorModule.getOrder() );
		configured.add( moduleInfo );

		contextInfo.setConfiguredModules( configured );

		for ( AcrossModule module : moduleBootstrapOrderBuilder.getOrderedModules() ) {
			moduleInfo = contextInfo.getConfigurableModuleInfo( module.getName() );

			moduleInfo.setRequiredDependencies( convertToModuleInfo( moduleBootstrapOrderBuilder.getConfiguredRequiredDependencies( module ), contextInfo ) );
			moduleInfo.setOptionalDependencies( convertToModuleInfo( moduleBootstrapOrderBuilder.getConfiguredOptionalDependencies( module ), contextInfo ) );
			moduleInfo.setModuleRole( moduleBootstrapOrderBuilder.getModuleRole( module ) );
			moduleInfo.setOrderInModuleRole( moduleBootstrapOrderBuilder.getOrderInRole( module ) );
		}

		return contextInfo;
	}

	private Collection<AcrossModuleInfo> convertToModuleInfo( Collection<AcrossModule> list,
	                                                          ConfigurableAcrossContextInfo contextInfo ) {
		Collection<AcrossModuleInfo> infoList = new ArrayList<>( list.size() );

		for ( AcrossModule module : list ) {
			infoList.add( contextInfo.getModuleInfo( module.getName() ) );
		}

		return infoList;
	}

	private void prepareForBootstrap( AcrossContextInfo contextInfo ) {

		for ( ModuleBootstrapConfig moduleConfig : contextInfo.getBootstrapConfiguration().getModules() ) {
			moduleConfig.getModule().prepareForBootstrap( moduleConfig, contextInfo.getBootstrapConfiguration() );
		}
	}

	/**
	 * Builds the bootstrap configuration entities.
	 */
	private AcrossBootstrapConfig createBootstrapConfiguration( ConfigurableAcrossContextInfo contextInfo ) {
		Map<String, ModuleBootstrapConfig> configs = new LinkedHashMap<>();

		ApplicationContext applicationContext = contextInfo.getApplicationContext();
		MetadataReaderFactory metadataReaderFactory
				= applicationContext.getBean( SharedMetadataReaderFactory.BEAN_NAME, MetadataReaderFactory.class );

		ClassPathScanningInstallerProvider installerProvider = new ClassPathScanningInstallerProvider( applicationContext, metadataReaderFactory );

		BeanFilter defaultExposeFilter = buildDefaultExposeFilter( applicationContext.getClassLoader() );

		for ( AcrossModuleInfo moduleInfo : contextInfo.getModules() ) {
			AcrossModule module = moduleInfo.getModule();
			ModuleBootstrapConfig config = new ModuleBootstrapConfig( moduleInfo );
			config.setExposeFilter( new BeanFilterComposite( defaultExposeFilter, module.getExposeFilter() ) );
			config.setExposeTransformer( module.getExposeTransformer() );
			config.setInstallerSettings( module.getInstallerSettings() );
			config.getInstallers().addAll( buildInstallerSet( module, installerProvider ) );

			// Provide the current module beans
			ProvidedBeansMap providedSingletons = new ProvidedBeansMap();
			providedSingletons.put( AcrossModule.CURRENT_MODULE + "Info",
			                        new PrimarySingletonBean(
					                        moduleInfo,
					                        new AutowireCandidateQualifier( Module.class.getName(),
					                                                        AcrossModule.CURRENT_MODULE )
			                        )
			);
			providedSingletons.put( AcrossModule.CURRENT_MODULE,
			                        new PrimarySingletonBean(
					                        module,
					                        new AutowireCandidateQualifier( Module.class.getName(),
					                                                        AcrossModule.CURRENT_MODULE )
			                        )
			);

			// context and modules should use the main configuration report bean name
			if ( contextInfo.getApplicationContext().containsBean( AUTO_CONFIGURATION_REPORT_BEAN_NAME ) ) {
				providedSingletons.put( AUTO_CONFIGURATION_REPORT_BEAN_NAME,
				                        contextInfo.getApplicationContext()
				                                   .getBean( AUTO_CONFIGURATION_REPORT_BEAN_NAME ) );
			}

			registerSettings( module, providedSingletons, false );

			// Provided singletons do not influence initial load
			config.addApplicationContextConfigurer( true, new ProvidedBeansConfigurer( providedSingletons ) );

			// add the module configuration importer
			config.addApplicationContextConfigurer( true, ModuleConfigurationImportSelector.class );

			if ( !isContextModule( config ) ) {
				// Only add default configurations if not a core module
				config.addApplicationContextConfigurers( AcrossContextUtils.getApplicationContextConfigurers( context, module ) );
			}

			// create installer application context
			config.addInstallerContextConfigurer( new ProvidedBeansConfigurer( providedSingletons ) );
			config.addInstallerContextConfigurers( contextInfo.getContext().getInstallerContextConfigurers() );
			config.addInstallerContextConfigurers( AcrossContextUtils.getInstallerContextConfigurers( module ) );

			configs.put( config.getModuleName(), config );

			( (ConfigurableAcrossModuleInfo) moduleInfo ).setBootstrapConfiguration( config );
		}

		AcrossBootstrapConfig contextConfig = new AcrossBootstrapConfig(
				contextInfo.getContext(), configs, buildModuleConfigurationSet( contextInfo )
		);
		contextConfig.setExposeTransformer( contextInfo.getContext().getExposeTransformer() );

		bootstrapConfigurers = new ArrayList<>(
				BeanFactoryUtils.beansOfTypeIncludingAncestors(
						(ListableBeanFactory) applicationContext.getAutowireCapableBeanFactory(), AcrossBootstrapConfigurer.class
				).values()
		);
		bootstrapConfigurers.sort( AnnotationAwareOrderComparator.INSTANCE );
		bootstrapConfigurers.forEach( configurer -> configurer.configureContext( contextConfig ) );

		contextInfo.setBootstrapConfiguration( contextConfig );

		return contextConfig;
	}

	private boolean isContextModule( ModuleBootstrapConfig config ) {
		return config.getModule() instanceof AcrossContextConfigurationModule;
	}

	private BeanFilter buildDefaultExposeFilter( ClassLoader classLoader ) {
		final Collection<String> exposedItems = AcrossConfiguration.get( classLoader ).getExposeRules();

		Class<?>[] classesOrAnnotations = exposedItems
				.stream()
				.filter( s -> !s.endsWith( ".*" ) )
				.filter( className -> ClassUtils.isPresent( className, classLoader ) )
				.map( className -> {
					try {
						return ClassUtils.forName( className, classLoader );
					}
					catch ( Exception e ) {
						LOG.error( "Unable to load Exposed class or annotation: {}", className, e );
						return null;
					}
				} )
				.filter( Objects::nonNull )
				.toArray( Class<?>[]::new );

		String[] packageNames = exposedItems
				.stream()
				.filter( s -> s.endsWith( ".*" ) )
				.map( s -> StringUtils.removeEnd( s, ".*" ) )
				.toArray( String[]::new );

		return new BeanFilterComposite(
				BeanFilter.instances( classesOrAnnotations ),
				BeanFilter.annotations( classesOrAnnotations ),
				BeanFilter.packages( packageNames )
		);
	}

	private void registerSettings( AcrossModule module, ProvidedBeansMap beansMap, boolean compatibility ) {
		String settingsClassName = ClassUtils.getUserClass( module.getClass() ).getName() + "Settings";

		try {
			Class settingsClass = ClassLoadingUtils.loadClass( settingsClassName );

			if ( !settingsClass.isInterface() && !Modifier.isAbstract( settingsClass.getModifiers() ) ) {
				if ( !compatibility ) {
					// Register settings as bean in the module application context
					AbstractBeanDefinition beanDefinition = BeanDefinitionBuilder.genericBeanDefinition( settingsClass ).setPrimary( true ).getBeanDefinition();
					beanDefinition.addQualifier(
							new AutowireCandidateQualifier( Module.class.getName(), AcrossModule.CURRENT_MODULE )
					);

					beansMap.put( AcrossModule.CURRENT_MODULE + "Settings", beanDefinition );
				}
			}
		}
		catch ( ClassNotFoundException ignore ) {
		}
	}

	private Collection<Object> buildInstallerSet( AcrossModule module, ClassPathScanningInstallerProvider installerProvider ) {
		InstallerSetBuilder installerSetBuilder = new InstallerSetBuilder( installerProvider );
		installerSetBuilder.add( module.getInstallers() );
		installerSetBuilder.scan( module.getInstallerScanPackages() );

		return Arrays.asList( installerSetBuilder.build() );
	}

	private ModuleConfigurationSet buildModuleConfigurationSet( AcrossContextInfo contextInfo ) {
		ApplicationContext applicationContext = contextInfo.getApplicationContext();
		MetadataReaderFactory metadataReaderFactory
				= applicationContext.getBean( SharedMetadataReaderFactory.BEAN_NAME, MetadataReaderFactory.class );

		Set<String> basePackages = new LinkedHashSet<>();

		contextInfo.getModules()
		           .stream()
		           .filter( AcrossModuleInfo::isEnabled )
		           .forEach( acrossModuleInfo -> Collections.addAll(
				           basePackages, acrossModuleInfo.getModule().getModuleConfigurationScanPackages()
		                     )
		           );

		Collections.addAll( basePackages, contextInfo.getContext().getModuleConfigurationScanPackages() );

		return new ClassPathScanningModuleConfigurationProvider( applicationContext, metadataReaderFactory )
				.scan( basePackages.toArray( new String[0] ) );
	}

	private void checkBootstrapIsPossible() {
		checkUniqueModuleNames( context.getModules() );
	}

	private void checkUniqueModuleNames( Collection<AcrossModule> modules ) {
		Set<String> moduleNames = new HashSet<>();

		for ( AcrossModule module : modules ) {
			if ( moduleNames.contains( module.getName() ) ) {
				throw new AcrossConfigurationException(
						"Each module must have a unique name, duplicate found for " + module.getName() );
			}

			moduleNames.add( module.getName() );
		}
	}

	private void runModuleBootstrapperCustomizations( Collection<AcrossModuleInfo> modules, ApplicationContext applicationContext ) {
		if ( applicationContext != null ) {
			Map<String, BootstrapAdapter> adapterMap = BeanFactoryUtils.beansOfTypeIncludingAncestors(
					(ListableBeanFactory) applicationContext.getAutowireCapableBeanFactory(), BootstrapAdapter.class
			);
			adapterMap.forEach( ( beanName, adapter ) -> adapter.customizeBootstrapper( this ) );
		}
		for ( AcrossModuleInfo moduleInfo : modules ) {
			if ( moduleInfo.getModule() instanceof BootstrapAdapter ) {
				( (BootstrapAdapter) moduleInfo.getModule() ).customizeBootstrapper( this );
			}
		}
	}

	private AcrossApplicationContextHolder createRootContext( ConfigurableAcrossContextInfo contextInfo ) {
		AcrossConfigurableApplicationContext rootApplicationContext =
				applicationContextFactory.createApplicationContext( context,
				                                                    context.getParentApplicationContext() );

		ProvidedBeansMap providedBeans = new ProvidedBeansMap();

		// Register the single autoConfigurationReport
		if ( context.getParentApplicationContext() != null ) {
			providedBeans.put(
					AUTO_CONFIGURATION_REPORT_BEAN_NAME,
					ConditionEvaluationReport.get( (ConfigurableListableBeanFactory)
							                               context.getParentApplicationContext()
							                                      .getAutowireCapableBeanFactory() )
			);
		}

		// Create the AcrossContextBeanRegistry
		AcrossContextBeanRegistry contextBeanRegistry = new DefaultAcrossContextBeanRegistry( contextInfo );
		providedBeans.put( contextBeanRegistry.getFactoryName(),
		                   new PrimarySingletonBean(
				                   new DefaultAcrossContextBeanRegistry( contextInfo ),
				                   new AutowireCandidateQualifier( Qualifier.class.getName(),
				                                                   AcrossContextBeanRegistry.BEAN )
		                   ) );

		// Put the context and its info as fixed singletons
		providedBeans.put( AcrossContext.BEAN, new PrimarySingletonBean( context ) );
		providedBeans.put( AcrossContextInfo.BEAN, new PrimarySingletonBean( contextInfo ) );

		// Put the module info as singletons in the context
		for ( AcrossModuleInfo moduleInfo : contextInfo.getConfiguredModules() ) {
			// Create the module instances as primary beans so they do not clash with modules
			// configured as beans in a parent application context
			providedBeans.put( "across.module." + moduleInfo.getName(),
			                   new PrimarySingletonBean(
					                   moduleInfo.getModule(),
					                   new AutowireCandidateQualifier( Module.class.getName(),
					                                                   moduleInfo.getName() )
			                   )
			);
			providedBeans.put( moduleInfo.getName(),
			                   new SingletonBean(
					                   moduleInfo,
					                   new AutowireCandidateQualifier( Module.class.getName(),
					                                                   moduleInfo.getName() )
			                   )
			);

			registerSettings( moduleInfo.getModule(), providedBeans, true );
		}

		context.addApplicationContextConfigurer( new ProvidedBeansConfigurer( providedBeans ),
		                                         ConfigurerScope.CONTEXT_ONLY );

		AcrossApplicationContextHolder root = new AcrossApplicationContextHolder( rootApplicationContext );
		AcrossContextUtils.setAcrossApplicationContextHolder( context, root );

		applicationContextFactory.loadApplicationContext( context, root );

		return root;
	}
}
