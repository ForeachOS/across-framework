package com.foreach.across.core.context;

import com.foreach.across.core.context.registry.AcrossContextBeanRegistry;
import com.foreach.across.core.transformers.ExposedBeanDefinitionTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractExposedBeanRegistry
{
	private final Logger LOG = LoggerFactory.getLogger( getClass() );

	protected final String moduleName;
	protected final AcrossContextBeanRegistry contextBeanRegistry;
	protected final ExposedBeanDefinitionTransformer transformer;

	protected final Map<String, ExposedBeanDefinition> exposedDefinitions = new HashMap<>();

	protected AbstractExposedBeanRegistry( AcrossContextBeanRegistry contextBeanRegistry,
	                                       String moduleName,
	                                       ExposedBeanDefinitionTransformer transformer ) {
		this.moduleName = moduleName;
		this.contextBeanRegistry = contextBeanRegistry;
		this.transformer = transformer;
	}

	public Map<String, ExposedBeanDefinition> getExposedDefinitions() {
		return Collections.unmodifiableMap( exposedDefinitions );
	}

	protected void addBeans( Map<String, BeanDefinition> definitions, Map<String, Object> beans ) {
		Map<String, ExposedBeanDefinition> candidates = new HashMap<>();

		for ( Map.Entry<String, BeanDefinition> definition : definitions.entrySet() ) {
			BeanDefinition original = definition.getValue();
			ExposedBeanDefinition exposed = new ExposedBeanDefinition(
					contextBeanRegistry,
					moduleName,
					definition.getKey(),
					original,
					determineBeanClass( original, beans.get( definition.getKey() ) )
			);

			candidates.put( definition.getKey(), exposed );
		}

		for ( Map.Entry<String, Object> singleton : beans.entrySet() ) {
			if ( !candidates.containsKey( singleton.getKey() )
					&& singleton.getValue() != null ) {
				ExposedBeanDefinition exposed = new ExposedBeanDefinition(
						contextBeanRegistry,
						moduleName,
						singleton.getKey(),
						determineBeanClass( null, singleton.getValue() )
				);

				candidates.put( singleton.getKey(), exposed );
			}
		}

		if ( transformer != null ) {
			transformer.transformBeanDefinitions( candidates );
		}

		exposedDefinitions.putAll( candidates );
	}

	private Class<?> determineBeanClass( BeanDefinition beanDefinition, Object singleton ) {
		if ( beanDefinition instanceof AbstractBeanDefinition ) {
			AbstractBeanDefinition originalAbstract = (AbstractBeanDefinition) beanDefinition;

			if ( !originalAbstract.hasBeanClass() ) {
				try {
					originalAbstract.resolveBeanClass( Thread.currentThread().getContextClassLoader() );
				}
				catch ( Exception e ) {
					throw new RuntimeException( e );
				}
			}

			if ( originalAbstract.hasBeanClass() ) {
				return originalAbstract.getBeanClass();
			}
		}

		if ( singleton != null ) {
			return singleton.getClass();
		}

		return null;
	}

	/**
	 * Copies the BeanDefinitions to the BeanFactory provided (if possible).
	 */
	public void copyTo( ConfigurableListableBeanFactory beanFactory ) {
		if ( beanFactory instanceof BeanDefinitionRegistry ) {
			BeanDefinitionRegistry registry = (BeanDefinitionRegistry) beanFactory;

			copyBeanDefinitions( beanFactory, registry );
		}
		else {
			LOG.warn(
					"Unable to copy exposed bean definitions to bean factory {}, " +
							"it is not a BeanDefinitionRegistry",
					beanFactory );
		}
	}

	protected void copyBeanDefinitions( ConfigurableListableBeanFactory beanFactory, BeanDefinitionRegistry registry ) {
		for ( Map.Entry<String, ExposedBeanDefinition> definition : exposedDefinitions.entrySet() ) {
			LOG.debug( "Exposing bean {}: {}", definition.getKey(), definition.getValue().getBeanClassName() );

			ExposedBeanDefinition beanDefinition = definition.getValue();

			String beanName = beanDefinition.getPreferredBeanName();

			if ( beanFactory.containsBean( beanName ) ) {
				LOG.trace(
						"BeanDefinitionRegistry already contains a bean with name {}, using fully qualified name for exposing",
						beanName );
				beanName = beanDefinition.getFullyQualifiedBeanName();
			}

			registry.registerBeanDefinition( beanName, beanDefinition );

			for ( String alias : beanDefinition.getAliases() ) {
				registry.registerAlias( definition.getKey(), alias );
			}
		}
	}
}