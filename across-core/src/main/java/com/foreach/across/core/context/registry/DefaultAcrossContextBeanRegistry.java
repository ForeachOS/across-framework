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

package com.foreach.across.core.context.registry;

import com.foreach.across.core.context.AcrossListableBeanFactory;
import com.foreach.across.core.context.AcrossOrderSpecifierComparator;
import com.foreach.across.core.context.info.AcrossModuleInfo;
import com.foreach.across.core.context.info.ConfigurableAcrossContextInfo;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.core.ResolvableType;

import java.util.*;

public class DefaultAcrossContextBeanRegistry implements AcrossContextBeanRegistry
{
	@Getter
	private ConfigurableAcrossContextInfo contextInfo;

	public DefaultAcrossContextBeanRegistry( ConfigurableAcrossContextInfo contextInfo ) {
		setContextInfo( contextInfo );
	}

	protected DefaultAcrossContextBeanRegistry() {
	}

	protected void setContextInfo( ConfigurableAcrossContextInfo contextInfo ) {
		this.contextInfo = contextInfo;
	}

	@Override
	public String getContextId() {
		return contextInfo.getId();
	}

	@Override
	public String getFactoryName() {
		return contextInfo.getId() + "@" + AcrossContextBeanRegistry.BEAN;
	}

	@Override
	public boolean containsBean( String beanName ) {
		return contextInfo.getApplicationContext().containsBean( beanName );
	}

	@Override
	public boolean moduleContainsLocalBean( String moduleName, String beanName ) {
		return contextInfo.getConfigurableModuleInfo( moduleName ).getApplicationContext().containsLocalBean(
				beanName );
	}

	@Override
	public Object getBean( String beanName ) {
		return contextInfo.getApplicationContext().getBean( beanName );
	}

	@Override
	public <T> T getBean( String beanName, Class<T> requiredType ) {
		return contextInfo.getApplicationContext().getBean( beanName, requiredType );
	}

	@Override
	public Class<?> getBeanType( String beanName ) {
		return contextInfo.getApplicationContext().getType( beanName );
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getBeanFromModule( String moduleName, String beanName ) {
		if ( StringUtils.isEmpty( moduleName ) ) {
			return (T) getBean( beanName );
		}

		return (T) contextInfo.getConfigurableModuleInfo( moduleName ).getApplicationContext().getBean( beanName );
	}

	@Override
	public Class<?> getBeanTypeFromModule( String moduleName, String beanName ) {
		if ( StringUtils.isEmpty( moduleName ) ) {
			return getBeanType( beanName );
		}

		return contextInfo.getConfigurableModuleInfo( moduleName ).getApplicationContext().getType( beanName );
	}

	@Override
	public <T> T getBeanOfType( Class<T> requiredType ) {
		return contextInfo.getApplicationContext().getBean( requiredType );
	}

	@Override
	public <T> T getBeanOfTypeFromModule( String moduleName, Class<T> requiredType ) {
		if ( StringUtils.isEmpty( moduleName ) ) {
			return null;
		}

		return contextInfo.getConfigurableModuleInfo( moduleName ).getApplicationContext().getBean( requiredType );
	}

	@Override
	public <T> Optional<T> findBeanOfTypeFromModule( String moduleName, Class<T> requiredType ) {
		try {
			return Optional.ofNullable( getBeanOfTypeFromModule( moduleName, requiredType ) );
		}
		catch ( BeansException be ) {
			return Optional.empty();
		}
	}

	@Override
	public <T> List<T> getBeansOfType( Class<T> beanClass ) {
		return getBeansOfType( beanClass, false );
	}

	@Override
	public <T> Map<String, T> getBeansOfTypeAsMap( Class<T> beanClass ) {
		return getBeansOfTypeAsMap( beanClass, false );
	}

	@Override
	public <T> List<T> getBeansOfType( Class<T> beanClass, boolean includeModuleInternals ) {
		return getBeansOfType( ResolvableType.forClass( beanClass ), includeModuleInternals );
	}

	@Override
	public <T> Map<String, T> getBeansOfTypeAsMap( Class<T> beanClass, boolean includeModuleInternals ) {
		return getBeansOfTypeAsMap( ResolvableType.forClass( beanClass ), includeModuleInternals );
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> List<T> getBeansOfType( ResolvableType resolvableType, boolean includeModuleInternals ) {
		return new ArrayList<>(
				(Collection<T>) getBeansOfTypeAsMap( resolvableType, includeModuleInternals ).values()
		);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> Map<String, T> getBeansOfTypeAsMap( @NonNull ResolvableType resolvableType, boolean includeModuleInternals ) {
		List<T> beans = new ArrayList<>();
		AcrossOrderSpecifierComparator comparator = new AcrossOrderSpecifierComparator();
		AcrossListableBeanFactory beanFactory = beanFactory( contextInfo.getApplicationContext() );

		String[] beanNames = BeanFactoryUtils.beanNamesForTypeIncludingAncestors( beanFactory, resolvableType );
		Map<T, String> beanNameForBean = new IdentityHashMap<>(
				includeModuleInternals ? beanNames.length + contextInfo.getModules().size() : beanNames.length
		);

		for ( String beanName : beanNames ) {
			if ( includeModuleInternals && beanFactory.isExposedBean( beanName ) ) {
				// when including module internals, skip any exposed beans on the root context
				continue;
			}

			Object bean = beanFactory.getBean( beanName );
			comparator.register( bean, beanFactory.retrieveOrderSpecifier( beanName ) );

			if ( beanNameForBean.put( (T) bean, beanName ) == null ) {
				beans.add( (T) bean );
			}
		}

		if ( includeModuleInternals ) {
			for ( AcrossModuleInfo module : contextInfo.getModules() ) {
				if ( module.isBootstrapped() ) {
					beanFactory = beanFactory( module.getApplicationContext() );

					for ( String beanName : beanFactory.getBeanNamesForType( resolvableType ) ) {
						if ( !beanFactory.isExposedBean( beanName ) ) {
							Object bean = beanFactory.getBean( beanName );
							comparator.register( bean, beanFactory.retrieveOrderSpecifier( beanName ) );

							if ( beanNameForBean.put( (T) bean, module.getName() + ":" + beanName ) == null ) {
								beans.add( (T) bean );
							}
						}
					}
				}
			}
		}

		LinkedHashMap<String, T> beansMap = new LinkedHashMap<>( beans.size() );

		comparator.sort( beans );

		for ( T bean : beans ) {
			beansMap.put( beanNameForBean.get( bean ), bean );
		}

		return beansMap;
	}

	private AcrossListableBeanFactory beanFactory( ApplicationContext applicationContext ) {
		return (AcrossListableBeanFactory) applicationContext.getAutowireCapableBeanFactory();
	}
}
