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

package com.foreach.across.core.context;

import com.foreach.across.core.context.beans.ProvidedBeansMap;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.type.filter.TypeFilter;

/**
 * Interface the ApplicationContext must implement for Across to be able to use it.
 */
public interface AcrossConfigurableApplicationContext extends ConfigurableApplicationContext
{
	ConfigurableEnvironment getEnvironment();

	void provide( ProvidedBeansMap... beans );

	ConfigurableListableBeanFactory getBeanFactory();

	void addBeanFactoryPostProcessor( BeanFactoryPostProcessor postProcessor );

	void register( Class<?>... annotatedClasses );

	void scan( String... basePackages );

	void scan( String[] basePackages, TypeFilter[] excludedTypeFilters );

	void refresh();

	void start();

	void setDisplayName( String displayName );

	void destroy();
}
