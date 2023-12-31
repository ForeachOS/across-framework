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

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.HierarchicalBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Registers the application module package. Separate configuration class so it would not be scanned in a parent class.
 *
 * @author Arne Vandamme
 * @see ApplicationAutoConfigurationPackage
 * @since 3.0.0
 */
@Slf4j
@Configuration
@Import(ApplicationModuleAutoConfigurationPackageRegistrar.Registrar.class)
class ApplicationModuleAutoConfigurationPackageRegistrar
{
	@Order(Ordered.HIGHEST_PRECEDENCE)
	final static class Registrar implements ImportBeanDefinitionRegistrar
	{
		@Override
		public void registerBeanDefinitions( AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry ) {
			val autoConfigurationPackage = ( (HierarchicalBeanFactory) registry ).getParentBeanFactory().getBean( ApplicationAutoConfigurationPackage.class );
			LOG.info( "Registering AutoConfigurationPackage {}", autoConfigurationPackage.getApplicationModulePackage() );
			AutoConfigurationPackages.register( registry, autoConfigurationPackage.getApplicationModulePackage() );
		}
	}
}
