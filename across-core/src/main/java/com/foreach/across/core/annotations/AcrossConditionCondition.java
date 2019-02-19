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

package com.foreach.across.core.annotations;

import com.foreach.across.core.AcrossModule;
import com.foreach.across.core.AcrossModuleSettings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.Scope;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

@Deprecated
public class AcrossConditionCondition implements Condition
{
	private static final Logger LOG = LoggerFactory.getLogger( AcrossConditionCondition.class );

	@Override
	public boolean matches( ConditionContext context, AnnotatedTypeMetadata metadata ) {
		String[] expressions = (String[]) metadata.getAnnotationAttributes( AcrossCondition.class.getName() )
		                                          .get( "value" );

		for ( String expression : expressions ) {
			if ( !StringUtils.isBlank( expression )
					&& !evaluate( expression, context.getBeanFactory(), context.getEnvironment() ) ) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Based on implementation of OnExpressionCondition in spring-boot package.
	 * https://github.com/spring-projects/spring-boot/blob/master/spring-boot-autoconfigure/src/main/java/org/springframework/boot/autoconfigure/condition/OnExpressionCondition.java
	 */
	public static boolean evaluate( String unresolvedExpression,
	                                ConfigurableListableBeanFactory beanFactory,
	                                Environment environment ) {
		// Explicitly allow environment placeholders inside the expression
		String expression = environment.resolvePlaceholders( unresolvedExpression );

		if ( !expression.startsWith( "#{" ) ) {
			// For convenience allow user to provide bare expression with no #{} wrapper
			expression = "#{" + expression + "}";
		}

		BeanExpressionResolver resolver = beanFactory.getBeanExpressionResolver();
		BeanExpressionContext expressionContext = new CurrentModuleBeanExpressionContext( beanFactory, null,
		                                                                                  environment );

		if ( resolver == null ) {
			resolver = new StandardBeanExpressionResolver();
		}

		boolean result = (Boolean) resolver.evaluate( expression, expressionContext );

		if ( LOG.isTraceEnabled() ) {
			LOG.trace( "AcrossDepends expression {} evaluated to {}", expression, result );
		}

		return result;
	}

	private static final class CurrentModuleBeanExpressionContext extends BeanExpressionContext
	{
		private final Environment environment;

		private CurrentModuleBeanExpressionContext( ConfigurableListableBeanFactory beanFactory,
		                                            Scope scope,
		                                            Environment environment ) {
			super( beanFactory, scope );

			this.environment = environment;
		}

		// Provided for SPEL property
		public AcrossModule getCurrentModule() {
			try {
				return getBeanFactory().getBean( AcrossModule.class );
			}
			catch ( NoSuchBeanDefinitionException nsbe ) {
				return null;
			}
		}

		@Deprecated
		public AcrossModuleSettings getSettings() {
			try {
				LOG.warn(
						"The use of @AcrossCondition('settings') is deprecated - functionality will be removed in the future." );
				BeanDefinitionRegistry registry = (BeanDefinitionRegistry) getBeanFactory();

				ConfigurableListableBeanFactory beanFactory = (ConfigurableListableBeanFactory) getBeanFactory();
				String[] beanNames = beanFactory.getBeanNamesForType( AcrossModuleSettings.class );

				if ( beanNames.length >= 1 ) {
					AcrossModuleSettings settings
							= (AcrossModuleSettings) beanFactory.createBean( beanFactory.getType( beanNames[0] ) );
					settings.setEnvironment( environment );

					return settings;
				}
			}
			catch ( NoSuchBeanDefinitionException ignore ) {
			}

			return null;
		}

		/**
		 * Can this module find a bean with the given name in the context.
		 *
		 * @deprecated use {@link org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean} instead
		 */
		@Deprecated
		public boolean hasBean( String beanName ) {
			return getBeanFactory().containsBean( beanName );
		}

		/**
		 * Can this module find a bean with the given name and of the given type in the context.
		 *
		 * @deprecated use {@link org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean} instead
		 */
		@Deprecated
		public boolean hasBean( String beanName, Class beanType ) {
			return getBeanFactory().containsBean( beanName ) && getBeanFactory().isTypeMatch( beanName, beanType );
		}

		/**
		 * Can this module pick up a bean of the given type from the context.
		 *
		 * @deprecated use {@link org.springframework.boot.autoconfigure.condition.ConditionalOnBean} instead
		 */
		@Deprecated
		public boolean hasBeanOfType( Class beanType ) {
			return !BeanFactoryUtils.beansOfTypeIncludingAncestors( (ListableBeanFactory) getBeanFactory(), beanType )
			                        .isEmpty();
		}

		@Override
		public boolean equals( Object other ) {
			return other instanceof CurrentModuleBeanExpressionContext && super.equals( other );
		}

		@Override
		public int hashCode() {
			return super.hashCode();
		}
	}
}