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
package com.foreach.across.modules.web.config;

import com.foreach.across.condition.ConditionalOnConfigurableServletContext;
import com.foreach.across.core.context.info.AcrossContextInfo;
import com.foreach.across.core.events.AcrossContextBootstrappedEvent;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.boot.web.servlet.ServletContextInitializerBeans;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import javax.servlet.Filter;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import java.util.Collection;

/**
 * Registers all {@link ServletContextInitializer} beans created in any module or in the Across context itself. Picks up any
 * {@link Servlet} and {@link Filter} beans and converts them to registrations using {@link ServletContextInitializerBeans}.
 * <p/>
 * Any initializers created in the parent context should be registered by the Spring Boot {@link org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext},
 * or manually in a non-embedded configuration.
 *
 * @author Arne Vandamme
 * @see ServletContextInitializerBeans
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnConfigurableServletContext
public class DynamicServletConfiguration
{
	private final ServletContext servletContext;

	@EventListener
	@SneakyThrows
	public void registerServletsAndFilters( AcrossContextBootstrappedEvent bootstrappedEvent ) {
		Collection<ServletContextInitializer> initializers = retrieveInitializersCreatedInAcrossContextOrAnyChildModule( bootstrappedEvent.getContext() );

		if ( !initializers.isEmpty() ) {
			LOG.info( "Found {} ServletContextInitializer beans found in the Across context", initializers.size() );

			for ( ServletContextInitializer i : initializers ) {
				LOG.debug( "Registering ServletContextInitializer - {}", i );
				i.onStartup( servletContext );
			}
		}

	}

	private Collection<ServletContextInitializer> retrieveInitializersCreatedInAcrossContextOrAnyChildModule( AcrossContextInfo contextInfo ) {
		return new ServletContextInitializerBeans(
				(ListableBeanFactory) contextInfo.getApplicationContext().getAutowireCapableBeanFactory()
		);
	}
}
