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
package com.foreach.across.test;

import com.foreach.across.modules.web.AcrossWebModule;
import com.foreach.across.modules.web.config.CharacterEncodingConfiguration;
import com.foreach.across.modules.web.config.multipart.MultipartResolverConfiguration;
import com.foreach.across.modules.web.config.resources.ResourcesConfiguration;
import com.foreach.across.modules.web.servlet.AcrossMultipartFilter;
import org.junit.jupiter.api.Test;
import org.springframework.web.filter.CharacterEncodingFilter;
import org.springframework.web.servlet.HandlerAdapter;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.resource.ResourceUrlEncodingFilter;

import javax.servlet.DispatcherType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;

import static com.foreach.across.test.support.AcrossTestBuilders.web;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Arne Vandamme
 * @since 1.1.2
 */
public class TestAcrossWebModuleBootstrap
{
	@Test
	public void acrossWebModuleDefaultExposed() {
		try (AcrossTestWebContext ctx = web().modules( AcrossWebModule.NAME ).build()) {
			assertFalse( ctx.getBeansOfType( HandlerMapping.class ).isEmpty() );
			assertFalse( ctx.getBeansOfType( HandlerAdapter.class ).isEmpty() );
			assertFalse( ctx.getBeansOfType( HandlerExceptionResolver.class ).isEmpty() );
		}
	}

	@Test
	public void acrossWebModuleDefaultFilters() {
		try (AcrossTestWebContext ctx = web().modules( AcrossWebModule.NAME ).build()) {
			MockAcrossServletContext servletContext = ctx.getServletContext();
			assertNotNull( servletContext );

			assertCharacterEncodingFilter(
					servletContext.getFilterRegistration( CharacterEncodingConfiguration.FILTER_NAME )
			);
			assertResourceUrlEncodingFilter(
					servletContext.getFilterRegistration( ResourcesConfiguration.RESOURCE_URL_ENCODING_FILTER )
			);
			assertMultipartResolverFilter(
					servletContext.getFilterRegistration( MultipartResolverConfiguration.FILTER_NAME )
			);

			// Resource url encoding must be last
			Map<String, MockFilterRegistration> filterRegistrations = servletContext.getFilterRegistrations();
			assertEquals(
					ResourcesConfiguration.RESOURCE_URL_ENCODING_FILTER,
					new ArrayList<>( filterRegistrations.keySet() ).get( filterRegistrations.size() - 1 )
			);
		}
	}

	private void assertCharacterEncodingFilter( MockFilterRegistration registration ) {
		assertNotNull( registration );
		assertTrue( registration.getFilter() instanceof CharacterEncodingFilter );
		assertTrue( registration.isAsyncSupported() );
		assertEquals(
				Collections.singletonList(
						new MockFilterRegistration.MappingRule(
								true,
								false,
								EnumSet.of( DispatcherType.REQUEST, DispatcherType.ASYNC, DispatcherType.ERROR ),
								"/*"
						) ),
				registration.getMappingRules()
		);
	}

	private void assertResourceUrlEncodingFilter( MockFilterRegistration registration ) {
		assertNotNull( registration );
		assertTrue( registration.getFilter() instanceof ResourceUrlEncodingFilter );
		assertTrue( registration.isAsyncSupported() );
		assertEquals(
				Collections.singletonList(
						new MockFilterRegistration.MappingRule(
								true,
								true,
								EnumSet.of( DispatcherType.REQUEST, DispatcherType.ASYNC, DispatcherType.ERROR ),
								"/*"
						) ),
				registration.getMappingRules()
		);
	}

	private void assertMultipartResolverFilter( MockFilterRegistration registration ) {
		assertNotNull( registration );
		assertTrue( registration.getFilter() instanceof AcrossMultipartFilter );
		assertTrue( registration.isAsyncSupported() );
		assertEquals(
				Collections.singletonList(
						new MockFilterRegistration.MappingRule(
								true,
								false,
								EnumSet.of( DispatcherType.REQUEST, DispatcherType.ASYNC, DispatcherType.ERROR ),
								"/*"
						) ),
				registration.getMappingRules()
		);
	}

	@Test
	public void dynamicConfigurationButDisabledThroughProperties() {
		try (
				AcrossTestWebContext ctx = web()
						.property( "spring.servlet.multipart.enabled", "false" )
						.property( "server.servlet.encoding.enabled", "false" )
						.property( "across.web.resources.versioning.enabled", "false" )
						.modules( AcrossWebModule.NAME )
						.build()
		) {
			verifyNoFiltersRegistered( ctx );
		}
	}

	@Test
	public void dynamicConfigurationDisabled() {
		try (
				AcrossTestWebContext ctx = web()
						.dynamicServletContext( false )
						.modules( AcrossWebModule.NAME )
						.build()
		) {
			verifyNoFiltersRegistered( ctx );
		}
	}

	private void verifyNoFiltersRegistered( AcrossTestWebContext ctx ) {
		MockAcrossServletContext servletContext = ctx.getServletContext();
		assertNotNull( servletContext );
		assertTrue( servletContext.isInitialized() );

		assertNull( servletContext.getFilterRegistration( CharacterEncodingConfiguration.FILTER_NAME ) );
		assertNull( servletContext.getFilterRegistration( ResourcesConfiguration.RESOURCE_URL_ENCODING_FILTER ) );
		assertNull( servletContext.getFilterRegistration( MultipartResolverConfiguration.FILTER_NAME ) );
	}
}
