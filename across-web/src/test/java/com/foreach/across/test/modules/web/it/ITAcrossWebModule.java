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
package com.foreach.across.test.modules.web.it;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foreach.across.config.EnableAcrossContext;
import com.foreach.across.core.context.bootstrap.AcrossBootstrapConfigurer;
import com.foreach.across.core.context.registry.AcrossContextBeanRegistry;
import com.foreach.across.modules.web.AcrossWebModule;
import com.foreach.across.modules.web.mvc.PrefixingRequestMappingHandlerMapping;
import com.foreach.across.modules.web.ui.ViewElementAttributeConverter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.HttpMessageConverters;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.validation.SmartValidator;
import org.springframework.validation.Validator;
import org.springframework.web.method.support.UriComponentsContributor;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import javax.servlet.ServletContext;

import static org.junit.Assert.*;

/**
 * @author Arne Vandamme
 * @since 3.0.0
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ITAcrossWebModule.Config.class)
public class ITAcrossWebModule extends AbstractWebIntegrationTest
{
	@Autowired
	private ApplicationContext applicationContext;

	@Autowired
	private AcrossContextBeanRegistry beanRegistry;

	@Autowired
	private ServletContext servletContext;

	@Test
	public void exposedAutoConfigurationBeans() {
		assertExposed( RestTemplateBuilder.class );
		assertExposed( HttpMessageConverters.class );
		assertExposed( ObjectMapper.class );
		assertExposed( Jackson2ObjectMapperBuilder.class );

		// Exposed from the post-processor
		assertExposed( UriComponentsContributor.class );
	}

	@Test
	public void mvcConversionServiceCreatedInAcrossWebModule() {
		FormattingConversionService acrossWebConversionService
				= applicationContext.getBean( AcrossWebModule.CONVERSION_SERVICE_BEAN, FormattingConversionService.class );
		assertSame( acrossWebConversionService, beanRegistry.getBeanFromModule( AcrossWebModule.NAME, AcrossWebModule.CONVERSION_SERVICE_BEAN ) );
		FormattingConversionService autoConfiguredConversionService
				= beanRegistry.getBeanFromModule( AcrossBootstrapConfigurer.CONTEXT_POSTPROCESSOR_MODULE, AcrossWebModule.CONVERSION_SERVICE_BEAN );
		assertSame( acrossWebConversionService, autoConfiguredConversionService );
	}

	@Test
	public void validatorShouldBeInitialized() {
		Validator validator = applicationContext.getBean( Validator.class );
		assertTrue( validator instanceof SmartValidator );
	}

	@Test
	public void exposedDomainBeans() {
		assertExposed( ViewElementAttributeConverter.class );
	}

	@Test
	public void registeredFilters() {
		assertNotNull( servletContext.getFilterRegistration( "characterEncodingFilter" ) );
	}

	@Test
	public void defaultRequestMappingIsExpectedToBePrefixedVariant() {
		RequestMappingHandlerMapping handlerMapping = applicationContext.getBean( RequestMappingHandlerMapping.class );
		assertTrue( handlerMapping instanceof PrefixingRequestMappingHandlerMapping );
		assertEquals( 0, handlerMapping.getOrder() );
	}

	private void assertExposed( Class<?> type ) {
		assertNotNull( applicationContext.getBean( type ) );
	}

	@EnableAcrossContext(modules = AcrossWebModule.NAME)
	@Configuration
	static class Config
	{
	}
}