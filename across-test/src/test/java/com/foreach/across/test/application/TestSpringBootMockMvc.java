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
package com.foreach.across.test.application;

import com.foreach.across.core.context.info.AcrossContextInfo;
import com.foreach.across.modules.web.AcrossWebModule;
import com.foreach.across.test.AcrossWebAppConfiguration;
import com.foreach.across.test.ExposeForTest;
import com.foreach.across.test.application.app.DummyApplication;
import com.foreach.across.test.application.app.application.controllers.NonExposedComponent;
import com.foreach.across.test.support.config.MockMvcConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Bootstrap using a {@link com.foreach.across.test.MockAcrossServletContext}.
 *
 * @author Arne Vandamme
 * @since 1.1.2
 */
@ExtendWith(SpringExtension.class)
@DirtiesContext
@ActiveProfiles("test")
@SpringBootTest(classes = { DummyApplication.class, MockMvcConfiguration.class })
@AcrossWebAppConfiguration
@ExposeForTest(NonExposedComponent.class)
public class TestSpringBootMockMvc
{
	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private AcrossContextInfo contextInfo;

	@Autowired(required = false)
	private NonExposedComponent nonExposedComponent;

	@Autowired(required = false)
	@Qualifier(DispatcherServletAutoConfiguration.DEFAULT_DISPATCHER_SERVLET_REGISTRATION_BEAN_NAME)
	private ServletRegistrationBean dispatcherServletRegistration;

	@Test
	public void modulesShouldBeRegistered() {
		assertTrue( contextInfo.hasModule( "emptyModule" ) );
		assertTrue( contextInfo.hasModule( AcrossWebModule.NAME ) );
		assertTrue( contextInfo.hasModule( "DummyApplicationModule" ) );
		assertTrue( contextInfo.hasModule( "DummyInfrastructureModule" ) );

		assertFalse( contextInfo.hasModule( "DummyPostProcessorModule" ) );
	}

	@Test
	public void controllersShouldSayHello() throws Exception {
		assertContent( "application says hello", get( "/application" ) );
		assertContent( "infrastructure says hello", get( "/infrastructure" ) );
	}

	@Test
	public void versionedResourceShouldBeReturned() throws Exception {
		assertContent( "hùllµ€", get( "/res/static/boot-1.0/testResources/test.txt" ) );
	}

	@Test
	public void manuallyExposedComponent() {
		assertNotNull( nonExposedComponent );
	}

	@Test
	public void multipartConfigurationShouldBeRegistered() {
		assertNotNull( dispatcherServletRegistration );
		assertNotNull( dispatcherServletRegistration.getMultipartConfig() );
	}

	private void assertContent( String expected, RequestBuilder requestBuilder ) throws Exception {
		mockMvc.perform( requestBuilder )
		       .andExpect( status().isOk() )
		       .andExpect( content().string( is( expected ) ) );
	}
}
