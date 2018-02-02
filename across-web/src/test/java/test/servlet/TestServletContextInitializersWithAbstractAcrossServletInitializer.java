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
package test.servlet;

import com.foreach.across.AcrossPlatform;
import com.foreach.across.config.EnableAcrossContext;
import com.foreach.across.core.context.web.AcrossWebApplicationContext;
import com.foreach.across.modules.web.config.CharacterEncodingConfiguration;
import com.foreach.across.modules.web.servlet.AbstractAcrossServletInitializer;
import com.foreach.across.modules.web.servlet.AcrossWebDynamicServletConfigurer;
import org.junit.Test;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;
import test.modules.TestModules;
import test.modules.testResources.TestResourcesModule;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.util.EnumSet;

import static org.junit.Assert.assertTrue;

/**
 * @author Arne Vandamme
 */
public class TestServletContextInitializersWithAbstractAcrossServletInitializer
{
	@Test
	public void allServletContextInitializersShouldBeRegistered() {
		TomcatServletWebServerFactory embeddedServletContainerFactory = new TomcatServletWebServerFactory();
		embeddedServletContainerFactory.setPort( 0 );
		embeddedServletContainerFactory.getWebServer( new ServletContextInitializer()
		{
			@Override
			public void onStartup( ServletContext servletContext ) throws ServletException {
				AbstractAcrossServletInitializer init = new AbstractAcrossServletInitializer()
				{

					@Override
					public void onStartup( ServletContext servletContext ) throws ServletException {
						super.onStartup( servletContext );
						assertTrue( servletContext.getFilterRegistrations()
						                          .containsKey( CharacterEncodingConfiguration.FILTER_NAME ) );
					}

					@Override
					protected void configure( AcrossWebApplicationContext acrossWebApplicationContext ) {
						acrossWebApplicationContext.register( Config.class );
					}
				};
				init.onStartup( servletContext );
			}
		} ).start();
	}

	@Configuration
	@EnableAcrossContext(
			modules = TestResourcesModule.NAME,
			modulePackageClasses = { AcrossPlatform.class, TestModules.class }
	)
	public static class Config extends AcrossWebDynamicServletConfigurer
	{
		@Override
		protected void dynamicConfigurationAllowed( ServletContext servletContext ) throws ServletException {
			FilterRegistration.Dynamic filter = servletContext.addFilter( "corsFilter", new CorsFilter(
					new UrlBasedCorsConfigurationSource() ) );
			filter.addMappingForUrlPatterns( EnumSet.allOf( DispatcherType.class ), false, "/*" );
		}

		@Override
		protected void dynamicConfigurationDenied( ServletContext servletContext ) throws ServletException {

		}
	}
}
