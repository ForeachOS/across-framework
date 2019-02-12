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
package com.foreach.across.modules.web.resource;

import com.foreach.across.config.AcrossContextConfigurer;
import com.foreach.across.core.AcrossContext;
import com.foreach.across.core.context.info.AcrossContextInfo;
import com.foreach.across.modules.web.AcrossWebModule;
import com.foreach.across.test.AcrossTestWebConfiguration;
import com.foreach.across.test.modules.webtest.WebTestModule;
import com.foreach.across.test.modules.webtest.controllers.WebResourceController;
import com.foreach.across.test.modules.webtest.controllers.WebResourcePackageController;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static com.foreach.across.utils.CustomResultMatchers.jsoup;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
@WebAppConfiguration(value = "classpath:")
@ContextConfiguration(classes = ITWebResourcePackage.Config.class)
public class ITWebResourcePackage
{
	@Autowired
	private AcrossContextInfo contextInfo;

	private MockMvc mockMvc;

	@Before
	public void initMvc() {
		mockMvc = MockMvcBuilders.webAppContextSetup( (WebApplicationContext) contextInfo.getApplicationContext() ).build();
	}

	@Test
	public void testInlineJavascriptRendered() throws Exception {
		mockMvc.perform( get( WebResourcePackageController.PATH ) )
		       .andExpect( jsoup().elementById( "not-inline-or-data-css" ).valueIgnoringLineEndings( "<link rel=\"stylesheet\" href=\"//maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css\">" ) )
		       .andExpect( jsoup().elementById( "javascript-page-end" ).valueIgnoringLineEndings( "<script src=\"//maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js\"></script>" ) )
		       .andExpect( status().isOk() )
		;

	}

	@Configuration
	@AcrossTestWebConfiguration
	protected static class Config implements AcrossContextConfigurer
	{
		@Override
		public void configure( AcrossContext context ) {
			context.addModule( new WebTestModule() );
			context.addModule( new AcrossWebModule() );
		}
	}
}