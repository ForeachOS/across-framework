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
package com.foreach.across.test.modules.webtest.controllers;

import com.foreach.across.core.annotations.Exposed;
import com.foreach.across.modules.web.template.ClearTemplate;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Collections;
import java.util.Map;

@Exposed
@Controller
@ClearTemplate
public class RenderViewElementController
{
	public static final String PATH = "/renderViewElement";

	private Callback callback;
	private Object element;
	private String template;

	public void setElement( Object element ) {
		this.element = element;
	}

	public void setCallback( Callback callback ) {
		this.callback = callback;
	}

	public void setTemplate( String template ) {
		this.template = template;
	}

	@RequestMapping(PATH)
	public String render( ModelMap model ) {
		if ( callback != null ) {
			callback.prepareModel( model );
		}

		Map<String, String> controllerAttributes = Collections.singletonMap( "name", "RenderViewElementController" );

		model.put( "controllerAttributes", controllerAttributes );
		model.put( "element", element );

		return StringUtils.isNotEmpty( template ) ? template : "th/WebTestModule/renderViewElement";
	}

	public interface Callback
	{
		void prepareModel( ModelMap model );
	}
}
