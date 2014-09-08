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

package com.foreach.across.modules.web.mvc;

import org.springframework.context.ApplicationContext;
import org.springframework.web.servlet.handler.AbstractHandlerMapping;
import org.springframework.web.servlet.handler.SimpleUrlHandlerMapping;

import javax.servlet.ServletContext;
import java.util.Collections;
import java.util.Map;

@SuppressWarnings("all")
public class ResourceHandlerRegistry extends org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry
{
	public ResourceHandlerRegistry( ApplicationContext applicationContext, ServletContext servletContext ) {
		super( applicationContext, servletContext );
	}

	@Override
	public AbstractHandlerMapping getHandlerMapping() {
		return super.getHandlerMapping();
	}

	public Map<String, ?> getUrlMap() {
		AbstractHandlerMapping mapping = getHandlerMapping();

		if ( mapping instanceof SimpleUrlHandlerMapping ) {
			return ( (SimpleUrlHandlerMapping) mapping ).getUrlMap();
		}

		return Collections.emptyMap();
	}
}
