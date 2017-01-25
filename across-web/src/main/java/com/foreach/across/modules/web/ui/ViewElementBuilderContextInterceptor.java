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
package com.foreach.across.modules.web.ui;

import com.foreach.across.modules.web.context.WebAppLinkBuilder;
import com.foreach.across.modules.web.resource.WebResourceUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Registers a global {@link ViewElementBuilderContext} on the request and attaches it as the
 * current thread-local builder context.
 * <p/>
 * Extend this interceptor if you want to register additional default attributes.
 *
 * @author Arne Vandamme
 * @since 2.0.0
 */
public class ViewElementBuilderContextInterceptor extends HandlerInterceptorAdapter
{
	private WebAppLinkBuilder webAppLinkBuilder;

	@Override
	public final boolean preHandle( HttpServletRequest request,
	                                HttpServletResponse response,
	                                Object handler ) throws Exception {
		ViewElementBuilderContext builderContext = createDefaultViewElementBuilderContext( request );
		WebResourceUtils.storeViewElementBuilderContext( builderContext, request );
		ViewElementBuilderContextHolder.setViewElementBuilderContext( builderContext );

		return true;
	}

	protected ViewElementBuilderContext createDefaultViewElementBuilderContext( HttpServletRequest request ) {
		DefaultViewElementBuilderContext builderContext = new DefaultViewElementBuilderContext();
		builderContext.setWebAppLinkBuilder( webAppLinkBuilder );
		return builderContext;
	}

	@Override
	public final void afterCompletion( HttpServletRequest request,
	                                   HttpServletResponse response,
	                                   Object handler,
	                                   Exception ex ) throws Exception {
		ViewElementBuilderContextHolder.clearViewElementBuilderContext();
	}

	@Autowired
	protected void setWebAppLinkBuilder( WebAppLinkBuilder webAppLinkBuilder ) {
		this.webAppLinkBuilder = webAppLinkBuilder;
	}
}
