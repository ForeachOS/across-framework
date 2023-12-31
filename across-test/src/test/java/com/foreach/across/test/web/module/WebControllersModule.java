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
package com.foreach.across.test.web.module;

import com.foreach.across.core.AcrossModule;
import com.foreach.across.core.annotations.AcrossDepends;
import com.foreach.across.core.context.configurer.ApplicationContextConfigurer;
import com.foreach.across.core.context.configurer.ComponentScanConfigurer;
import com.foreach.across.modules.web.AcrossWebModule;
import com.foreach.across.test.web.module.controllers.DefaultController;
import com.foreach.across.test.web.module.ui.MainTemplate;

import java.util.Set;

/**
 * @author Arne Vandamme
 * @since 1.1.3
 */
@AcrossDepends(required = AcrossWebModule.NAME)
public class WebControllersModule extends AcrossModule
{
	@Override
	public String getName() {
		return "WebControllersModule";
	}

	@Override
	public String getDescription() {
		return "Contains test controllers for integration testing templates and registry";
	}

	@Override
	protected void registerDefaultApplicationContextConfigurers( Set<ApplicationContextConfigurer> contextConfigurers ) {
		contextConfigurers.add( new ComponentScanConfigurer( MainTemplate.class, DefaultController.class ) );
	}
}
