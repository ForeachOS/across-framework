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
package com.foreach.across.test.ordering.one;

import com.foreach.across.core.AcrossModule;
import com.foreach.across.core.annotations.OrderInModule;
import com.foreach.across.core.context.configurer.ApplicationContextConfigurer;
import com.foreach.across.core.context.configurer.ComponentScanConfigurer;
import com.foreach.across.test.ordering.MyComponent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.util.List;
import java.util.Set;

/**
 * @author Arne Vandamme
 * @since 3.0.0
 */
@Configuration
public class ModuleOne extends AcrossModule
{
	@Override
	public String getName() {
		return "ModuleOne";
	}

	@Override
	protected void registerDefaultApplicationContextConfigurers( Set<ApplicationContextConfigurer> contextConfigurers ) {
		contextConfigurers.add( ComponentScanConfigurer.forAcrossModule( ModuleOne.class ) );
	}

	@Bean
	public List<MyComponent> componentList( List<MyComponent> components ) {
		return components;
	}

	@Bean
	@OrderInModule(3)
	public ModuleOneComponentOne moduleOneComponentThree() {
		return new ModuleOneComponentOne();
	}

	@Bean
	@Order
	public ModuleOneComponentTwo moduleOneComponentFour() {
		return new ModuleOneComponentTwo();
	}
}
