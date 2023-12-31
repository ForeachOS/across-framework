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
package test.scan.extensions;

import com.foreach.across.core.annotations.ModuleConfiguration;
import test.scan.packageTwo.OtherValidModule;
import org.springframework.context.annotation.Bean;
import test.scan.packageTwo.OtherValidModule;

/**
 * @author Arne Vandamme
 */
@ModuleConfiguration(exclude = OtherValidModule.NAME)
public class BeanFourAndFiveConfiguration
{
	@Bean
	public String beanFour() {
		return "beanFour";
	}

	@ModuleConfiguration
	public static class BeanFiveConfiguration
	{
		@Bean
		public String beanFive() {
			return "beanFive";
		}
	}
}
