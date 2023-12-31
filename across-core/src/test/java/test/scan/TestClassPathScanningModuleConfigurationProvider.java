/*
 * Copyright 2019 the original author or authors
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
package test.scan;

import com.foreach.across.core.context.ClassPathScanningModuleConfigurationProvider;
import com.foreach.across.core.context.ModuleConfigurationSet;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import test.scan.moduleExtendingValidModule.ModuleExtendingValidModule;
import test.scan.moduleExtendingValidModule.config.BeanOneConfiguration;
import test.scan.moduleExtendingValidModule.extensions.BeanTwoConfiguration;
import test.scan.moduleExtendingValidModule.extensions.SameBeanConfiguration;
import test.scan.packageOne.ValidModule;
import test.scan.packageTwo.OtherValidModule;

import static com.foreach.across.core.context.module.ModuleConfigurationExtension.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Arne Vandamme
 */
class TestClassPathScanningModuleConfigurationProvider
{
	private ClassPathScanningModuleConfigurationProvider provider
			= new ClassPathScanningModuleConfigurationProvider( new PathMatchingResourcePatternResolver() );

	@Test
	void noAnnotatedClasses() {
		ModuleConfigurationSet configurationSet = provider.scan( "illegal" );

		assertNotNull( configurationSet );
		assertEquals( 0, configurationSet.getConfigurations( ValidModule.NAME ).length );
		assertEquals( 0, configurationSet.getConfigurations( OtherValidModule.NAME ).length );
		assertEquals( 0, configurationSet.getConfigurations( "badModule" ).length );
	}

	@Test
	void annotatedClassForAllModulesExceptOne() {
		ModuleConfigurationSet configurationSet = provider.scan(
				"test.scan.moduleExtendingValidModule.config"
		);

		assertNotNull( configurationSet );
		assertThat( configurationSet.getConfigurations( ValidModule.NAME ) )
				.containsExactly( of( BeanOneConfiguration.class.getName(), true, false ) );
		assertThat( configurationSet.getConfigurations( OtherValidModule.NAME ) )
				.containsExactly( of( BeanOneConfiguration.class.getName(), true, false ) );
		assertThat( configurationSet.getConfigurations( "badModule" ) )
				.containsExactly( of( BeanOneConfiguration.class.getName(), true, false ) );

		// excluded
		assertEquals( 0, configurationSet.getConfigurations( ModuleExtendingValidModule.NAME ).length );
	}

	@Test
	void multiplePackages() {
		ModuleConfigurationSet configurationSet = provider.scan(
				"test.scan.moduleExtendingValidModule.config",
				"test.scan.moduleExtendingValidModule.extensions"
		);

		assertNotNull( configurationSet );
		assertThat( configurationSet.getConfigurations( ValidModule.NAME ) )
				.containsExactly(
						of( BeanOneConfiguration.class.getName(), true, false ),
						of( BeanTwoConfiguration.class.getName(), false, false ),
						of( SameBeanConfiguration.class.getName(), true, false )
				);

		assertThat( configurationSet.getConfigurations( OtherValidModule.NAME ) )
				.containsExactly( of( BeanOneConfiguration.class.getName(), true, false ) );

		assertThat( configurationSet.getConfigurations( "badModule" ) )
				.containsExactly( of( BeanOneConfiguration.class.getName(), true, false ) );

		assertEquals( 0, configurationSet.getConfigurations( ModuleExtendingValidModule.NAME ).length );
	}
}
