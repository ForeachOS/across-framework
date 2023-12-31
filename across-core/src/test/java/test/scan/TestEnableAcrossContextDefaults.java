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
package test.scan;

import com.foreach.across.config.AcrossContextConfigurer;
import com.foreach.across.config.EnableAcrossContext;
import com.foreach.across.core.AcrossContext;
import com.foreach.across.core.EmptyAcrossModule;
import com.foreach.across.core.context.info.AcrossContextInfo;
import com.foreach.across.core.context.registry.AcrossContextBeanRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import test.scan.packageOne.ExtendedValidModule;
import test.scan.packageOne.ValidModule;
import test.scan.packageTwo.OtherValidModule;
import test.scan.packageTwo.YetAnotherValidModule;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Arne Vandamme
 */
@ExtendWith(SpringExtension.class)
@DirtiesContext
@ContextConfiguration(classes = TestEnableAcrossContextDefaults.Config.class)
public class TestEnableAcrossContextDefaults
{
	@Autowired
	private AcrossContextInfo contextInfo;

	@Autowired
	private AcrossContextBeanRegistry beanRegistry;

	@Autowired
	private ExtendedValidModule extendedValidModule;

	@Test
	public void beansOverrideScan() {
		assertTrue( contextInfo.hasModule( ExtendedValidModule.NAME ) );
		assertSame( extendedValidModule, contextInfo.getModuleInfo( ExtendedValidModule.NAME ).getModule() );
	}

	@Test
	public void configurerOverridesScan() {
		assertTrue( contextInfo.getModuleInfo( ValidModule.NAME ).getModule() instanceof EmptyAcrossModule );
	}

	@Test
	public void optionalDependencyShouldNotBeAdded() {
		assertFalse( contextInfo.hasModule( OtherValidModule.NAME ) );
	}

	@Test
	public void scannedModulesShouldBeAdded() {
		assertTrue( contextInfo.hasModule( ValidModule.NAME ) );
		assertTrue( contextInfo.hasModule( YetAnotherValidModule.NAME ) );
	}

	@Test
	public void beanFourAndFiveShouldHaveBeenCreated() {
		assertTrue( beanRegistry.moduleContainsLocalBean( ValidModule.NAME, "beanFour" ) );
		assertTrue( beanRegistry.moduleContainsLocalBean( ValidModule.NAME, "beanFive" ) );
		assertTrue( beanRegistry.moduleContainsLocalBean( YetAnotherValidModule.NAME, "beanFour" ) );
		assertTrue( beanRegistry.moduleContainsLocalBean( YetAnotherValidModule.NAME, "beanFive" ) );
	}

	@Configuration
	@EnableAcrossContext({ "ExtendedValidModule", "ValidModule", "YetAnotherValidModule" })
	static class Config implements AcrossContextConfigurer
	{
		@Bean
		public ExtendedValidModule extendedValidModule() {
			return new ExtendedValidModule();
		}

		@Override
		public void configure( AcrossContext context ) {
			// Replace module
			context.addModule( new EmptyAcrossModule( ValidModule.NAME ) );

			// Verify configuration scan packages set
			assertArrayEquals(
					new String[] { "test.scan.config",
					               "test.scan.extensions" },
					context.getModuleConfigurationScanPackages()
			);
		}
	}
}
