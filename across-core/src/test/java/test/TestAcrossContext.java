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

package test;

import com.foreach.across.core.AcrossContext;
import com.foreach.across.core.installers.InstallerAction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import test.modules.exposing.ExposingModule;
import test.modules.installer.InstallerModule;
import test.modules.module1.TestModule1;
import test.modules.module2.TestModule2;

import static org.junit.jupiter.api.Assertions.*;

public class TestAcrossContext
{
	@Test
	void withinSameJvmIdChanges() {
		String firstId = new AcrossContext().getId();
		String secondId = new AcrossContext().getId();

		assertNotNull( firstId );
		assertNotNull( secondId );
		assertNotEquals( firstId, secondId );
	}

	@Test
	void displayNameAndId() {
		AcrossContext ctx = new AcrossContext();
		String id = ctx.getId();
		assertNotNull( id );
		assertEquals( id, ctx.getDisplayName() );
		ctx.setDisplayName( "My Application" );
		assertNotEquals( "My Application", ctx.getId() );
		assertEquals( id, ctx.getId() );
		assertEquals( "My Application", ctx.getDisplayName() );
	}

	@Test
	void getModuleByName() {
		TestModule1 module = new TestModule1();
		ExposingModule other = new ExposingModule( "my module" );

		AcrossContext context = new AcrossContext();
		context.addModule( module );
		context.addModule( other );

		assertNull( context.getModule( "not present" ) );
		assertSame( module, context.getModule( module.getName() ) );
		assertSame( other, context.getModule( "my module" ) );
	}

	@Test
	void getValidTypedModuleByName() {
		TestModule1 module = new TestModule1();
		ExposingModule other = new ExposingModule( "my module" );

		AcrossContext context = new AcrossContext();
		context.addModule( module );
		context.addModule( other );

		ExposingModule fetched = context.getModule( "my module", ExposingModule.class );
		assertNotNull( fetched );
		assertSame( other, fetched );
	}

	@Test
	void getInvalidTypedModuleByName() {
		Assertions.assertThrows( ClassCastException.class, () -> {
			TestModule1 module = new TestModule1();
			ExposingModule other = new ExposingModule( "my module" );

			AcrossContext context = new AcrossContext();
			context.addModule( module );
			context.addModule( other );

			context.getModule( "my module", TestModule1.class );
		} );
	}

	@Test
	void dataSourceIsNotRequiredIfNoInstallers() {
		AcrossContext context = new AcrossContext();
		context.setInstallerAction( InstallerAction.EXECUTE );
		context.addModule( new TestModule1() );

		context.bootstrap();
	}

	@Test
	void dataSourceIsNotRequiredIfInstallersWontRun() {
		AcrossContext context = new AcrossContext();
		// Default installer action is disabled
		context.addModule( new InstallerModule() );

		context.bootstrap();
	}

	@Test
	void dataSourceIsRequiredIfInstallersWantToRun() {
		AcrossContext context = new AcrossContext();
		context.setInstallerAction( InstallerAction.EXECUTE );
		context.addModule( new InstallerModule() );

		boolean failed = false;

		try {
			context.bootstrap();
		}
		catch ( RuntimeException re ) {
			failed = true;
		}

		assertTrue( failed, "A datasource should be required if installers want to run." );
	}

	@Test
	void unableToAddModuleAfterBootstrap() {
		AcrossContext context = new AcrossContext();
		context.bootstrap();
	}

	@Test
	void sameModuleIsNotAllowed() {
		AcrossContext context = new AcrossContext();

		TestModule1 duplicate = new TestModule1();

		context.addModule( duplicate );
		context.addModule( new TestModule2() );

		boolean failed = false;

		try {
			context.addModule( duplicate );
		}
		catch ( RuntimeException re ) {
			failed = true;
		}

		assertTrue( failed, "Adding same module instance should not be allowed" );
	}

	@Test
	void sameModuleCanOnlyBeInOneAcrossContext() {
		TestModule1 module = new TestModule1();

		AcrossContext contextOne = new AcrossContext();
		contextOne.addModule( module );

		AcrossContext contextTwo = new AcrossContext();

		boolean failed = false;

		try {
			contextTwo.addModule( module );
		}
		catch ( RuntimeException re ) {
			failed = true;
		}

		assertTrue( failed, "Adding same module to another Across context should not be allowed" );
	}
}
