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
package com.foreach.across.config;

import com.foreach.across.config.IllegalConfigurationValidator.ModuleMatcher;
import com.foreach.across.core.context.info.AcrossModuleInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * @author Arne Vandamme
 * @since 3.0.0
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestIllegalConfigurationValidator
{
	@Mock
	private AcrossModuleInfo moduleInfo;

	@Test
	public void illegalOnApplicationButNotOnModule() {
		ModuleMatcher moduleMatcher = new ModuleMatcher( null, null );
		assertThat( moduleMatcher.test( null ) ).isTrue();
		assertThat( moduleMatcher.test( moduleInfo ) ).isFalse();

		moduleMatcher = new ModuleMatcher( null, "AcrossContext" );
		assertThat( moduleMatcher.test( null ) ).isTrue();
		assertThat( moduleMatcher.test( moduleInfo ) ).isFalse();
	}

	@Test
	public void allowedOnApplicationButNotModule() {
		ModuleMatcher moduleMatcher = new ModuleMatcher( "AcrossContext", null );
		assertThat( moduleMatcher.test( null ) ).isFalse();
		assertThat( moduleMatcher.test( moduleInfo ) ).isTrue();
	}

	@Test
	public void illegalAnywhere() {
		ModuleMatcher moduleMatcher = new ModuleMatcher( null, "AcrossContext,AcrossModule" );
		assertThat( moduleMatcher.test( null ) ).isTrue();
		assertThat( moduleMatcher.test( moduleInfo ) ).isTrue();
	}

	@Test
	public void illegalInSpecificModuleOnly() {
		ModuleMatcher moduleMatcher = new ModuleMatcher( null, "MyModule,OtherModule" );
		assertThat( moduleMatcher.test( null ) ).isFalse();

		when( moduleInfo.matchesModuleName( "MyModule" ) ).thenReturn( false );
		when( moduleInfo.matchesModuleName( "OtherModule" ) ).thenReturn( false );
		assertThat( moduleMatcher.test( moduleInfo ) ).isFalse();

		when( moduleInfo.matchesModuleName( "MyModule" ) ).thenReturn( true );
		when( moduleInfo.matchesModuleName( "OtherModule" ) ).thenReturn( false );
		assertThat( moduleMatcher.test( moduleInfo ) ).isTrue();

		when( moduleInfo.matchesModuleName( "MyModule" ) ).thenReturn( false );
		when( moduleInfo.matchesModuleName( "OtherModule" ) ).thenReturn( true );
		assertThat( moduleMatcher.test( moduleInfo ) ).isTrue();
	}

	@Test
	public void allowedInSpecificModulesOnly() {
		ModuleMatcher moduleMatcher = new ModuleMatcher( "MyModule,OtherModule", null );
		assertThat( moduleMatcher.test( null ) ).isTrue();

		when( moduleInfo.matchesModuleName( "MyModule" ) ).thenReturn( false );
		when( moduleInfo.matchesModuleName( "OtherModule" ) ).thenReturn( false );
		assertThat( moduleMatcher.test( moduleInfo ) ).isTrue();

		when( moduleInfo.matchesModuleName( "MyModule" ) ).thenReturn( true );
		when( moduleInfo.matchesModuleName( "OtherModule" ) ).thenReturn( false );
		assertThat( moduleMatcher.test( moduleInfo ) ).isFalse();

		when( moduleInfo.matchesModuleName( "MyModule" ) ).thenReturn( false );
		when( moduleInfo.matchesModuleName( "OtherModule" ) ).thenReturn( true );
		assertThat( moduleMatcher.test( moduleInfo ) ).isFalse();
	}
}

