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

package test;

import com.foreach.across.core.AcrossModule;
import com.foreach.across.core.EmptyAcrossModule;
import com.foreach.across.core.annotations.AcrossDepends;
import com.foreach.across.core.annotations.AcrossRole;
import com.foreach.across.core.context.AcrossModuleRole;
import com.foreach.across.core.context.bootstrap.ModuleBootstrapOrderBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.core.Ordered;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestAcrossModuleLoadingOrderOne
{
	private ModuleOne one = new ModuleOne();
	private ModuleTwo two = new ModuleTwo();
	private ModuleThree three = new ModuleThree();
	private ModuleFour requiresTwo = new ModuleFour();
	private ModuleFive requiresTwoThreeAndOptionalOne = new ModuleFive();
	private ModuleSix cyclicOne = new ModuleSix();
	private ModuleSeven cyclicTwo = new ModuleSeven();
	private ModuleEight infrastructureRequiringTwo = new ModuleEight();
	private ModuleNine requiresTwoAndOptionalThreeTen = new ModuleNine();
	private ModuleTen requiresOneAndOptionalTwoNine = new ModuleTen();
	private ModuleEleven postProcessorRequiringOther = new ModuleEleven();
	private ModuleTwelve postProcessor = new ModuleTwelve();
	private ModuleFifteen postProcessorOptionalOther = new ModuleFifteen();
	private ModuleThirteen infraOne = new ModuleThirteen();
	private ModuleFourteen infraOptionalInfraOne = new ModuleFourteen();
	private ModuleSixteen firstInfrastructure = new ModuleSixteen();

	@Test
	public void resetEnabled() {
		infrastructureRequiringTwo.setEnabled( true );
	}

	@Test
	public void addingOrderIsKeptIfNoDependencies() {
		Collection<AcrossModule> added = list( one, two, three );
		Collection<AcrossModule> ordered = order( added );

		assertEquals( added, ordered );
	}

	@Test
	public void runtimeDependencyInfluencesOrder() {
		ModuleTwo runtime = new ModuleTwo();

		Collection<AcrossModule> added = list( one, runtime, three );
		Collection<AcrossModule> ordered = order( added );

		assertEquals( added, ordered );

		runtime.addRuntimeDependency( three.getName() );

		ordered = order( added );
		assertEquals( list( one, three, runtime ), ordered );
	}

	@Test
	public void addingOrderIsKeptBetweenDependencies() {
		Collection<AcrossModule> added = list( one, requiresTwo, two, three );
		Collection<AcrossModule> ordered = order( added );

		assertEquals( list( one, two, requiresTwo, three ), ordered );

		// No change to adding order since dependency is met in the adding order
		added = list( one, two, three, requiresTwo );
		ordered = order( added );

		assertEquals( list( one, two, three, requiresTwo ), ordered );
	}

	@Test
	public void optionalAndRequiredMixed() {
		Collection<AcrossModule> added = list( requiresTwoThreeAndOptionalOne, one, requiresTwo, two, three );
		Collection<AcrossModule> ordered = order( added );

		assertEquals( list( one, two, three, requiresTwoThreeAndOptionalOne, requiresTwo ), ordered );
	}

	@Test
	public void disabledOptionalDependencyHasNoImpact() {
		one.setEnabled( false );

		Collection<AcrossModule> added = list( requiresTwoThreeAndOptionalOne, one, requiresTwo, two, three );
		Collection<AcrossModule> ordered = order( added );

		assertEquals( list( one, two, three, requiresTwoThreeAndOptionalOne, requiresTwo ), ordered );
	}

	@Test
	public void disabledRequiredDependencyCausesException() {
		Assertions.assertThrows( RuntimeException.class, () -> {
			two.setEnabled( false );

			Collection<AcrossModule> added = list( requiresTwoThreeAndOptionalOne, one, requiresTwo, two, three );
			Collection<AcrossModule> ordered = order( added );

			assertEquals( list( two, three, one, requiresTwoThreeAndOptionalOne, requiresTwo ), ordered );
		} );
	}

	@Test
	public void infrastructureModuleGetsPushedToTheFirstPossibleSpot() {
		Collection<AcrossModule> added =
				list( requiresTwoThreeAndOptionalOne, one, requiresTwo, two, three, infrastructureRequiringTwo );
		Collection<AcrossModule> ordered = order( added );

		assertEquals( list( two, infrastructureRequiringTwo, one, three, requiresTwoThreeAndOptionalOne, requiresTwo ),
		              ordered );
	}

	@Test
	public void optionalAndOrderedInfrastructureOrdering() {
		Collection<AcrossModule> added = list( one, infraOptionalInfraOne, infraOne, firstInfrastructure );
		Collection<AcrossModule> ordered = order( added );

		assertEquals( list( firstInfrastructure, infraOne, infraOptionalInfraOne, one ), ordered );
	}

	@Test
	public void disablingInfrastructureModuleIsNoProblem() {
		Collection<AcrossModule> added =
				list( requiresTwoThreeAndOptionalOne, one, requiresTwo, two, three, infrastructureRequiringTwo );
		infrastructureRequiringTwo.setEnabled( false );

		ModuleBootstrapOrderBuilder moduleBootstrapOrderBuilder = new ModuleBootstrapOrderBuilder();
		moduleBootstrapOrderBuilder.setSourceModules( added );
		Collection<AcrossModule> ordered = moduleBootstrapOrderBuilder.getOrderedModules();

		assertEquals( list( two, infrastructureRequiringTwo, one, three, requiresTwoThreeAndOptionalOne, requiresTwo ),
		              ordered );

		Collection<AcrossModule> dependencies = moduleBootstrapOrderBuilder.getConfiguredRequiredDependencies( one );
		assertFalse( dependencies.contains( infrastructureRequiringTwo ) );
	}

	@Test
	public void postProcessorModulesGetPushedToLastPossibleSpot() {
		Collection<AcrossModule> added = list( postProcessorRequiringOther, postProcessorOptionalOther, postProcessor,
		                                       one, requiresTwo, two, three, infrastructureRequiringTwo );
		Collection<AcrossModule> ordered = order( added );

		assertEquals( list( two, infrastructureRequiringTwo, one, requiresTwo, three, postProcessor,
		                    postProcessorRequiringOther, postProcessorOptionalOther ), ordered );
	}

	@Test
	public void missingRequiredDependencyWillBreak() {
		Assertions.assertThrows( RuntimeException.class, () -> {
			Collection<AcrossModule> added = list( one, requiresTwo );
			order( added );
		} );
	}

	@Test
	public void cyclicDependencyWillBreak() {
		Assertions.assertThrows( RuntimeException.class, () -> {
			Collection<AcrossModule> added =
					list( requiresTwoThreeAndOptionalOne, one, requiresTwo, two, three, cyclicOne, cyclicTwo );
			order( added );
		} );
	}

	@Test
	public void cyclicOptionalDependencyShouldWork() {
		Collection<AcrossModule> added =
				list( requiresTwoAndOptionalThreeTen, requiresOneAndOptionalTwoNine, one, two, three );
		Collection<AcrossModule> ordered = order( added );

		assertEquals( list( three, two, one, requiresTwoAndOptionalThreeTen, requiresOneAndOptionalTwoNine ), ordered );
	}

	@Test
	public void missingRuntimeDependencyWillBreak() {
		Assertions.assertThrows( RuntimeException.class, () -> {
			ModuleTwo twoWithRuntimeDependency = new ModuleTwo();
			twoWithRuntimeDependency.addRuntimeDependency( three.getName() );

			Collection<AcrossModule> added = list( one, twoWithRuntimeDependency );
			order( added );
		} );
	}

	@Test
	public void emptyAcrossModuleForDependencySatisfying() {
		EmptyAcrossModule fakeTwo = new EmptyAcrossModule( two.getName() );

		Collection<AcrossModule> added = list( one, requiresTwo, fakeTwo );
		Collection<AcrossModule> ordered = order( added );

		assertEquals( list( one, fakeTwo, requiresTwo ), ordered );
	}

	@Test
	public void complexRuntimeDependencies() {
		EmptyAcrossModule one = new EmptyAcrossModule( "one" );

		EmptyAcrossModule two = new EmptyAcrossModule( "two" );
		two.addRuntimeDependency( "five" );

		EmptyAcrossModule three = new EmptyAcrossModule( "three" );
		three.addRuntimeDependency( "one" );
		three.addRuntimeDependency( "six" );

		EmptyAcrossModule four = new EmptyAcrossModule( "four" );
		four.addRuntimeDependency( "three" );

		EmptyAcrossModule five = new EmptyAcrossModule( "five" );
		five.addRuntimeDependency( "three" );
		five.addRuntimeDependency( "four" );

		EmptyAcrossModule six = new EmptyAcrossModule( "six" );
		six.addRuntimeDependency( "one" );

		Collection<AcrossModule> added = list( one, two, three, four, five, six );
		Collection<AcrossModule> ordered = order( added );

		assertEquals( list( one, six, three, four, five, two ), ordered );
	}

	private Collection<AcrossModule> order( Collection<AcrossModule> list ) {
		ModuleBootstrapOrderBuilder bootstrapOrderBuilder = new ModuleBootstrapOrderBuilder();
		bootstrapOrderBuilder.setSourceModules( list );
		return bootstrapOrderBuilder.getOrderedModules();
	}

	private Collection<AcrossModule> list( AcrossModule... modules ) {
		return Arrays.asList( modules );
	}

	public static class ModuleOne extends AcrossModule
	{
		@Override
		public String getName() {
			return "ModuleOne";
		}

		@Override
		public String getDescription() {
			return null;
		}
	}

	public static class ModuleTwo extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleTwo";
		}
	}

	public static class ModuleThree extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleThree";
		}
	}

	@AcrossDepends(required = "ModuleTwo")
	public static class ModuleFour extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleFour";
		}
	}

	@AcrossDepends(required = { "ModuleTwo", "ModuleThree" }, optional = { "ModuleOne", "NonExistingModule" })
	public static class ModuleFive extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleFive";
		}
	}

	@AcrossDepends(required = "ModuleSeven")
	public static class ModuleSix extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleSix";
		}
	}

	@AcrossDepends(required = "ModuleSix")
	public static class ModuleSeven extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleSeven";
		}
	}

	@AcrossRole(AcrossModuleRole.INFRASTRUCTURE)
	@AcrossDepends(required = "ModuleTwo")
	public static class ModuleEight extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleEight";
		}
	}

	@AcrossDepends(required = "ModuleTwo", optional = { "ModuleThree", "ModuleTen" })
	public static class ModuleNine extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleNine";
		}
	}

	@AcrossDepends(required = "ModuleOne", optional = { "ModuleTwo", "ModuleNine" })
	public static class ModuleTen extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleTen";
		}
	}

	@AcrossDepends(required = "ModuleTwelve")
	@AcrossRole(AcrossModuleRole.POSTPROCESSOR)
	public static class ModuleEleven extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleEleven";
		}
	}

	@AcrossRole(AcrossModuleRole.POSTPROCESSOR)
	public static class ModuleTwelve extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleTwelve";
		}
	}

	@AcrossRole(AcrossModuleRole.INFRASTRUCTURE)
	public static class ModuleThirteen extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleThirteen";
		}
	}

	@AcrossRole(AcrossModuleRole.INFRASTRUCTURE)
	@AcrossDepends(optional = "ModuleThirteen")
	public static class ModuleFourteen extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleFourteen";
		}
	}

	@AcrossRole(AcrossModuleRole.POSTPROCESSOR)
	@AcrossDepends(optional = "ModuleEleven")
	public static class ModuleFifteen extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleFifteen";
		}
	}

	@AcrossRole(value = AcrossModuleRole.INFRASTRUCTURE, order = Ordered.HIGHEST_PRECEDENCE)
	public static class ModuleSixteen extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleSixteen";
		}
	}
}

