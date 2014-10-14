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

package com.foreach.across.test.context;

import com.foreach.across.core.AcrossContext;
import com.foreach.across.core.AcrossModule;
import com.foreach.across.core.annotations.Module;
import com.foreach.across.core.annotations.RefreshableCollection;
import com.foreach.across.core.context.AcrossContextUtils;
import com.foreach.across.core.context.configurer.AnnotatedClassConfigurer;
import com.foreach.across.core.context.configurer.ApplicationContextConfigurer;
import com.foreach.across.core.context.info.AcrossModuleInfo;
import com.foreach.across.core.context.registry.AcrossContextBeanRegistry;
import com.foreach.across.core.registry.IncrementalRefreshableRegistry;
import com.foreach.across.core.registry.RefreshableRegistry;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.ResolvableType;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class TestContextScanning
{
	@Test
	public void beansShouldBeReturnedInTheRegisteredOrderOfTheModules() {
		AcrossContext context = new AcrossContext();
		context.addModule( new ModuleOne() );
		context.addModule( new ModuleTwo() );
		context.addModule( new ModuleThree() );
		context.bootstrap();

		AcrossContextBeanRegistry registry = AcrossContextUtils.getBeanRegistry( context );
		List<MyBeanConfig> beans = registry.getBeansOfType( MyBeanConfig.class );
		assertTrue( beans.isEmpty() );

		beans = registry.getBeansOfType( MyBeanConfig.class, true );
		assertEquals( 3, beans.size() );

		assertEquals( "ModuleOne", beans.get( 0 ).getModule() );
		assertEquals( "ModuleTwo", beans.get( 1 ).getModule() );
		assertEquals( "ModuleThree", beans.get( 2 ).getModule() );

		context.shutdown();
	}

	@Test
	public void internalGenericBeanResolving() {
		AcrossContext context = new AcrossContext();
		context.addModule( new ModuleOne() );
		context.addModule( new ModuleTwo() );
		context.addModule( new ModuleThree() );
		context.bootstrap();

		AcrossContextBeanRegistry registry = AcrossContextUtils.getBeanRegistry( context );
		List<GenericBean> beans = registry.getBeansOfType( GenericBean.class, true );

		assertEquals( 6, beans.size() );

		ResolvableType listType = ResolvableType.forClassWithGenerics( List.class, Integer.class );
		ResolvableType type = ResolvableType.forClassWithGenerics( GenericBean.class,
		                                                           ResolvableType.forClass( Long.class ),
		                                                           listType
		);

		beans = registry.getBeansOfType( type, true );
		assertEquals( 3, beans.size() );
		assertEquals( "longWithIntegerList", beans.get( 0 ).getName() );
		assertEquals( "longWithIntegerList", beans.get( 1 ).getName() );
		assertEquals( "longWithIntegerList", beans.get( 2 ).getName() );

		listType = ResolvableType.forClassWithGenerics( List.class, Date.class );
		type = ResolvableType.forClassWithGenerics( GenericBean.class,
		                                            ResolvableType.forClass( String.class ),
		                                            listType
		);

		beans = registry.getBeansOfType( type, true );
		assertEquals( 3, beans.size() );
		assertEquals( "stringWithDateList", beans.get( 0 ).getName() );
		assertEquals( "stringWithDateList", beans.get( 1 ).getName() );
		assertEquals( "stringWithDateList", beans.get( 2 ).getName() );
	}

	@Test
	public void refreshableCollectionTesting() {
		AcrossContext context = new AcrossContext();
		context.addModule( new ModuleOne() );
		context.addModule( new ModuleTwo() );
		context.bootstrap();

		AcrossContextBeanRegistry registry = AcrossContextUtils.getBeanRegistry( context );
		MyBeanConfig one = registry.getBeanOfTypeFromModule( "ModuleOne", MyBeanConfig.class );
		MyBeanConfig two = registry.getBeanOfTypeFromModule( "ModuleTwo", MyBeanConfig.class );

		Collection<GenericBean<Long, List<Integer>>> integersOne = one.getIntegerLists();
		assertNotNull( integersOne );
		assertTrue( integersOne.getClass().equals( RefreshableRegistry.class ) );
		assertEquals( 2, integersOne.size() );

		Collection<GenericBean<String, List<Date>>> datesOne = one.getDateLists();
		assertNotNull( datesOne );
		assertTrue( datesOne.getClass().equals( IncrementalRefreshableRegistry.class ) );
		assertTrue( datesOne.isEmpty() );

		Collection<GenericBean<Long, List<Integer>>> integersTwo = two.getIntegerLists();
		assertNotNull( integersTwo );
		assertTrue( integersTwo.getClass().equals( RefreshableRegistry.class ) );
		assertEquals( 2, integersTwo.size() );

		Collection<GenericBean<String, List<Date>>> datesTwo = two.getDateLists();
		assertNotNull( datesTwo );
		assertTrue( datesTwo.getClass().equals( IncrementalRefreshableRegistry.class ) );
		assertTrue( datesTwo.isEmpty() );
	}

	@Test
	public void beansShouldBeReturnedInTheBootstrapOrderOfModules() {
		AcrossContext context = new AcrossContext();

		ModuleOne moduleOne = new ModuleOne();
		moduleOne.addRuntimeDependency( "ModuleThree" );
		context.addModule( moduleOne );

		ModuleTwo moduleTwo = new ModuleTwo();
		moduleTwo.addRuntimeDependency( "ModuleOne" );
		context.addModule( moduleTwo );

		ModuleThree moduleThree = new ModuleThree();
		context.addModule( moduleThree );

		context.bootstrap();

		AcrossContextBeanRegistry registry = AcrossContextUtils.getBeanRegistry( context );
		List<MyBeanConfig> beans = registry.getBeansOfType( MyBeanConfig.class, true );
		assertEquals( 3, beans.size() );

		assertEquals( "ModuleThree", beans.get( 0 ).getModule() );
		assertEquals( "ModuleOne", beans.get( 1 ).getModule() );
		assertEquals( "ModuleTwo", beans.get( 2 ).getModule() );

		context.shutdown();
	}

	@Test
	public void beansFromTheParentContextArePositionedBeforeTheModuleBeans() {
		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.getBeanFactory().registerSingleton( "", new MyFixedBeanConfig() );
		applicationContext.refresh();

		AcrossContext context = new AcrossContext( applicationContext );

		ModuleOne moduleOne = new ModuleOne();
		moduleOne.addRuntimeDependency( "ModuleTwo" );
		context.addModule( moduleOne );

		ModuleTwo moduleTwo = new ModuleTwo();
		moduleTwo.addRuntimeDependency( "ModuleThree" );
		context.addModule( moduleTwo );

		ModuleThree moduleThree = new ModuleThree();
		context.addModule( moduleThree );

		context.bootstrap();

		AcrossContextBeanRegistry registry = AcrossContextUtils.getBeanRegistry( context );
		List<MyBeanConfig> beans = registry.getBeansOfType( MyBeanConfig.class, true );
		assertEquals( 4, beans.size() );

		assertEquals( "ApplicationContext", beans.get( 0 ).getModule() );
		assertEquals( "ModuleThree", beans.get( 1 ).getModule() );
		assertEquals( "ModuleTwo", beans.get( 2 ).getModule() );
		assertEquals( "ModuleOne", beans.get( 3 ).getModule() );

		context.shutdown();
		applicationContext.destroy();
	}

	@Configuration
	static class MyBeanConfig
	{
		@Autowired
		@Module(AcrossModule.CURRENT_MODULE)
		private AcrossModule module;

		@Autowired
		@Module(AcrossModule.CURRENT_MODULE)
		private AcrossModuleInfo moduleInfo;

		@RefreshableCollection(includeModuleInternals = true)
		private Collection<GenericBean<Long, List<Integer>>> integerLists;

		@RefreshableCollection(incremental = true)
		private Collection<GenericBean<String, List<Date>>> dateLists;

		public String getModule() {
			assertSame( module, moduleInfo.getModule() );
			return module.getName();
		}

		public Collection<GenericBean<Long, List<Integer>>> getIntegerLists() {
			return integerLists;
		}

		public Collection<GenericBean<String, List<Date>>> getDateLists() {
			return dateLists;
		}

		@Bean
		public GenericBean<Long, List<Integer>> longWithIntegerList() {
			return new GenericBean<>( "longWithIntegerList" );
		}

		@Bean
		public GenericBean<String, List<Date>> stringWithDateList() {
			return new GenericBean<>( "stringWithDateList" );
		}
	}

	static class GenericBean<T, Y>
	{
		private final String name;

		GenericBean( String name ) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	static class MyFixedBeanConfig extends MyBeanConfig
	{
		@Override
		public String getModule() {
			return "ApplicationContext";
		}
	}

	static class ModuleOne extends AcrossModule
	{
		@Override
		public String getName() {
			return "ModuleOne";
		}

		@Override
		public String getDescription() {
			return null;
		}

		@Override
		protected void registerDefaultApplicationContextConfigurers( Set<ApplicationContextConfigurer> contextConfigurers ) {
			contextConfigurers.add( new AnnotatedClassConfigurer( MyBeanConfig.class ) );
		}
	}

	static class ModuleTwo extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleTwo";
		}
	}

	static class ModuleThree extends ModuleOne
	{
		@Override
		public String getName() {
			return "ModuleThree";
		}
	}
}
