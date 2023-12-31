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
package com.foreach.across.boot;

import com.foreach.across.boot.application.RootComponent;
import com.foreach.across.boot.postprocessor.config.SamplePostProcessorModule;
import com.foreach.across.config.AcrossApplication;
import com.foreach.across.core.DynamicAcrossModule;
import com.foreach.across.core.context.info.AcrossContextInfo;
import com.foreach.across.core.context.registry.AcrossContextBeanRegistry;
import lombok.Getter;
import org.assertj.core.data.Index;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.HierarchicalBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.context.event.EventListener;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.validation.SmartValidator;
import org.springframework.validation.Validator;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Arne Vandamme
 */
@ExtendWith(SpringExtension.class)
@DirtiesContext
@TestPropertySource(properties = "across.displayName=My Application")
@SpringBootTest(classes = { TestAcrossApplicationBootstrap.SampleApplication.class })
public class TestAcrossApplicationBootstrap
{
	@Autowired
	private ApplicationContext applicationContext;

	@Autowired
	private AcrossContextInfo contextInfo;

	@Autowired
	private AcrossContextBeanRegistry contextBeanRegistry;

	@Test
	public void displayNameShouldBeSetFromProperties() {
		assertEquals( "My Application", contextInfo.getDisplayName() );
	}

	@Test
	public void infrastructureModuleShouldBeAdded() {
		assertEquals( 2, contextInfo.getModuleIndex( "SampleInfrastructureModule" ) );
		assertEquals( "sampleInfrastructure", contextInfo.getModuleInfo( "SampleInfrastructureModule" )
		                                                 .getResourcesKey() );
		assertTrue( contextInfo.getModuleInfo( "SampleInfrastructureModule" )
		                       .getModule() instanceof DynamicAcrossModule );
	}

	@Test
	public void applicationModuleShouldBeAdded() {
		assertEquals( 3, contextInfo.getModuleIndex( "SampleApplicationModule" ) );
		assertEquals( "sample", contextInfo.getModuleInfo( "SampleApplicationModule" ).getResourcesKey() );
		assertTrue( contextInfo.getModuleInfo( "SampleApplicationModule" )
		                       .getModule() instanceof DynamicAcrossModule );
		assertNotNull( contextInfo.getModuleInfo( "SampleApplicationModule" ).getApplicationContext().getBean( RootComponent.class ) );
		assertFalse( contextInfo.getModuleInfo( "SampleApplicationModule" ).getApplicationContext().containsBean( "extensionComponent" ) );
		assertFalse( contextInfo.getModuleInfo( "SampleApplicationModule" ).getApplicationContext().containsBean( "installerComponent" ) );
	}

	@Test
	public void postProcessorModuleShouldBeAddedButScanned() {
		assertEquals( 4, contextInfo.getModuleIndex( "SamplePostProcessorModule" ) );
		assertEquals( "SamplePostProcessorModule", contextInfo.getModuleInfo( "SamplePostProcessorModule" )
		                                                      .getResourcesKey() );
		assertTrue( contextInfo.getModuleInfo( "SamplePostProcessorModule" )
		                       .getModule() instanceof SamplePostProcessorModule );
	}

	@Test
	public void singleValidatorShouldBeRegisteredAndInTheRootApplicationContext() {
		Validator validator = applicationContext.getBean( Validator.class );
		assertNotNull( validator );

		assertEquals( 1, contextBeanRegistry.getBeansOfType( Validator.class, true ).size() );
		assertSame( validator, contextBeanRegistry.getBeanOfType( Validator.class ) );

		assertTrue( validator instanceof SmartValidator );
	}

	@Test
	public void autoConfigurationPackageShouldNotBeRegisteredInNonModuleContext() {
		HierarchicalBeanFactory ctx = (HierarchicalBeanFactory) contextInfo.getApplicationContext().getAutowireCapableBeanFactory();

		do {
			if ( "ax:expose-parent".equals( ( (DefaultListableBeanFactory) ctx ).getSerializationId() ) ) {
				assertThat( AutoConfigurationPackages.has( ctx ) ).isFalse();
			}
			else {
				assertThat( AutoConfigurationPackages.get( ctx ) ).hasSize( 1 ).contains( "should.only.match.application.package", Index.atIndex( 0 ) );
			}
			ctx = (HierarchicalBeanFactory) ctx.getParentBeanFactory();
		}
		while ( ctx != null );
	}

	@Test
	public void springApplicationEventShouldBeReceivedInsideModulesAndParent() {
		RootComponent applicationComponent = contextInfo.getModuleInfo( "SampleApplicationModule" ).getApplicationContext().getBean( RootComponent.class );
		SampleApplication application = applicationContext.getBean( SampleApplication.class );

		assertThat( application.getEventsReceived() )
				.hasSize( 1 )
				.containsKey( ApplicationReadyEvent.class.getName() );

		assertThat( applicationComponent.getEventsReceived() )
				.hasSize( 1 )
				.containsKey( ApplicationReadyEvent.class.getName() );

		assertThat( application.getEventsReceived().get( ApplicationReadyEvent.class.getName() ) )
				.isGreaterThan( applicationComponent.getEventsReceived().get( ApplicationReadyEvent.class.getName() ) );
	}

	@AcrossApplication
	@Import(VerifyNoAutoConfigurationPackages.class)
	protected static class SampleApplication
	{
		@Getter
		private Map<String, Integer> eventsReceived = new LinkedHashMap<>();

		@EventListener
		public void handle( ApplicationReadyEvent applicationReadyEvent ) {
			eventsReceived.put( applicationReadyEvent.getClass().getName(), RootComponent.EVENT_COUNTER.getAndIncrement() );
		}
	}

	static class VerifyNoAutoConfigurationPackages implements ImportBeanDefinitionRegistrar
	{
		@Override
		public void registerBeanDefinitions( AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry ) {
			assertThat( AutoConfigurationPackages.get( (BeanFactory) registry ) ).hasSize( 1 ).contains( "should.only.match.application.package",
			                                                                                             Index.atIndex( 0 ) );
		}
	}
}
