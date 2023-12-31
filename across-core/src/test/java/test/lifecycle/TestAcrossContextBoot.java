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

package test.lifecycle;

import com.foreach.across.core.AcrossContext;
import com.foreach.across.core.installers.InstallerAction;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import test.modules.TestContextEventListener;
import test.modules.TestEvent;
import test.modules.module1.*;
import test.modules.module2.ConstructedBeanModule2;
import test.modules.module2.ScannedBeanModule2;
import test.modules.module2.TestModule2;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { TestAcrossContextBoot.Config.class, PropertyPlaceholderAutoConfiguration.class })
@DirtiesContext
public class TestAcrossContextBoot
{
	@Autowired
	private AcrossContext context;

	@Autowired
	private TestModule1 module1;

	@Autowired
	private TestModule2 module2;

	@Autowired
	private ScannedBeanModule1 scannedBeanModule1;

	@Autowired
	private ScannedBeanModule2 scannedBeanModule2;

	@Autowired
	private ConstructedBeanModule1 constructedBeanModule1;

	@Autowired
	private ConstructedBeanModule2 constructedBeanModule2;

	@Autowired
	@Qualifier("refreshable")
	private ConstructedBeanModule1 refreshedBeanModule1;

	@Autowired
	private ScannedPrototypeBeanModule1 prototype1;

	@Autowired
	private ScannedPrototypeBeanModule1 prototype2;

	@Autowired
	private BeanWithOnlyPostRefresh beanWithOnlyPostRefresh;

	@Autowired
	@Qualifier("testListener")
	private TestContextEventListener testListener;

	@Value("${general.version}")
	private String version;

	@Test
	public void moduleContextLoadingOrder() {
		assertNotNull( module1 );
		assertNotNull( module2 );

		assertEquals( "versionForAcross", version );
		assertEquals( "versionForModule1", module1.getVersion() );
		assertEquals( "versionForModule2", module2.getVersion() );

		// Beans should only be constructed & (post-)constructed - the counter is incremented for construct & postconstruct
		assertEquals( 2, ScannedBeanModule1.CONSTRUCTION_COUNTER.get() );
		assertEquals( 2, ScannedBeanModule2.CONSTRUCTION_COUNTER.get() );

		// Prototypes should be different but should have module set
		assertNotNull( prototype1 );
		assertNotNull( prototype2 );
		assertNotSame( prototype1, prototype2 );
		assertSame( scannedBeanModule1, prototype1.getScannedBeanModule1() );
		assertSame( scannedBeanModule2, prototype1.getScannedBeanModule2() );
		assertSame( constructedBeanModule2, prototype1.getConstructedBeanModule2() );
		assertSame( scannedBeanModule1, prototype2.getScannedBeanModule1() );

		// As module 1 is initialized before module 2, the bean references from inside module 2 should not be set
		assertNotNull( scannedBeanModule1 );
		assertEquals( "valueForModule1", scannedBeanModule1.getBeanValue() );
		assertTrue( scannedBeanModule1.isPostConstructed() );
		assertSame( module2, scannedBeanModule1.getReferenceToModule2() );
		assertNull( scannedBeanModule1.getReferenceToBeanFromModule2() );

		assertNotNull( constructedBeanModule1 );
		// Only post refresh has been called on the non-refreshable bean
		assertEquals( "i have been refreshed", constructedBeanModule1.getText() );
		assertEquals( "i have also been refreshed", constructedBeanModule1.getOtherText() );
		assertSame( scannedBeanModule1, constructedBeanModule1.getScannedBeanModule1() );
		assertEquals( 1, constructedBeanModule1.getSomeInterfaces().size() );
		assertNull( constructedBeanModule1.getScannedBeanModule2() );

		// The refreshable constructed bean in module 1 does hold all references
		assertNotNull( refreshedBeanModule1 );
		assertEquals( "i have been refreshed", refreshedBeanModule1.getText() );
		assertEquals( "i have also been refreshed", refreshedBeanModule1.getOtherText() );
		assertSame( scannedBeanModule1, refreshedBeanModule1.getScannedBeanModule1() );
		assertSame( scannedBeanModule2, refreshedBeanModule1.getScannedBeanModule2() );
		assertEquals( 2, refreshedBeanModule1.getSomeInterfaces().size() );

		// Module 2 should have the references to beans from module 1
		assertNotNull( scannedBeanModule2 );
		assertSame( module1, scannedBeanModule2.getReferenceToModule1() );
		assertSame( scannedBeanModule1, scannedBeanModule2.getReferenceToBeanFromModule1() );

		assertNotNull( constructedBeanModule2 );
		assertEquals( "helloFromModule2", constructedBeanModule2.getText() );
		assertSame( scannedBeanModule1, constructedBeanModule1.getScannedBeanModule1() );
		assertSame( constructedBeanModule1, constructedBeanModule2.getConstructedBeanModule1() );
	}

	@Test
	public void allAcrossEventHandlersShouldReceiveTheEvents() {
		assertNotNull( scannedBeanModule1 );
		assertNotNull( scannedBeanModule2 );
		assertNotNull( testListener );
		assertNotSame( scannedBeanModule1, testListener );
		assertNotSame( scannedBeanModule2, testListener );

		assertTrue( scannedBeanModule1.getEventsReceived().isEmpty() );
		assertTrue( scannedBeanModule2.getEventsReceived().isEmpty() );
		assertTrue( constructedBeanModule1.getEventsReceived().isEmpty() );
		assertTrue( constructedBeanModule2.getEventsReceived().isEmpty() );
		assertTrue( testListener.getEventsReceived().isEmpty() );

		TestEvent testEvent = new TestEvent();
		context.publishEvent( testEvent );

		assertEquals( 1, scannedBeanModule1.getEventsReceived().size() );
		assertEquals( 1, scannedBeanModule2.getEventsReceived().size() );
		assertEquals( 1, constructedBeanModule1.getEventsReceived().size() );
		assertEquals( 1, constructedBeanModule2.getEventsReceived().size() );
		assertEquals( 1, testListener.getEventsReceived().size() );

		assertSame( testEvent, scannedBeanModule1.getEventsReceived().get( 0 ) );
		assertSame( testEvent, scannedBeanModule2.getEventsReceived().get( 0 ) );
		assertSame( testEvent, constructedBeanModule1.getEventsReceived().get( 0 ) );
		assertSame( testEvent, constructedBeanModule2.getEventsReceived().get( 0 ) );
		assertSame( testEvent, testListener.getEventsReceived().get( 0 ) );
	}

	@Test
	public void defaultModuleShouldAlwaysReferenceCurrentAsPrimary() {
		assertSame( module1, scannedBeanModule1.getDefaultModule() );
		assertSame( module2, scannedBeanModule2.getDefaultModule() );
	}

	@Test
	public void currentModuleShouldAlwaysReferenceRightOne() {
		assertSame( module1, scannedBeanModule1.getCurrentModule() );
		assertSame( module2, scannedBeanModule2.getCurrentModule() );
	}

	@Test
	public void currentModuleCanAlsoBeUsedDirectlyInExposedScopedBeans() {
		assertNotSame( prototype1, prototype2 );
		assertSame( module1, prototype1.getCurrentModule() );
		assertSame( module1, prototype2.getCurrentModule() );
	}

	@Test
	public void postRefreshShouldBeCalledOnBeansWithoutRefreshable() {
		assertTrue( beanWithOnlyPostRefresh.isRefreshed() );
		assertSame( scannedBeanModule2, beanWithOnlyPostRefresh.getScannedBeanFromLaterModule() );
	}

	@Test
	public void postRefreshShouldNotBeCalledIfDependenciesNotFound() {
		assertFalse( beanWithOnlyPostRefresh.isUnknownBeanSet() );
	}

	@Configuration
	@PropertySource("classpath:test/TestAcrossContextBoot.properties")
	public static class Config
	{
		@Bean
		public TestContextEventListener testListener() {
			return new TestContextEventListener();
		}

		@Bean
		public DataSource acrossDataSource() {
			return DataSourceBuilder.create().driverClassName( "org.hsqldb.jdbc.JDBCDriver" ).type( HikariDataSource.class )
			                        .url( "jdbc:hsqldb:mem:acrossTest" ).username( "sa" ).build();
		}

		@Bean
		@Autowired
		public AcrossContext acrossContext( ConfigurableApplicationContext applicationContext ) {
			ScannedBeanModule1.CONSTRUCTION_COUNTER.set( 0 );
			ScannedBeanModule2.CONSTRUCTION_COUNTER.set( 0 );

			AcrossContext context = new AcrossContext( applicationContext );
			context.setDataSource( acrossDataSource() );
			context.setInstallerAction( InstallerAction.EXECUTE );

			context.addModule( testModule1() );
			context.addModule( testModule2() );

			context.bootstrap();

			return context;
		}

		@Bean
		public TestModule1 testModule1() {
			return new TestModule1();
		}

		@Bean
		public TestModule2 testModule2() {
			return new TestModule2();
		}
	}
}
