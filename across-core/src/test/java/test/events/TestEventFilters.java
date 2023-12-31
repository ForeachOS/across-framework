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

package test.events;

import com.foreach.across.core.AcrossContext;
import com.foreach.across.core.installers.InstallerAction;
import com.zaxxer.hikari.HikariDataSource;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ResolvableType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import test.modules.EventPubSub;
import test.modules.module1.ReplyEvent;
import test.modules.module1.TestModule1;
import test.modules.module2.*;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = TestEventFilters.Config.class)
@DirtiesContext
public class TestEventFilters
{
	@Autowired
	private AcrossContext context;

	@Autowired
	private CustomEventHandlers eventHandlers;

	@Autowired
	private EventPubSub publisherModuleOne;

	@Autowired
	private EventPubSub publisherModuleTwo;

	@Test
	public void eventsAreReceivedByAllModules() {
		val event = publisherModuleOne.publish( "event1" );
		assertEquals( Arrays.asList( "moduleOne", "moduleTwo" ), event.getReceivedBy() );

		val event2 = publisherModuleTwo.publish( "event2" );
		assertEquals( Arrays.asList( "moduleOne", "moduleTwo" ), event2.getReceivedBy() );
	}

	@Test
	public void replyIsAlsoReceivedByAllModules() {
		val original = new ReplyEvent();
		assertNull( original.getByName() );

		context.publishEvent( original );

		assertNotNull( original.getByName() );
		Assertions.assertEquals( Arrays.asList( "moduleOne", "moduleTwo" ), original.getByName().getReceivedBy() );
	}

	@Test
	public void simpleEventIsNotReceivedByNamedHandlers() {
		SimpleEvent event = new SimpleEvent();

		context.publishEvent( event );

		assertTrue( eventHandlers.getReceivedAll().contains( event ) );
		assertFalse( eventHandlers.getReceivedOne().contains( event ) );
		assertFalse( eventHandlers.getReceivedTwo().contains( event ) );
	}

	@Test
	public void specificNamedEventIsReceivedByMatchingHandlers() {
		NamedEvent event = new NamedEvent( "one" );

		context.publishEvent( event );

		assertTrue( eventHandlers.getReceivedAll().contains( event ) );
		assertTrue( eventHandlers.getReceivedOne().contains( event ) );
		assertFalse( eventHandlers.getReceivedTwo().contains( event ) );

		event = new NamedEvent( "two" );

		context.publishEvent( event );

		assertTrue( eventHandlers.getReceivedAll().contains( event ) );
		assertFalse( eventHandlers.getReceivedOne().contains( event ) );
		assertTrue( eventHandlers.getReceivedTwo().contains( event ) );

		event = new NamedEvent( "three" );

		context.publishEvent( event );

		assertTrue( eventHandlers.getReceivedAll().contains( event ) );
		assertTrue( eventHandlers.getReceivedOne().contains( event ) );
		assertTrue( eventHandlers.getReceivedTwo().contains( event ) );
	}

	@Test
	public void unknownNamedEventIsOnlyReceivedByAllHandler() {
		NamedEvent event = new NamedEvent( "nomatch" );

		context.publishEvent( event );

		assertTrue( eventHandlers.getReceivedAll().contains( event ) );
		assertFalse( eventHandlers.getReceivedOne().contains( event ) );
		assertFalse( eventHandlers.getReceivedTwo().contains( event ) );
	}

	@Test
	public void singleGenericParameter() {
		SingleGenericEvent<Integer> integer = new SingleGenericEvent<>( Integer.class );
		context.publishEvent( integer );
		assertTrue( eventHandlers.getReceivedSingleInteger().contains( integer ) );
		assertFalse( eventHandlers.getReceivedSingleDecimal().contains( integer ) );
		assertTrue( eventHandlers.getReceivedSingleNumber().contains( integer ) );
		assertFalse( eventHandlers.getReceivedStrongTypedGenericEvent().contains( integer ) );

		SingleGenericEvent<String> string = new SingleGenericEvent<>( String.class );
		context.publishEvent( string );
		assertFalse( eventHandlers.getReceivedSingleInteger().contains( string ) );
		assertFalse( eventHandlers.getReceivedSingleDecimal().contains( string ) );
		assertFalse( eventHandlers.getReceivedSingleNumber().contains( string ) );

		SingleGenericEvent<BigDecimal> decimal = new SingleGenericEvent<>( BigDecimal.class );
		context.publishEvent( decimal );
		assertFalse( eventHandlers.getReceivedSingleInteger().contains( decimal ) );
		assertTrue( eventHandlers.getReceivedSingleDecimal().contains( decimal ) );
		assertTrue( eventHandlers.getReceivedSingleNumber().contains( decimal ) );

		SingleGenericEvent<Long> longNumber = new SingleGenericEvent<>( Long.class );
		context.publishEvent( longNumber );
		assertFalse( eventHandlers.getReceivedSingleInteger().contains( longNumber ) );
		assertFalse( eventHandlers.getReceivedSingleDecimal().contains( longNumber ) );
		assertTrue( eventHandlers.getReceivedSingleNumber().contains( longNumber ) );
	}

	@Test
	public void strongTypedEventThasAlsoMatchesGeneric() {
		IntegerEvent integer = new IntegerEvent();
		context.publishEvent( integer );
		assertTrue( eventHandlers.getReceivedSingleInteger().contains( integer ) );
		assertFalse( eventHandlers.getReceivedSingleDecimal().contains( integer ) );
		assertTrue( eventHandlers.getReceivedSingleNumber().contains( integer ) );
		assertTrue( eventHandlers.getReceivedStrongTypedGenericEvent().contains( integer ) );
	}

	@SuppressWarnings("unchecked")
	@Test
	public void specificTypedEventsAreReceivedByAllMatchingHandlers() {
		GenericEvent<Long, HashMap> longMap = new GenericEvent<>( Long.class, HashMap.class );
		GenericEvent<Integer, List<Integer>> integerList = new GenericEvent<>(
				Integer.class,
				ResolvableType.forClassWithGenerics( ArrayList.class, Integer.class )
		);
		GenericEvent<Integer, List<Long>> longList = new GenericEvent<>(
				Integer.class,
				ResolvableType.forClassWithGenerics( ArrayList.class, Long.class )
		);
		GenericEvent<BigDecimal, Set> decimalSet = new GenericEvent<>( BigDecimal.class, Set.class );

		context.publishEvent( longMap );
		context.publishEvent( integerList );
		context.publishEvent( longList );
		context.publishEvent( decimalSet );

		assertTrue( eventHandlers.getReceivedAll().contains( longMap ) );
		assertFalse( eventHandlers.getReceivedOne().contains( longMap ) );
		assertFalse( eventHandlers.getReceivedTwo().contains( longMap ) );
		assertTrue( eventHandlers.getReceivedTypedLongMap().contains( longMap ) );
		assertFalse( eventHandlers.getReceivedTypedIntegerList().contains( longMap ) );
		assertFalse( eventHandlers.getReceivedTypedNumberCollection().contains( longMap ) );

		assertTrue( eventHandlers.getReceivedAll().contains( integerList ) );
		assertFalse( eventHandlers.getReceivedOne().contains( integerList ) );
		assertFalse( eventHandlers.getReceivedTwo().contains( integerList ) );
		assertFalse( eventHandlers.getReceivedTypedLongMap().contains( integerList ) );
		assertTrue( eventHandlers.getReceivedTypedIntegerList().contains( integerList ) );
		assertTrue( eventHandlers.getReceivedTypedNumberCollection().contains( integerList ) );

		assertTrue( eventHandlers.getReceivedAll().contains( longList ) );
		assertFalse( eventHandlers.getReceivedOne().contains( longList ) );
		assertFalse( eventHandlers.getReceivedTwo().contains( longList ) );
		assertFalse( eventHandlers.getReceivedTypedLongMap().contains( longList ) );
		assertFalse( eventHandlers.getReceivedTypedIntegerList().contains( longList ) );
		assertTrue( eventHandlers.getReceivedTypedNumberCollection().contains( longList ) );

		assertTrue( eventHandlers.getReceivedAll().contains( decimalSet ) );
		assertFalse( eventHandlers.getReceivedOne().contains( decimalSet ) );
		assertFalse( eventHandlers.getReceivedTwo().contains( decimalSet ) );
		assertFalse( eventHandlers.getReceivedTypedLongMap().contains( decimalSet ) );
		assertFalse( eventHandlers.getReceivedTypedIntegerList().contains( decimalSet ) );
		assertTrue( eventHandlers.getReceivedTypedNumberCollection().contains( decimalSet ) );
	}

	@Test
	public void listenerWithoutArguments() {
		eventHandlers.reset();

		context.publishEvent( new GenericEvent<>(
				Integer.class,
				ResolvableType.forClassWithGenerics( ArrayList.class, Integer.class )
		) );

		context.publishEvent( new SingleGenericEvent<>( Long.class ) );

		assertEquals( 2, eventHandlers.getGenericsReceivedCounter() );
	}

	@Configuration
	public static class Config
	{
		@Bean
		public DataSource acrossDataSource() {
			return DataSourceBuilder.create().driverClassName( "org.hsqldb.jdbc.JDBCDriver" ).type( HikariDataSource.class )
			                        .url( "jdbc:hsqldb:mem:acrossTest" ).username( "sa" ).build();
		}

		@Bean
		@Autowired
		public AcrossContext acrossContext( ConfigurableApplicationContext applicationContext ) {
			AcrossContext context = new AcrossContext( applicationContext );
			context.setDataSource( acrossDataSource() );
			context.setInstallerAction( InstallerAction.DISABLED );

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

