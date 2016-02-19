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
package com.foreach.across.test.events;

import com.foreach.across.config.EnableAcrossContext;
import com.foreach.across.core.AcrossModule;
import com.foreach.across.core.EmptyAcrossModule;
import com.foreach.across.core.annotations.AcrossEventHandler;
import com.foreach.across.core.annotations.Event;
import com.foreach.across.core.events.AcrossContextBootstrappedEvent;
import com.foreach.across.core.events.AcrossModuleBeforeBootstrapEvent;
import com.foreach.across.core.events.AcrossModuleBootstrappedEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Arne Vandamme
 */
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
@ContextConfiguration(classes = TestEventHandlersFromParent.Config.class)
public class TestEventHandlersFromParent
{
	@Autowired
	private Config config;

	@Test
	public void eventsShouldHaveBeenIntercepted() {
		assertEquals(
				Arrays.asList( "before:named", "after:named", "bootstrapped" ),
				config.eventsReceived
		);
	}

	@Configuration
	@EnableAcrossContext
	@AcrossEventHandler
	protected static class Config
	{
		public final List<String> eventsReceived = new ArrayList<>();

		@Event
		public void beforeModuleBootstrapEvent( AcrossModuleBeforeBootstrapEvent moduleBeforeBootstrapEvent ) {
			eventsReceived.add( "before:" + moduleBeforeBootstrapEvent.getModule().getName() );
		}

		@Event
		public void afterModuleBootstrapEvent( AcrossModuleBootstrappedEvent moduleBootstrappedEvent ) {
			eventsReceived.add( "after:" + moduleBootstrappedEvent.getModule().getName() );
		}

		@Event
		public void contextBootstrappedEvent( AcrossContextBootstrappedEvent contextBootstrappedEvent ) {
			eventsReceived.add( "bootstrapped" );
		}

		@Bean
		public AcrossModule namedModule() {
			return new EmptyAcrossModule( "named" );
		}
	}
}