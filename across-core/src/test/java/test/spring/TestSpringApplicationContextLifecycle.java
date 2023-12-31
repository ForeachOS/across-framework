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

package test.spring;

import com.foreach.across.core.AcrossContext;
import com.foreach.across.core.AcrossModule;
import com.foreach.across.core.EmptyAcrossModule;
import com.foreach.across.core.context.AcrossConfigurableApplicationContext;
import com.foreach.across.core.context.AcrossContextUtils;
import com.foreach.across.core.context.configurer.AnnotatedClassConfigurer;
import com.foreach.across.core.context.info.AcrossContextInfo;
import com.foreach.across.core.installers.InstallerAction;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class TestSpringApplicationContextLifecycle
{
	@Test
	public void destroySpringContextWithoutChild() {
		AnnotationConfigApplicationContext parent = new AnnotationConfigApplicationContext( Config.class );
		ConfigurableListableBeanFactory beanFactory = parent.getBeanFactory();

		assertNotNull( beanFactory.getSingleton( "myBean" ) );

		parent.close();
		assertNull( beanFactory.getSingleton( "myBean" ) );
	}

	@Test
	public void destroySpringContextWithChild() {
		AnnotationConfigApplicationContext parent = new AnnotationConfigApplicationContext( Config.class );
		ConfigurableListableBeanFactory parentFactory = parent.getBeanFactory();

		AnnotationConfigApplicationContext child = new AnnotationConfigApplicationContext( Config.class );
		child.setParent( parent );
		ConfigurableListableBeanFactory childFactory = child.getBeanFactory();

		assertNotNull( parentFactory.getSingleton( "myBean" ) );
		assertNotNull( childFactory.getSingleton( "myBean" ) );
		assertNotSame( parentFactory.getSingleton( "myBean" ), childFactory.getSingleton( "myBean" ) );

		parent.close();
		assertNull( parentFactory.getSingleton( "myBean" ) );
		assertNotNull( childFactory.getSingleton( "myBean" ) );
	}

	@Test
	public void destroySpringChildContext() {
		AnnotationConfigApplicationContext parent = new AnnotationConfigApplicationContext( Config.class );
		ConfigurableListableBeanFactory parentFactory = parent.getBeanFactory();

		AnnotationConfigApplicationContext child = new AnnotationConfigApplicationContext( Config.class );
		child.setParent( parent );
		ConfigurableListableBeanFactory childFactory = child.getBeanFactory();

		assertNotNull( parentFactory.getSingleton( "myBean" ) );
		assertNotNull( childFactory.getSingleton( "myBean" ) );
		assertNotSame( parentFactory.getSingleton( "myBean" ), childFactory.getSingleton( "myBean" ) );

		child.close();
		assertNotNull( parentFactory.getSingleton( "myBean" ) );
		assertNull( childFactory.getSingleton( "myBean" ) );
	}

	@Test
	public void destroyAcrossContextDirectly() {
		AcrossContext across = new AcrossContext();
		across.setInstallerAction( InstallerAction.DISABLED );
		across.setDataSource( mock( DataSource.class ) );

		AcrossModule moduleOne = new EmptyAcrossModule( "moduleOne" );
		moduleOne.addApplicationContextConfigurer( new AnnotatedClassConfigurer( Config.class ) );

		AcrossModule moduleTwo = new EmptyAcrossModule( "moduleTwo" );
		moduleTwo.addApplicationContextConfigurer( new AnnotatedClassConfigurer( Config.class ) );

		across.addModule( moduleOne );
		across.addModule( moduleTwo );

		across.bootstrap();

		AcrossConfigurableApplicationContext acrossApplicationContext = AcrossContextUtils.getApplicationContext( across );
		ConfigurableListableBeanFactory acrossFactory = AcrossContextUtils.getBeanFactory( across );
		ConfigurableListableBeanFactory moduleOneFactory = AcrossContextUtils.getBeanFactory( moduleOne );
		ConfigurableListableBeanFactory moduleTwoFactory = AcrossContextUtils.getBeanFactory( moduleTwo );

		assertTrue( acrossApplicationContext.isActive() );
		assertNotNull( acrossFactory.getSingleton( AcrossContextInfo.BEAN ) );
		assertNotNull( moduleOneFactory.getSingleton( "myBean" ) );
		assertNotNull( moduleTwoFactory.getSingleton( "myBean" ) );
		assertNotSame( moduleOneFactory.getSingleton( "myBean" ), moduleTwoFactory.getSingleton( "myBean" ) );

		across.shutdown();

		assertFalse( acrossApplicationContext.isActive() );
		assertNull( acrossFactory.getSingleton( AcrossContextInfo.BEAN ) );
		assertNull( moduleOneFactory.getSingleton( "myBean" ) );
		assertNull( moduleTwoFactory.getSingleton( "myBean" ) );
	}

	@Test
	public void destroyingParentContextHasNoEffectIfAcrossContextBeanNotPresent() {
		AnnotationConfigApplicationContext parent = new AnnotationConfigApplicationContext( Config.class );

		AcrossContext across = new AcrossContext( parent );
		across.setInstallerAction( InstallerAction.DISABLED );
		across.setDataSource( mock( DataSource.class ) );

		AcrossModule moduleOne = new EmptyAcrossModule( "moduleOne" );
		moduleOne.addApplicationContextConfigurer( new AnnotatedClassConfigurer( Config.class ) );

		AcrossModule moduleTwo = new EmptyAcrossModule( "moduleTwo" );
		moduleTwo.addApplicationContextConfigurer( new AnnotatedClassConfigurer( Config.class ) );

		across.addModule( moduleOne );
		across.addModule( moduleTwo );

		across.bootstrap();

		AcrossConfigurableApplicationContext acrossApplicationContext = AcrossContextUtils.getApplicationContext(
				across );
		ConfigurableListableBeanFactory acrossFactory = AcrossContextUtils.getBeanFactory( across );
		ConfigurableListableBeanFactory moduleOneFactory = AcrossContextUtils.getBeanFactory( moduleOne );
		ConfigurableListableBeanFactory moduleTwoFactory = AcrossContextUtils.getBeanFactory( moduleTwo );

		assertTrue( acrossApplicationContext.isActive() );
		assertEquals( parent, acrossApplicationContext.getParent() );
		assertNotNull( acrossFactory.getSingleton( AcrossContextInfo.BEAN ) );
		assertNotNull( moduleOneFactory.getSingleton( "myBean" ) );
		assertNotNull( moduleTwoFactory.getSingleton( "myBean" ) );
		assertNotSame( moduleOneFactory.getSingleton( "myBean" ), moduleTwoFactory.getSingleton( "myBean" ) );

		parent.close();

		assertTrue( acrossApplicationContext.isActive() );
		assertEquals( parent, acrossApplicationContext.getParent() );
		assertNotNull( acrossFactory.getSingleton( AcrossContextInfo.BEAN ) );
		assertNotNull( moduleOneFactory.getSingleton( "myBean" ) );
		assertNotNull( moduleTwoFactory.getSingleton( "myBean" ) );
		assertNotSame( moduleOneFactory.getSingleton( "myBean" ), moduleTwoFactory.getSingleton( "myBean" ) );
	}

	@Test
	public void destroyParentContextWithAcrossAsSingleton() {
		AnnotationConfigApplicationContext parent = new AnnotationConfigApplicationContext( Config.class );

		AcrossContext across = new AcrossContext( parent );
		across.setInstallerAction( InstallerAction.DISABLED );
		across.setDataSource( mock( DataSource.class ) );

		// AcrossContext configuration is bean in the parent and should be destroyed
		( (DefaultListableBeanFactory) parent.getBeanFactory() ).registerDisposableBean( "acrossContext", across );

		AcrossModule moduleOne = new EmptyAcrossModule( "moduleOne" );
		moduleOne.addApplicationContextConfigurer( new AnnotatedClassConfigurer( Config.class ) );

		AcrossModule moduleTwo = new EmptyAcrossModule( "moduleTwo" );
		moduleTwo.addApplicationContextConfigurer( new AnnotatedClassConfigurer( Config.class ) );

		across.addModule( moduleOne );
		across.addModule( moduleTwo );

		across.bootstrap();

		AcrossConfigurableApplicationContext acrossApplicationContext
				= AcrossContextUtils.getApplicationContext( across );
		ConfigurableListableBeanFactory acrossFactory = AcrossContextUtils.getBeanFactory( across );
		ConfigurableListableBeanFactory moduleOneFactory = AcrossContextUtils.getBeanFactory( moduleOne );
		ConfigurableListableBeanFactory moduleTwoFactory = AcrossContextUtils.getBeanFactory( moduleTwo );

		assertTrue( acrossApplicationContext.isActive() );
		assertEquals( parent, acrossApplicationContext.getParent() );
		assertNotNull( acrossFactory.getSingleton( AcrossContextInfo.BEAN ) );
		assertNotNull( moduleOneFactory.getSingleton( "myBean" ) );
		assertNotNull( moduleTwoFactory.getSingleton( "myBean" ) );
		assertNotSame( moduleOneFactory.getSingleton( "myBean" ), moduleTwoFactory.getSingleton( "myBean" ) );

		parent.close();

		assertFalse( acrossApplicationContext.isActive() );
		assertNull( acrossFactory.getSingleton( AcrossContextInfo.BEAN ) );
		assertNull( moduleOneFactory.getSingleton( "myBean" ) );
		assertNull( moduleTwoFactory.getSingleton( "myBean" ) );
	}

	@Configuration
	public static class Config
	{
		@Bean
		@SuppressWarnings("all")
		public Object myBean() {
			// create an actual separate instance (different reference)
			return new String( "bean" );
		}
	}
}
