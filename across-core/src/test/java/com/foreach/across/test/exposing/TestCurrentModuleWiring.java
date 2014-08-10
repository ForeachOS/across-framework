package com.foreach.across.test.exposing;

import com.foreach.across.core.AcrossContext;
import com.foreach.across.core.AcrossModule;
import com.foreach.across.core.annotations.Exposed;
import com.foreach.across.core.annotations.Module;
import com.foreach.across.core.context.AcrossContextUtils;
import com.foreach.across.core.context.info.AcrossContextInfo;
import com.foreach.across.core.context.info.AcrossModuleInfo;
import com.foreach.across.core.installers.InstallerAction;
import com.foreach.across.core.transformers.BeanPrefixingTransformer;
import com.foreach.across.test.AbstractInlineModule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestCurrentModuleWiring.Config.class)
@DirtiesContext
public class TestCurrentModuleWiring
{
	@Autowired
	private AcrossContext acrossContext;

	@Autowired(required = false)
	@Module("ModuleOne")
	private ModuleConfig.BeanWithCurrentModules beanFromOne;

	@Autowired(required = false)
	@Module("ModuleTwo")
	private ModuleConfig.BeanWithCurrentModules beanFromTwo;

	@Test
	public void verifyCurrentModuleWiredCorrectly() {
		AcrossContextInfo contextInfo = AcrossContextUtils.getContextInfo( acrossContext );
		assertNotNull( contextInfo );

		AcrossModuleInfo moduleOne = contextInfo.getModuleInfo( "ModuleOne" );
		assertNotNull( moduleOne );
		ModuleConfig.BeanWithCurrentModules beanWithCurrentModules = moduleOne.getBean( "beanWithCurrentModules" );
		assertNull( beanWithCurrentModules.getParent() );
		beanWithCurrentModules.assertCurrentModule( moduleOne.getModule() );

		AcrossModuleInfo moduleTwo = contextInfo.getModuleInfo( "ModuleTwo" );
		assertNotNull( moduleTwo );
		beanWithCurrentModules = moduleTwo.getBean( "beanWithCurrentModules" );
		beanWithCurrentModules.assertCurrentModule( moduleTwo.getModule() );

		assertNotNull( beanWithCurrentModules.getParent() );

		ModuleConfig.BeanWithCurrentModules parent = beanWithCurrentModules.getParent();
		parent.assertCurrentModule( moduleOne.getModule() );
	}

	@Test
	public void verifyBeansExposedToParentContext() {
		AcrossContextInfo contextInfo = AcrossContextUtils.getContextInfo( acrossContext );
		assertNotNull( contextInfo );

		AcrossModuleInfo moduleOne = contextInfo.getModuleInfo( "ModuleOne" );
		assertSame( beanFromOne, moduleOne.getBean( "beanWithCurrentModules" ) );

		AcrossModuleInfo moduleTwo = contextInfo.getModuleInfo( "ModuleTwo" );
		assertSame( beanFromTwo, moduleTwo.getBean( "beanWithCurrentModules" ) );
	}

	@Configuration
	protected static class Config
	{
		@Bean
		public AcrossContext acrossContext( ApplicationContext applicationContext ) {
			AcrossContext context = new AcrossContext( applicationContext );
			context.setInstallerAction( InstallerAction.DISABLED );

			context.addModule( new ModuleOne() );
			context.addModule( new ModuleTwo() );

			return context;
		}
	}

	protected static class ModuleOne extends AbstractInlineModule
	{
		public ModuleOne() {
			super( "ModuleOne", ModuleConfig.class );

			setExposeTransformer( new BeanPrefixingTransformer( "parent" ) );
		}
	}

	protected static class ModuleTwo extends AbstractInlineModule
	{
		public ModuleTwo() {
			super( "ModuleTwo", ModuleConfig.class );
		}
	}

	@Configuration
	protected static class ModuleConfig
	{
		@Bean
		@Exposed
		public BeanWithCurrentModules beanWithCurrentModules() {
			return new BeanWithCurrentModules();
		}

		static class BeanWithCurrentModules
		{
			@Autowired(required = false)
			private BeanWithCurrentModules parent;

			@Autowired
			private AcrossModule currentModuleWithoutQualifier;

			@Autowired
			@Qualifier(AcrossModule.CURRENT_MODULE)
			private AcrossModule currentModuleByGeneralQualifier;

			@Autowired
			@Module(AcrossModule.CURRENT_MODULE)
			private AcrossModule currentModuleByModuleQualifier;

			public BeanWithCurrentModules getParent() {
				return parent;
			}

			public void assertCurrentModule( AcrossModule expected ) {
				assertSame( expected, currentModuleWithoutQualifier );
				assertSame( expected, currentModuleByGeneralQualifier );
				assertSame( expected, currentModuleByModuleQualifier );
			}
		}
	}

}