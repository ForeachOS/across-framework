package com.foreach.across.test.installers;

import com.foreach.across.core.AcrossContext;
import com.foreach.across.core.AcrossModule;
import com.foreach.across.core.annotations.Installer;
import com.foreach.across.core.context.AcrossApplicationContext;
import com.foreach.across.core.context.bootstrap.AcrossBootstrapConfig;
import com.foreach.across.core.context.bootstrap.ModuleBootstrapConfig;
import com.foreach.across.core.installers.*;
import com.foreach.across.test.modules.installer.installers.AlwaysRunAfterModuleBootstrapInstaller;
import com.foreach.across.test.modules.installer.installers.AlwaysRunBeforeContextBootstrapInstaller;
import com.foreach.across.test.modules.installer.installers.TestInstaller;
import com.foreach.across.test.modules.installer.installers.VersionBasedInstaller;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class TestAcrossInstallerRegistry
{
	private AcrossInstallerRegistry registry;
	private AcrossBootstrapConfig contextConfig;
	private ModuleBootstrapConfig moduleConfig;
	private AcrossInstallerRepository installerRepository;
	private AcrossModule module;

	private InstallerSettings contextSettings;
	private ConfigurableListableBeanFactory beanFactory;

	@Before
	public void setup() {
		installerRepository = mock( AcrossInstallerRepository.class );

		beanFactory = mock( ConfigurableListableBeanFactory.class );
		when( beanFactory.getBean( AcrossInstallerRepository.class ) ).thenReturn( installerRepository );

		AcrossApplicationContext acrossApplicationContext = mock( AcrossApplicationContext.class );
		when( acrossApplicationContext.getBeanFactory() ).thenReturn( beanFactory );

		AcrossContext acrossContext = mock( AcrossContext.class );
		when( acrossContext.hasApplicationContext() ).thenReturn( true );
		when( acrossContext.getAcrossApplicationContext() ).thenReturn( acrossApplicationContext );

		module = mock( AcrossModule.class );
		contextConfig = mock( AcrossBootstrapConfig.class );

		moduleConfig = mock( ModuleBootstrapConfig.class );
		when( moduleConfig.getModule() ).thenReturn( module );

		contextSettings = mock( InstallerSettings.class );

		when( contextConfig.getContext() ).thenReturn( acrossContext );
		when( contextConfig.getModule( anyString() ) ).thenReturn( moduleConfig );
		when( contextConfig.getInstallerSettings() ).thenReturn( contextSettings );

		registry = new AcrossInstallerRegistry( contextConfig );

		TestInstaller.reset();
	}

	@Test
	public void moduleSettingsUsedIfContextSettingsNotDisabled() {
		installers( AlwaysRunBeforeContextBootstrapInstaller.class );

		InstallerSettings moduleSettings = mock( InstallerSettings.class );
		when( moduleConfig.getInstallerSettings() ).thenReturn( moduleSettings );

		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.EXECUTE );
		when( moduleSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.DISABLED );

		registry.runInstallersForModule( "", InstallerPhase.BeforeContextBootstrap );

		verify( contextSettings ).shouldRun( anyString(), any( AlwaysRunBeforeContextBootstrapInstaller.class ) );
		verify( moduleSettings ).shouldRun( anyString(), any( AlwaysRunBeforeContextBootstrapInstaller.class ) );

		// Nothing executed
		assertExecuted();
	}

	@Test
	public void moduleSettingsNotUsedIfContextSettingsDisabled() {
		installers( AlwaysRunBeforeContextBootstrapInstaller.class );

		InstallerSettings moduleSettings = mock( InstallerSettings.class );
		when( moduleConfig.getInstallerSettings() ).thenReturn( moduleSettings );

		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.DISABLED );
		when( moduleSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.FORCE );

		registry.runInstallersForModule( "", InstallerPhase.BeforeContextBootstrap );

		verify( contextSettings ).shouldRun( anyString(), any( AlwaysRunBeforeContextBootstrapInstaller.class ) );
		verify( moduleSettings, never() ).shouldRun( anyString(), any(
				AlwaysRunBeforeContextBootstrapInstaller.class ) );

		// Nothing executed
		assertExecuted();
	}

	@Test
	public void installerBeansAreNotWiredIfNotExecuted() {
		installers( AlwaysRunBeforeContextBootstrapInstaller.class );

		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.SKIP );

		registry.runInstallersForModule( "", InstallerPhase.BeforeContextBootstrap );

		verify( beanFactory, never() ).autowireBeanProperties( anyObject(), anyInt(), anyBoolean() );

		assertExecuted();
	}

	@Test
	public void installerBeansAreWiredInParentIfNoModuleContextAvailable() {
		installers( AlwaysRunBeforeContextBootstrapInstaller.class );

		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.EXECUTE );

		registry.runInstallersForModule( "", InstallerPhase.BeforeContextBootstrap );

		verify( beanFactory )
				.autowireBeanProperties(
						any( AlwaysRunBeforeContextBootstrapInstaller.class ),
						anyInt(),
						anyBoolean()
				);

		assertExecuted( AlwaysRunBeforeContextBootstrapInstaller.class );
	}

	@Test
	public void installerBeansAreWiredInModuleContextIfAvailable() {
		installers( AlwaysRunBeforeContextBootstrapInstaller.class );
		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.EXECUTE );

		ConfigurableListableBeanFactory moduleBeanFactory = mock( ConfigurableListableBeanFactory.class );

		AcrossApplicationContext moduleAcrossApplicationContext = mock( AcrossApplicationContext.class );
		when( moduleAcrossApplicationContext.getBeanFactory() ).thenReturn( moduleBeanFactory );

		when( module.hasApplicationContext() ).thenReturn( true );
		when( module.getAcrossApplicationContext() ).thenReturn( moduleAcrossApplicationContext );

		registry.runInstallersForModule( "", InstallerPhase.BeforeContextBootstrap );

		verify( beanFactory, never() ).autowireBeanProperties( anyObject(), anyInt(), anyBoolean() );
		verify( moduleBeanFactory )
				.autowireBeanProperties(
						any( AlwaysRunBeforeContextBootstrapInstaller.class ),
						anyInt(),
						anyBoolean()
				);

		assertExecuted( AlwaysRunBeforeContextBootstrapInstaller.class );
	}

	@Test
	public void alwaysRunInstallerShouldExecute() {
		installers(
				AlwaysRunBeforeContextBootstrapInstaller.class,
				AlwaysRunAfterModuleBootstrapInstaller.class
		);
		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.EXECUTE );

		registry.runInstallersForModule( "", InstallerPhase.BeforeContextBootstrap );

		verify( installerRepository, never() ).getInstalledVersion( module,
		                                                            AlwaysRunBeforeContextBootstrapInstaller.class );
		verify( installerRepository )
				.setInstalled(
						module,
						AlwaysRunBeforeContextBootstrapInstaller.class.getAnnotation( Installer.class ),
						AlwaysRunBeforeContextBootstrapInstaller.class
				);

		verify( installerRepository, never() )
				.setInstalled(
						eq( module ),
						any( Installer.class ),
						eq( AlwaysRunAfterModuleBootstrapInstaller.class )
				);

		assertExecuted(
				AlwaysRunBeforeContextBootstrapInstaller.class
		);
	}

	@Test
	public void registerActionShouldLogButNotExecuteInstallerMethod() {
		installers(
				AlwaysRunAfterModuleBootstrapInstaller.class
		);
		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.REGISTER );

		registry.runInstallersForModule( "", InstallerPhase.AfterModuleBootstrap );

		verify( installerRepository )
				.setInstalled(
						module,
						AlwaysRunAfterModuleBootstrapInstaller.class.getAnnotation( Installer.class ),
						AlwaysRunAfterModuleBootstrapInstaller.class
				);

		assertExecuted();
	}

	@Test
	public void versionBasedShouldNotExecuteIfInstalledVersionEqual() {
		installers(
				VersionBasedInstaller.class
		);
		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.EXECUTE );

		when( installerRepository.getInstalledVersion( module, VersionBasedInstaller.class ) ).thenReturn(
				VersionBasedInstaller.VERSION );

		registry.runInstallersForModule( "", InstallerPhase.BeforeContextBootstrap );

		verify( installerRepository, never() )
				.setInstalled(
						module,
						VersionBasedInstaller.class.getAnnotation( Installer.class ),
						VersionBasedInstaller.class
				);

		assertExecuted();
	}

	@Test
	public void versionBasedShouldNotExecuteIfInstalledVersionHigher() {
		installers( VersionBasedInstaller.class );
		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.EXECUTE );

		when( installerRepository.getInstalledVersion( module, VersionBasedInstaller.class ) ).thenReturn(
				VersionBasedInstaller.VERSION + 1 );

		registry.runInstallersForModule( "", InstallerPhase.BeforeContextBootstrap );

		verify( installerRepository, never() )
				.setInstalled(
						module,
						VersionBasedInstaller.class.getAnnotation( Installer.class ),
						VersionBasedInstaller.class
				);

		assertExecuted();
	}

	@Test
	public void versionBasedShouldExecuteIfInstalledVersionLower() {
		installers( VersionBasedInstaller.class );
		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.EXECUTE );

		when( installerRepository.getInstalledVersion( module, VersionBasedInstaller.class ) ).thenReturn(
				VersionBasedInstaller.VERSION - 1 );

		registry.runInstallersForModule( "", InstallerPhase.BeforeContextBootstrap );

		verify( installerRepository )
				.setInstalled(
						module,
						VersionBasedInstaller.class.getAnnotation( Installer.class ),
						VersionBasedInstaller.class
				);

		assertExecuted( VersionBasedInstaller.class );
	}

	@Test
	public void forceActionShouldExecuteEvenIfNoVersionMatch() {
		installers( VersionBasedInstaller.class );
		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.FORCE );

		when( installerRepository.getInstalledVersion( module, VersionBasedInstaller.class ) ).thenReturn(
				VersionBasedInstaller.VERSION );

		registry.runInstallersForModule( "", InstallerPhase.BeforeContextBootstrap );

		verify( installerRepository )
				.setInstalled(
						module,
						VersionBasedInstaller.class.getAnnotation( Installer.class ),
						VersionBasedInstaller.class
				);

		assertExecuted( VersionBasedInstaller.class );
	}

	@Test
	public void skipActionShouldNotExecuteInstaller() {
		installers( AlwaysRunBeforeContextBootstrapInstaller.class );
		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.SKIP );

		registry.runInstallersForModule( "", InstallerPhase.BeforeContextBootstrap );

		verify( installerRepository, never() )
				.setInstalled(
						module,
						AlwaysRunBeforeContextBootstrapInstaller.class.getAnnotation( Installer.class ),
						AlwaysRunBeforeContextBootstrapInstaller.class
				);

		assertExecuted();
	}

	@Test
	public void disableActionShouldNotExecuteInstaller() {
		installers( AlwaysRunBeforeContextBootstrapInstaller.class );
		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.DISABLED );

		registry.runInstallersForModule( "", InstallerPhase.BeforeContextBootstrap );

		verify( installerRepository, never() )
				.setInstalled(
						module,
						AlwaysRunBeforeContextBootstrapInstaller.class.getAnnotation( Installer.class ),
						AlwaysRunBeforeContextBootstrapInstaller.class
				);

		assertExecuted();
	}

	@Test
	public void installerGroupShouldBePassedToSettings() {
		installers( VersionBasedInstaller.class );
		when( contextSettings.shouldRun( anyString(), anyObject() ) ).thenReturn( InstallerAction.DISABLED );

		registry.runInstallersForModule( "", InstallerPhase.BeforeContextBootstrap );

		verify( contextSettings ).shouldRun( eq( VersionBasedInstaller.GROUP ), any( VersionBasedInstaller.class ) );
	}

	@SuppressWarnings("unchecked")
	private void installers( Class... installerClass ) {
		Collection collection = Arrays.asList( installerClass );
		when( moduleConfig.getInstallers() ).thenReturn( collection );
	}

	private void assertExecuted( Class... installerClasses ) {
		assertArrayEquals( installerClasses, TestInstaller.executed() );

		TestInstaller.reset();
	}
}
