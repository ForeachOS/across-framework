package com.foreach.across.core.context.registry;

import com.foreach.across.core.context.info.ConfigurableAcrossContextInfo;

public class DefaultAcrossContextBeanRegistry implements AcrossContextBeanRegistry
{
	private final ConfigurableAcrossContextInfo contextInfo;

	public DefaultAcrossContextBeanRegistry( ConfigurableAcrossContextInfo contextInfo ) {
		this.contextInfo = contextInfo;
	}

	@Override
	public String getContextId() {
		return contextInfo.getId();
	}

	@Override
	public String getFactoryName() {
		return contextInfo.getId() + "@" + AcrossContextBeanRegistry.BEAN;
	}

	@Override
	public Object getBeanFromModule( String moduleName, String beanName ) {
		return contextInfo.getConfigurableModuleInfo( moduleName ).getApplicationContext().getBean( beanName );
	}

	@Override
	public Class<?> getBeanTypeFromModule( String moduleName, String beanName ) {
		return contextInfo.getConfigurableModuleInfo( moduleName ).getApplicationContext().getType( beanName );
	}
}
