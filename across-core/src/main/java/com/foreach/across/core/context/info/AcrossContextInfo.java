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

package com.foreach.across.core.context.info;

import com.foreach.across.core.AcrossContext;
import com.foreach.across.core.AcrossModule;
import com.foreach.across.core.context.AcrossEntity;
import com.foreach.across.core.context.ExposedBeanDefinition;
import com.foreach.across.core.context.bootstrap.AcrossBootstrapConfig;
import org.springframework.context.ApplicationContext;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Holds the intermediate and final state of a running AcrossContext.
 */
public interface AcrossContextInfo extends AcrossEntity
{
	String BEAN = "across.contextInfo";

	/**
	 * @return The unique id of the AcrossContext.
	 */
	String getId();

	/**
	 * @return The display name of the AcrossContext.
	 */
	String getDisplayName();

	/**
	 * @return The original AcrossContext.
	 */
	AcrossContext getContext();

	/**
	 * Gets the collection of all modules that were configured in the context,
	 * even if they were disabled.
	 *
	 * @return Collection of AcrossModuleInfo instances.
	 */
	Collection<AcrossModuleInfo> getConfiguredModules();

	/**
	 * Gets the collection of modules that have fully bootstrapped.
	 * A bootstrapped module has its own {@link ApplicationContext} available.
	 *
	 * @return collection of module info instances
	 */
	default Collection<AcrossModuleInfo> getBootstrappedModules() {
		return getModules().stream().filter( AcrossModuleInfo::isBootstrapped ).collect( Collectors.toList() );
	}

	/**
	 * Gets the collection of all modules that were slated to be bootstrapped.
	 * These will include modules still to bootstrap, fully bootstrapped modules
	 * as well as modules with a {@link ModuleBootstrapStatus#Skipped}.
	 *
	 * @return Collection of AcrossModuleInfo instances.
	 */
	Collection<AcrossModuleInfo> getModules();

	/**
	 * @param moduleName Unique name of the module.
	 * @return True if the module with that name is configured on the context.
	 */
	boolean hasModule( String moduleName );

	/**
	 * @param moduleName Unique name of the module.
	 * @return AcrossModuleInfo instance or null if not present.
	 */
	AcrossModuleInfo getModuleInfo( String moduleName );

	/**
	 * @return AcrossModuleInfo instance of null if bootstrap finished.
	 */
	AcrossModuleInfo getModuleBeingBootstrapped();

	/**
	 * @return True if the AcrossContext is bootstrapped, false if bootstrap in progress.
	 */
	boolean isBootstrapped();

	/**
	 * @return The Spring application context for this Across context.
	 */
	ApplicationContext getApplicationContext();

	/**
	 * @return Configuration object used for bootstrapping the AcrossContext.
	 */
	AcrossBootstrapConfig getBootstrapConfiguration();

	/**
	 * @param moduleName Unique name of the module.
	 * @return Index of the module in the context bootstrap, max integer if not found.
	 */
	int getModuleIndex( String moduleName );

	/**
	 * @param module AcrossModule instance to lookup the index for.
	 * @return Index of the module in the context bootstrap, max integer if not found.
	 */
	int getModuleIndex( AcrossModule module );

	/**
	 * @param moduleInfo AcrossModuleInfo instance to lookup the index for.
	 * @return Index of the module in the context bootstrap, max integer if not found.
	 */
	int getModuleIndex( AcrossModuleInfo moduleInfo );

	/**
	 * Exposed beans on an AcrossContext are beans pushed to the parent context.
	 *
	 * @return The collection of exposed BeanDefinitions.
	 */
	Map<String, ExposedBeanDefinition> getExposedBeanDefinitions();
}
