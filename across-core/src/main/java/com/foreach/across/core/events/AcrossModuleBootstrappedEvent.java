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

package com.foreach.across.core.events;

import com.foreach.across.core.context.info.AcrossContextInfo;
import com.foreach.across.core.context.info.AcrossModuleInfo;

/**
 * Event published when a module has bootstrapped.
 * That means the module itself has started, but installers registered to {@link com.foreach.across.core.installers.InstallerPhase#AfterModuleBootstrap}
 * have not yet been executed.
 * <p/>
 * Source of this event is the {@link AcrossContextInfo} where the module was created.
 */
public final class AcrossModuleBootstrappedEvent extends AcrossLifecycleEvent
{
	private final AcrossModuleInfo module;

	public AcrossModuleBootstrappedEvent( AcrossModuleInfo module ) {
		super( module.getContextInfo() );
		this.module = module;
	}

	public AcrossContextInfo getContext() {
		return module.getContextInfo();
	}

	public AcrossModuleInfo getModule() {
		return module;
	}

	/**
	 * @return the Across context where the module has bootstrapped
	 */
	@Override
	public AcrossContextInfo getSource() {
		return (AcrossContextInfo) super.getSource();
	}

	@Override
	public String toString() {
		return "AcrossModuleBootstrappedEvent{" +
				"context=" + getContext().getDisplayName() +
				", module=" + module.getName() +
				'}';
	}
}
