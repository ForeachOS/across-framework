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
package test.installers.scan;

import com.foreach.across.core.AcrossModule;
import test.installers.examples.InstallerThree;
import test.installers.scan.installers.InstallerOne;
import test.installers.examples.InstallerThree;
import test.installers.scan.installers.InstallerOne;

/**
 * @author Arne Vandamme
 */
public class InstallerScanModule extends AcrossModule
{
	private boolean includeManualInstallers;

	public InstallerScanModule( boolean includeManualInstallers ) {
		this.includeManualInstallers = includeManualInstallers;
	}

	@Override
	public String getName() {
		return "InstallerScanModule";
	}

	@Override
	public String getDescription() {
		return "Has installers detected by scanning and optional one manual installer.";
	}

	@Override
	public Object[] getInstallers() {
		return includeManualInstallers ? new Object[] { InstallerOne.class, InstallerThree.class } : new Object[0];
	}
}
