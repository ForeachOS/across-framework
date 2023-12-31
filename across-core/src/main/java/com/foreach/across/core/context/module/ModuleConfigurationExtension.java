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
package com.foreach.across.core.context.module;

import lombok.*;

/**
 * Represents a single configuration class that should be added as an extension configuration to a module.
 * <p/>
 * The value of {@code deferred} indicates if the configuration should be added after the initial module
 * configuration ({@code true}) or before ({@code false}).
 * <p/>
 * The value op {@code optional} indicates if the presence of this configuration is sufficient to require
 * the separate module {@link org.springframework.context.ApplicationContext} to be started.
 * Optional configurations are often used to only activate infrastructure like caching or transaction management.
 *
 * @author Arne Vandamme
 * @see com.foreach.across.core.annotations.ModuleConfiguration
 * @since 5.0.0
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@ToString
@EqualsAndHashCode
public class ModuleConfigurationExtension
{
	private final String annotatedClass;
	private final boolean deferred;
	private final boolean optional;

	public static ModuleConfigurationExtension of( @NonNull Class<?> annotatedClass, boolean deferred, boolean optional ) {
		return of( annotatedClass.getName(), deferred, optional );
	}

	public static ModuleConfigurationExtension of( @NonNull String annotatedClass, boolean deferred, boolean optional ) {
		return new ModuleConfigurationExtension( annotatedClass, deferred, optional );
	}
}
