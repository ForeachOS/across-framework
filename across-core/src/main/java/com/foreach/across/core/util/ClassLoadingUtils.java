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
package com.foreach.across.core.util;

import org.springframework.util.ClassUtils;

/**
 * @author Arne Vandamme
 */
public abstract class ClassLoadingUtils
{
	private ClassLoadingUtils() {
	}

	public static Class loadClass( String className ) throws ClassNotFoundException, NoClassDefFoundError {
		return Class.forName( className, true, Thread.currentThread().getContextClassLoader() );
	}

	/**
	 * @return null if class can't be resolved
	 */
	public static Class resolveClass( String className ) {
		try {
			return ClassUtils.forName( className, Thread.currentThread().getContextClassLoader() );
		}
		catch ( ClassNotFoundException cnfe ) {
			return null;
		}
	}
}
