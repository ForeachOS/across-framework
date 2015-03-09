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
package com.foreach.across.test.support;

import com.foreach.across.modules.web.ui.ViewElement;
import com.foreach.across.modules.web.ui.ViewElementBuilderFactory;
import com.foreach.across.modules.web.ui.ViewElementBuilderSupport;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

/**
 * Base unit test for {@link com.foreach.across.modules.web.ui.ViewElementBuilderSupport} implementations.  Mainly
 * verifies that all implemented methods return the same strongly typed builder instances - unless they are deliberately
 * excepted.
 *
 * @param <T> ViewElementBuilder implementation extending ViewElementBuilderSupport
 * @param <U> ViewElement type that is generated by the builder
 */
public abstract class AbstractViewElementBuilderTest<T extends ViewElementBuilderSupport<U, T>, U extends ViewElement>
{
	protected T builder;
	protected U element;

	protected ViewElementBuilderFactory builderFactory;

	@Before
	public void reset() {
		builderFactory = mock( ViewElementBuilderFactory.class );

		builder = createBuilder( builderFactory );
		element = null;
	}

	@Test
	public void commonProperties() {
		assertSame( builder, builder.name( "componentName" ).customTemplate( "custom/template" ) );

		build();

		assertEquals( "componentName", element.getName() );
		assertEquals( "custom/template", element.getCustomTemplate() );
	}

	@Test
	public void methodsShouldReturnBuilderInstance() throws Exception {
		Class<?> c = builder.getClass();

		Collection<String> methodExceptions = Arrays.asList( "build", "wait", "equals", "toString", "hashCode",
		                                                     "getClass", "notify", "notifyAll" );
		methodExceptions.addAll( nonBuilderReturningMethods() );

		for ( Method method : c.getMethods() ) {
			if ( !methodExceptions.contains( method.getName() ) ) {
				Method declared = c.getDeclaredMethod( method.getName(), method.getParameterTypes() );

				assertEquals( "Method [" + method + "] does not return same builder type",
				              c,
				              declared.getReturnType() );
			}
		}
	}

	protected abstract T createBuilder( ViewElementBuilderFactory builderFactory );

	protected Collection<String> nonBuilderReturningMethods() {
		return Collections.emptyList();
	}

	protected void build() {
		element = builder.build( null );
	}
}
