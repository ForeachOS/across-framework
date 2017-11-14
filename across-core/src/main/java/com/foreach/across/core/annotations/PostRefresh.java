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

package com.foreach.across.core.annotations;

import java.lang.annotation.*;

/**
 * Marks a method as to be autowired by Spring's dependency injection facilities,
 * <u>after</u> the Across context has been bootstrapped.  This is pretty much an
 * equivalent of {@link org.springframework.beans.factory.annotation.Autowired} but
 * specifying a different time-frame for wiring.
 * <p/>
 * It's perfectly possible to use both {@link org.springframework.beans.factory.annotation.Autowired}
 * and {@link PostRefresh} on the same method, in which case it will execute twice.
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface PostRefresh
{
	/**
	 * Declares whether the annotated dependency is required.
	 * <p>Defaults to {@code true}.
	 */
	boolean required() default true;
}
