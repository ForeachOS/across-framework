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

import com.foreach.across.core.AcrossContext;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

/**
 * @author Arne Vandamme
 * @since 3.0.0
 */
public abstract class AcrossContextEvent extends ApplicationEvent implements AcrossEvent
{
	@Getter
	private final AcrossContext context;

	protected AcrossContextEvent( AcrossContext context ) {
		super( context );
		this.context = context;
	}
}
