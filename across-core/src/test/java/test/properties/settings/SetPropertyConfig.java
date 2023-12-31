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

package test.properties.settings;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class SetPropertyConfig
{
	@Value("${contextValue}")
	public String contextValue;

	@Value("${moduleSourceValue}")
	public String moduleSourceValue;

	@Value("${moduleDirectValue}")
	public String moduleDirectValue;

	@Value("${contextDirectValue}")
	public int contextDirectValue;

	@Value("${unresolvable:50}")
	public long unresolvable;

	@Getter
	private final PropertiesModuleSettings settings;
}
