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

package com.foreach.across.core.annotations;

import com.foreach.across.core.context.bootstrap.AcrossBootstrapConfig;
import com.foreach.across.core.context.info.AcrossContextInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * Condition that checks that a given module is present in the AcrossContext.
 * To be used on @Configuration and @Bean instances to load components only if other modules
 * are being loaded.
 *
 * @see com.foreach.across.core.annotations.ConditionalOnAcrossModule
 */
@Slf4j
class AcrossModuleCondition extends SpringBootCondition
{
	@Override
	public ConditionOutcome getMatchOutcome( ConditionContext context, AnnotatedTypeMetadata metadata ) {
		Map<String, Object> attributes = metadata.getAnnotationAttributes( ConditionalOnAcrossModule.class.getName() );

		Assert.notNull( attributes, "@ConditionalOnAcrossModule was expected" );

		String[] allOf = (String[]) attributes.get( "allOf" );
		if ( allOf == null || allOf.length == 0 ) {
			allOf = (String[]) attributes.get( "value" );
		}
		String[] anyOf = (String[]) attributes.get( "anyOf" );
		String[] noneOf = (String[]) attributes.get( "noneOf" );

		try {
			AcrossContextInfo acrossContext = findAcrossContextInfo( context.getBeanFactory() );

			if ( acrossContext != null ) {
				return applies( acrossContext.getBootstrapConfiguration(), allOf, anyOf, noneOf );
			}
		}
		catch ( NoSuchBeanDefinitionException ignore ) {
		}

		return ConditionOutcome.match( "use of ConditionalOnAcrossModule outside of an AcrossContext always matches" );
	}

	private AcrossContextInfo findAcrossContextInfo( ConfigurableListableBeanFactory beanFactory ) {
		if ( beanFactory.containsLocalBean( AcrossContextInfo.BEAN ) ) {
			return beanFactory.getBean( AcrossContextInfo.BEAN, AcrossContextInfo.class );
		}

		BeanFactory parentFactory = beanFactory.getParentBeanFactory();
		return parentFactory instanceof ConfigurableListableBeanFactory ? findAcrossContextInfo( (ConfigurableListableBeanFactory) parentFactory ) : null;
	}

	/**
	 * Checks if the required and optional dependencies apply against a given bootstrap configuration.
	 *
	 * @param config Bootstrap configuration to check against.
	 * @param allOf  Required modules.
	 * @param anyOf  Optional modules.
	 * @param noneOf Forbidden modules.
	 * @return True if all required modules are present and at least one of the optionals (if any defined).
	 * @see com.foreach.across.core.annotations.ConditionalOnAcrossModule
	 */
	private ConditionOutcome applies( AcrossBootstrapConfig config, String[] allOf, String[] anyOf, String[] noneOf ) {
		if ( allOf.length > 0 || anyOf.length > 0 || noneOf.length > 0 ) {
			for ( String requiredModuleId : allOf ) {
				if ( !config.hasModule( requiredModuleId ) ) {
					return ConditionOutcome.noMatch( "required module " + requiredModuleId + " is not present" );
				}
			}

			for ( String forbiddenModuleId : noneOf ) {
				if ( config.hasModule( forbiddenModuleId ) ) {
					return ConditionOutcome.noMatch( "forbidden module " + forbiddenModuleId + " is present" );
				}
			}

			// If all required modules are present, the condition is matched if there is no optional preference
			boolean shouldLoad = anyOf.length == 0;

			for ( String optionalModuleId : anyOf ) {
				if ( config.hasModule( optionalModuleId ) ) {
					return ConditionOutcome.match( "optional module " + optionalModuleId + " is present" );
				}
			}

			if ( !shouldLoad ) {
				return ConditionOutcome.noMatch(
						"none of the optional modules were present: " + StringUtils.join( anyOf, "," ) );
			}
			else if ( allOf.length > 0 ) {
				return ConditionOutcome.match(
						"all required modules were present: " + StringUtils.join( allOf, "," ) );
			}
		}

		return ConditionOutcome.match( "no required or optional modules were configured for the condition" );
	}
}
