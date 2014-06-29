package com.foreach.across.core.annotations.conditions;

import com.foreach.across.core.AcrossModule;
import com.foreach.across.core.annotations.AcrossCondition;
import com.foreach.across.core.context.info.AcrossContextInfo;
import com.foreach.across.core.context.info.AcrossModuleInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.*;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class AcrossConditionCondition implements Condition
{
	private static final Logger LOG = LoggerFactory.getLogger( AcrossConditionCondition.class );

	@Override
	public boolean matches( ConditionContext context, AnnotatedTypeMetadata metadata ) {
		String[] expressions = (String[]) metadata.getAnnotationAttributes( AcrossCondition.class.getName() ).get(
				"value" );

		for ( String expression : expressions ) {
			if ( !StringUtils.isBlank( expression )
					&& !evaluate( expression, context.getBeanFactory(), context.getEnvironment() ) ) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Based on implementation of OnExpressionCondition in spring-boot package.
	 * https://github.com/spring-projects/spring-boot/blob/master/spring-boot-autoconfigure/src/main/java/org/springframework/boot/autoconfigure/condition/OnExpressionCondition.java
	 */
	public static boolean evaluate( String expression,
	                                ConfigurableListableBeanFactory beanFactory,
	                                Environment environment ) {
		// Explicitly allow environment placeholders inside the expression
		expression = environment.resolvePlaceholders( expression );

		if ( !expression.startsWith( "#{" ) ) {
			// For convenience allow user to provide bare expression with no #{} wrapper
			expression = "#{" + expression + "}";
		}

		BeanExpressionResolver resolver = beanFactory.getBeanExpressionResolver();
		BeanExpressionContext expressionContext = new CurrentModuleBeanExpressionContext( beanFactory, null );

		if ( resolver == null ) {
			resolver = new StandardBeanExpressionResolver();
		}

		boolean result = (Boolean) resolver.evaluate( expression, expressionContext );

		if ( LOG.isTraceEnabled() ) {
			LOG.trace( "AcrossDepends expression {} evaluated to {}", expression, result );
		}

		return result;
	}

	private static final class CurrentModuleBeanExpressionContext extends BeanExpressionContext
	{
		private CurrentModuleBeanExpressionContext( ConfigurableBeanFactory beanFactory, Scope scope ) {
			super( beanFactory, scope );
		}

		// Provided for SPEL property
		public AcrossModule getCurrentModule() {
			AcrossModuleInfo moduleInfo = getBeanFactory().getBean( AcrossContextInfo.class )
			                                              .getModuleBeingBootstrapped();

			return moduleInfo != null ? moduleInfo.getModule() : null;

		}
	}
}