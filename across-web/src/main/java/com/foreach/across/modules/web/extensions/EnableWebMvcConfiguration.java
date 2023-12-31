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
package com.foreach.across.modules.web.extensions;

import com.foreach.across.core.annotations.ModuleConfiguration;
import com.foreach.across.core.context.bootstrap.AcrossBootstrapConfigurer;
import com.foreach.across.core.context.registry.AcrossContextBeanRegistry;
import com.foreach.across.modules.web.AcrossWebModule;
import com.foreach.across.modules.web.mvc.PrefixingRequestMappingHandlerMapping;
import com.foreach.across.modules.web.resource.WebResourceRegistryInterceptor;
import com.foreach.across.modules.web.template.LayoutSupportingExceptionHandlerExceptionResolver;
import com.foreach.across.modules.web.template.WebTemplateInterceptor;
import lombok.RequiredArgsConstructor;
import org.springframework.aop.support.annotation.AnnotationClassFilter;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcRegistrations;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.stereotype.Controller;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.mvc.method.annotation.ExceptionHandlerExceptionResolver;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.util.List;

/**
 * Activates Web MVC support in the post processor module.
 * Infrastructure created here will not be available except after refresh.
 * <p/>
 * Requires configuration classes to be either injected in the post processor module as well,
 * or to be exposed.
 *
 * @author Arne Vandamme
 * @since 3.0.0
 */
@ModuleConfiguration(AcrossBootstrapConfigurer.CONTEXT_POSTPROCESSOR_MODULE)
@Import(WebMvcAutoConfiguration.class)
@RequiredArgsConstructor
public class EnableWebMvcConfiguration implements WebMvcRegistrations
{
	private final AcrossContextBeanRegistry beanRegistry;

	/**
	 * Create custom request mapping handler mapping that only matches on @Controller instead of anything with @RequestMapping.
	 *
	 * @return mapping new instance
	 */
	@Override
	public RequestMappingHandlerMapping getRequestMappingHandlerMapping() {
		return new PrefixingRequestMappingHandlerMapping( new AnnotationClassFilter( Controller.class, true ) );
	}

	/**
	 * Create custom layout supporting handler exception resolver.
	 *
	 * @return exception resolver instance
	 */
	@Override
	public ExceptionHandlerExceptionResolver getExceptionHandlerExceptionResolver() {
		LayoutSupportingExceptionHandlerExceptionResolver exceptionHandlerExceptionResolver = new LayoutSupportingExceptionHandlerExceptionResolver();

		beanRegistry.findBeanOfTypeFromModule( AcrossWebModule.NAME, WebResourceRegistryInterceptor.class )
		            .ifPresent( exceptionHandlerExceptionResolver::setWebResourceRegistryInterceptor );
		beanRegistry.findBeanOfTypeFromModule( AcrossWebModule.NAME, WebTemplateInterceptor.class )
		            .ifPresent( exceptionHandlerExceptionResolver::setWebTemplateInterceptor );

		return exceptionHandlerExceptionResolver;
	}

	/**
	 * Replace the {@link org.springframework.core.convert.ConversionService} bean definition,
	 * ensure the earlier created service from AcrossWebModule is being used.
	 * <p/>
	 * Manually iterate over the {@link WebMvcConfigurer} instances as the default MVC support no longer
	 * adds the formatter since we overruled the bean definition.
	 *
	 * @param beanRegistry to get the conversion service
	 * @return existing instance
	 */
	@Bean
	public FormattingConversionService mvcConversionService( AcrossContextBeanRegistry beanRegistry, List<WebMvcConfigurer> configurers ) {
		FormattingConversionService conversionService = beanRegistry.getBeanFromModule( AcrossWebModule.NAME, AcrossWebModule.CONVERSION_SERVICE_BEAN );
		configurers.forEach( c -> c.addFormatters( conversionService ) );
		return conversionService;
	}

	/**
	 * Inject {@link ServerProperties} for {@link ErrorMvcAutoConfiguration} to work.
	 *
	 * @return ServerProperties.
	 */
	@Bean
	@ConditionalOnMissingBean
	ServerProperties serverProperties() {
		return new ServerProperties();
	}

	/**
	 * Remove bean definitions of beans that work in a regular Spring Boot application, but not from within an Across module.
	 *
	 * @return post processor
	 */
	@Bean
	static BeanDefinitionRegistryPostProcessor uselessBeansRemover() {
		return new BeanDefinitionRegistryPostProcessor()
		{
			@Override
			public void postProcessBeanDefinitionRegistry( BeanDefinitionRegistry registry ) throws BeansException {
				if ( registry.containsBeanDefinition( "errorPageCustomer" ) ) {
					registry.removeBeanDefinition( "errorPageCustomizer" );
				}
			}

			@Override
			public void postProcessBeanFactory( ConfigurableListableBeanFactory beanFactory ) throws BeansException {

			}
		};
	}
}
