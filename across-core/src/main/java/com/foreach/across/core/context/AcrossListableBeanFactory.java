package com.foreach.across.core.context;

import com.foreach.across.core.context.registry.AcrossContextBeanRegistry;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.GenericTypeResolver;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Extends a {@link org.springframework.beans.factory.support.DefaultListableBeanFactory}
 * with support for Exposed beans.
 * <p/>
 * Exposed beans are fetched from the module context but are not managed by the bean factory.
 */
public class AcrossListableBeanFactory extends DefaultListableBeanFactory
{
	public AcrossListableBeanFactory() {
	}

	public AcrossListableBeanFactory( BeanFactory parentBeanFactory ) {
		super( parentBeanFactory );
	}

	/**
	 * Ensures ExposedBeanDefinition instances are returned as the RootBeanDefinition.
	 */
	@Override
	protected RootBeanDefinition getMergedBeanDefinition( String beanName,
	                                                      BeanDefinition bd,
	                                                      BeanDefinition containingBd ) throws BeanDefinitionStoreException {
		if ( bd instanceof ExposedBeanDefinition ) {
			return (ExposedBeanDefinition) bd;
		}

		return super.getMergedBeanDefinition( beanName, bd, containingBd );
	}

	/**
	 * An exposed bean definition does not really get created but gets fetched
	 * from the external context.
	 */
	@Override
	protected Object doCreateBean( String beanName, RootBeanDefinition mbd, Object[] args ) {
		if ( mbd instanceof ExposedBeanDefinition ) {
			List<ConstructorArgumentValues.ValueHolder> factoryArguments =
					mbd.getConstructorArgumentValues().getGenericArgumentValues();

			return ( (AcrossContextBeanRegistry) getBean( mbd.getFactoryBeanName() ) ).getBeanFromModule(
					(String) factoryArguments.get( 0 ).getValue(),
					(String) factoryArguments.get( 1 ).getValue()
			);
		}

		return super.doCreateBean( beanName, mbd, args );
	}

	@Override
	protected Class<?> getTypeForFactoryBean( String beanName, RootBeanDefinition mbd ) {
		if ( mbd instanceof ExposedBeanDefinition ) {
			List<ConstructorArgumentValues.ValueHolder> factoryArguments =
					mbd.getConstructorArgumentValues().getGenericArgumentValues();

			return ( (AcrossContextBeanRegistry) getBean( mbd.getFactoryBeanName() ) ).getBeanTypeFromModule(
					(String) factoryArguments.get( 0 ).getValue(),
					(String) factoryArguments.get( 1 ).getValue()
			);
		}

		return super.getTypeForFactoryBean( beanName, mbd );
	}
}
