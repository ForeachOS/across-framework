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
package com.foreach.across.modules.web.ui;

import com.foreach.across.modules.web.resource.WebResourceRegistry;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Base class for a {@link ViewElementBuilder} of a {@link MutableViewElement}.  Provides defaults functionality
 * like configuring a custom templates or a set of post processors.
 *
 * @param <T>    resulting {@link ViewElement}
 * @param <SELF> return type for builder methods
 */
public abstract class ViewElementBuilderSupport<T extends MutableViewElement, SELF extends ViewElementBuilder<T>> implements ViewElementBuilder<T>
{
	protected String name, customTemplate;
	private Collection<ViewElementPostProcessor<T>> postProcessors = new ArrayList<>();
	private Collection<ViewElement.WitherSetter> setters = new ArrayList<>();

	private Function<ViewElementBuilderContext, ? extends T> elementSupplier;

	/**
	 * Set a separate supplier which supplies the initial element to which this builder should be applied.
	 *
	 * @param supplier to use instead of calling {@link #createElement(ViewElementBuilderContext)}
	 * @return current builder
	 */
	public SELF elementSupplier( Supplier<? extends T> supplier ) {
		return elementSupplier( buildContext -> supplier.get() );
	}

	/**
	 * Set a separate supplier which supplies the initial element to which this builder should be applied.
	 *
	 * @param supplierFunction to use instead of calling {@link #createElement(ViewElementBuilderContext)}
	 * @return current builder
	 */
	@SuppressWarnings("unchecked")
	public SELF elementSupplier( Function<ViewElementBuilderContext, ? extends T> supplierFunction ) {
		elementSupplier = supplierFunction;
		return (SELF) this;
	}

	/**
	 * Apply a collection of setters to the element.
	 */
	@SuppressWarnings("unchecked")
	public SELF with( ViewElement.WitherSetter... setters ) {
		this.setters.addAll( Arrays.asList( setters ) );
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF name( String name ) {
		this.name = name;
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF customTemplate( String template ) {
		this.customTemplate = template;
		return (SELF) this;
	}

	/**
	 * Execute a consumer that applies some configuration to this builder.
	 * Useful if you do not want to break a fluent api style of coding.
	 * <p/>
	 * The consumer is called immediately if it is not {@code null}.
	 */
	@SuppressWarnings("unchecked")
	public SELF configure( Consumer<SELF> consumer ) {
		if ( consumer != null ) {
			consumer.accept( (SELF) this );
		}
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF postProcessor( ViewElementPostProcessor<T> postProcessor ) {
		this.postProcessors.add( postProcessor );
		return (SELF) this;
	}

	/**
	 * Builds the element using a specific {@link ViewElementBuilderContext}.
	 *
	 * @param builderContext provides the context for this build event
	 * @return element created and post processed
	 */
	@Override
	public final T build( ViewElementBuilderContext builderContext ) {
		T element = elementSupplier != null ? elementSupplier.apply( builderContext ) : createElement( builderContext );
		setters.forEach( element::set );

		WebResourceRegistry webResourceRegistry = builderContext.getAttribute( WebResourceRegistry.class );
		if ( webResourceRegistry != null ) {
			registerWebResources( webResourceRegistry );
		}

		return postProcess( builderContext, element );
	}

	protected abstract T createElement( ViewElementBuilderContext builderContext );

	/**
	 * Method to implement if web resources should be registered.  Will only be called
	 * if there is a {@link WebResourceRegistry} present in the current (thread local) context.
	 *
	 * @param webResourceRegistry to add entries to
	 */
	protected void registerWebResources( WebResourceRegistry webResourceRegistry ) {

	}

	// todo: refactor so this always gets applied
	protected T apply( T viewElement, ViewElementBuilderContext builderContext ) {
		if ( name != null ) {
			viewElement.setName( name );
		}
		if ( customTemplate != null ) {
			viewElement.setCustomTemplate( customTemplate );
		}

		return viewElement;
	}

	protected final T postProcess( ViewElementBuilderContext builderContext, T viewElement ) {
		boolean defaultPostProcessorPresent = false;

		for ( ViewElementPostProcessor<T> postProcessor : postProcessors ) {
			if ( postProcessor == DefaultViewElementPostProcessor.INSTANCE ) {
				defaultPostProcessorPresent = true;
			}

			postProcessor.postProcess( builderContext, viewElement );
		}

		if ( !defaultPostProcessorPresent ) {
			DefaultViewElementPostProcessor.INSTANCE.postProcess( builderContext, viewElement );
		}

		return viewElement;
	}

	/**
	 * Encapsulates either a {@link ViewElement} or {@link ViewElementBuilder} for fetching within a
	 * {@link ViewElementBuilderContext}.
	 */
	public static class ElementOrBuilder
	{
		private final Object elementOrBuilder;

		protected ElementOrBuilder( Object elementOrBuilder ) {
			this.elementOrBuilder = elementOrBuilder;
		}

		public static ElementOrBuilder wrap( ViewElement viewElement ) {
			return new ElementOrBuilder( viewElement );
		}

		public static ElementOrBuilder wrap( ViewElementBuilder builder ) {
			return new ElementOrBuilder( builder );
		}

		public static Collection<ElementOrBuilder> wrap( Iterable<?> viewElements ) {
			List<ElementOrBuilder> wrapped = new ArrayList<>();
			for ( Object viewElement : viewElements ) {
				if ( viewElement != null ) {
					Assert.isTrue( viewElement instanceof ViewElement || viewElement instanceof ViewElementBuilder,
					               "viewElement should be an instance of ViewElement of ViewElementBuilder" );
					wrapped.add( new ElementOrBuilder( viewElement ) );
				}
			}

			return wrapped;
		}

		public static Collection<ElementOrBuilder> wrap( ViewElementBuilder... viewElementBuilders ) {
			List<ElementOrBuilder> wrapped = new ArrayList<>( viewElementBuilders.length );
			for ( ViewElementBuilder builder : viewElementBuilders ) {
				if ( builder != null ) {
					wrapped.add( new ElementOrBuilder( builder ) );
				}
			}

			return wrapped;
		}

		@SuppressWarnings("unchecked")
		public <V> V getSource() {
			return (V) elementOrBuilder;
		}

		public boolean isBuilder() {
			return elementOrBuilder instanceof ViewElementBuilder;
		}

		public ViewElement get( ViewElementBuilderContext builderContext ) {
			if ( isBuilder() ) {
				return ( (ViewElementBuilder) elementOrBuilder ).build( builderContext );
			}

			return (ViewElement) elementOrBuilder;
		}
	}
}
