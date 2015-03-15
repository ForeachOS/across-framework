package com.foreach.across.modules.web.ui.elements.builder;

import com.foreach.across.modules.web.ui.ViewElement;
import com.foreach.across.modules.web.ui.ViewElementBuilder;
import com.foreach.across.modules.web.ui.ViewElementBuilderContext;
import com.foreach.across.modules.web.ui.ViewElementBuilderSupport;
import com.foreach.across.modules.web.ui.elements.ContainerViewElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class ContainerViewElementBuilderSupport<T extends ContainerViewElement, SELF extends ContainerViewElementBuilderSupport<T, SELF>>
		extends ViewElementBuilderSupport<T, SELF>
{
	private final List<Object> children = new ArrayList<>();
	private String[] sortElements;

	@SuppressWarnings("unchecked")
	public SELF add( ViewElement... viewElements ) {
		Collections.addAll( children, viewElements );
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF add( ViewElementBuilder... viewElements ) {
		Collections.addAll( children, viewElements );
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	public SELF sort( String... elementNames ) {
		this.sortElements = elementNames;
		return (SELF) this;
	}

	protected T apply( T viewElement, ViewElementBuilderContext builderContext ) {
		T container = super.apply( viewElement );

		for ( Object child : children ) {
			if ( child instanceof ViewElement ) {
				container.add( (ViewElement) child );
			}
			else {
				container.add( ( (ViewElementBuilder) child ).build( builderContext ) );
			}
		}

		if ( sortElements != null ) {
			container.sort( sortElements );
		}

		return container;
	}
}
