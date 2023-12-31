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
package com.foreach.across.modules.web.ui.elements.support;

import com.foreach.across.modules.web.ui.ViewElement;
import com.foreach.across.modules.web.ui.elements.HtmlViewElement;
import lombok.RequiredArgsConstructor;

import java.util.function.Predicate;

/**
 * Function for {@link com.foreach.across.modules.web.ui.ViewElement.Wither} which allows for
 * setting and removing of css classes on a {@link com.foreach.across.modules.web.ui.elements.HtmlViewElement}.
 * Also a {@link Predicate} which can be used with {@link ViewElement#matches(Predicate)} to check if the css classes are present.
 *
 * @author Arne Vandamme
 * @since 5.0.0
 */
@RequiredArgsConstructor
public class CssClassWitherFunction implements ViewElement.WitherSetter<HtmlViewElement>, ViewElement.WitherRemover<HtmlViewElement>, Predicate<HtmlViewElement>
{
	private final String[] cssClassNames;

	@Override
	public void applyTo( HtmlViewElement target ) {
		target.addCssClass( cssClassNames );
	}

	@Override
	public void removeFrom( HtmlViewElement target ) {
		target.removeCssClass( cssClassNames );
	}

	@Override
	public boolean test( HtmlViewElement target ) {
		for ( String cssClassName : cssClassNames ) {
			if ( !target.hasCssClass( cssClassName ) ) {
				return false;
			}
		}
		return true;
	}
}
