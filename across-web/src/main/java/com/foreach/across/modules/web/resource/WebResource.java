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

package com.foreach.across.modules.web.resource;

import lombok.Getter;
import lombok.Setter;

/**
 * <p>Represents a single entry in the WebResourceRegistry.</p>
 * <p>All constants are deliberately Strings so they can easily be used in different view
 * layers and custom values can be added in other modules.</p>
 */
public class WebResource
{
	/**
	 * Default type of web resources.
	 */
	public static final String CSS = "css";
	public static final String JAVASCRIPT = "javascript";
	public static final String JAVASCRIPT_PAGE_END = "javascript-page-end";

	/**
	 * Used for data that should be serialized and passed to the client (usually as json).
	 */
	public static final String DATA = "data";

	/**
	 * Inline resource - entire content
	 */
	public static final String INLINE = "inline";

	/**
	 * External resource - usually an absolute link
	 */
	public static final String EXTERNAL = "external";

	/**
	 * Relative to the context/controller being rendered - this is usually the default.
	 */
	public static final String RELATIVE = "relative";

	/**
	 * Embedded resource in the views directory - these usually find translated into a path using a
	 * {@link com.foreach.across.modules.web.resource.WebResourceTranslator}.
	 */
	public static final String VIEWS = "views";

	@Deprecated
	@Getter
	@Setter
	private String key, type, location;
	@Deprecated
	@Getter
	@Setter
	private Object data;

	@Deprecated
	public boolean hasKey() {
		return key != null;
	}

	/**
	 * @deprecated since 3.1.3 - replaced by {@link #javascript()} and {@link #css()} methods
	 */
	@Deprecated
	public WebResource( String type, String key, Object data, String location ) {
		this.type = type;
		this.key = key;
		this.data = data;
		this.location = location;
	}

	/**
	 * A builder for constructing a {@link com.foreach.across.modules.web.ui.ViewElementBuilder}
	 * This builder will generate a script tag and can be used for inline or json
	 */
	public static JavascriptWebResourceBuilder javascript() {
		return new JavascriptWebResourceBuilder();
	}

	/**
	 * A builder for constructing a {@link com.foreach.across.modules.web.ui.ViewElementBuilder}
	 * This builder will generate a script tag for external resources
	 */
	public static JavascriptWebResourceBuilder javascript( String url ) {
		return javascript().url( url );
	}

	/**
	 * A builder for constructing a {@link com.foreach.across.modules.web.ui.ViewElementBuilder}
	 * This builder will generate a link or style tag, depending if inline or url is used
	 */
	public static CssWebResourceBuilder css() {
		return new CssWebResourceBuilder();
	}

	/**
	 * A builder for constructing a {@link com.foreach.across.modules.web.ui.ViewElementBuilder}
	 * This builder will generate a link tag
	 */
	public static CssWebResourceBuilder css( String url ) {
		return css().url( url );
	}
}
