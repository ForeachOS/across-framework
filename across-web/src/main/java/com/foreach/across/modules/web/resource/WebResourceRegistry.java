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

import com.foreach.across.modules.web.ui.ViewElementBuilder;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static com.foreach.across.modules.web.resource.WebResource.*;

/**
 * <p>Registry for a set of web resources. Usually there is one registry per view.
 * Used to specify things like css files, javascript files etc that should be loaded by the page.
 * These can be added with a specific order.</p>
 * <p>Web resources are divided into separate named buckets. A single bucket usually corresponds with
 * a location in a layout page, for example "the resources that should be added inside the {@code <head>} of a page".
 * <p>
 * As of version {@code 3.2.0} the functionality of web resources and the registry has been thoroughly
 * reworked for more flexibility.
 * <p>
 * A single web resource is represented by a {@link WebResourceReference}, the {@link WebResource} class itself is
 * deprecated. The rendering of a resource is determined by the {@link ViewElementBuilder} attached to the reference.
 * Default implementations for CSS, Javascript and META tags are available and can be created using the factory
 * methods {@link WebResource#css()}, {@link WebResource#javascript()} or {@link WebResource#meta()}.
 * <p>
 * In practice the easiest way to add web resources is by configuring them as rules to apply to the registry: *
 * <pre>{@code
 * webResourceRegistry.apply(
 *     WebResourceRule.add( WebResource.css( "@static:/MODULE_RESORCES/css/bootstrap.min.css" ) ).withKey( "bootstrap-min-css" ).toBucket( CSS ),
 *     WebResourceRule.add( WebResource.javascript( "bootstrap.min.js" ) ).withKey( "bootstrap-min-js" ).toBucket( JAVASCRIPT_PAGE_END ),
 *     WebResourceRule.add( WebResource.css().inline( "body {background-color: powderblue;}" ) ).withKey( "inline-body-blue" ).toBucket( CSS )
 * );
 * }</pre>
 * <p>
 * A web resource can optionally be registered with a key. This is a {@code String} that identifies the resource in its bucket.
 * Alternatively the {@link ViewElementBuilder} can implement {@link WebResourceKeyProvider} to provide a default key.
 *
 * @see WebResource
 * @see WebResourceReference
 * @see WebResourcePackage
 * @see WebResourceKeyProvider
 * @since 1.0.0
 */
public class WebResourceRegistry
{
	private String defaultLocation = WebResource.RELATIVE;

	private final WebResourcePackageManager packageManager;
	private final Map<String, List<WebResourceReference>> webResources = new LinkedHashMap<>();

	private final Set<String> installedPackages = new HashSet<>();

	public WebResourceRegistry( WebResourcePackageManager packageManager ) {
		this.packageManager = packageManager;
	}

	/**
	 * @return The default location resources will be registered with.
	 * @see com.foreach.across.modules.web.resource.WebResource
	 * @deprecated since 3.2.0
	 */
	@Deprecated
	public String getDefaultLocation() {
		return defaultLocation;
	}

	/**
	 * @param defaultLocation Default location to set.
	 * @see com.foreach.across.modules.web.resource.WebResource
	 * @deprecated since 3.2.0 - it's advised to always specify a bucket name
	 */
	@Deprecated
	public void setDefaultLocation( String defaultLocation ) {
		this.defaultLocation = defaultLocation;
	}

	/**
	 * Register a specific resource.
	 *
	 * @param webResource WebResource instance to add.
	 * @deprecated since 3.2.0 - replaced by {@link WebResourceRule#add(ViewElementBuilder)}
	 */
	@Deprecated
	public void add( @NonNull WebResource webResource ) {
		addWithKey( webResource.getType(), webResource.getKey(), webResource.getData(), webResource.getLocation() );
	}

	/**
	 * Register a new resource with the default location.
	 * Since there is no key, any other resource of the same type with the same data will be replaced.
	 *
	 * @param type Type of the resource, see {@link com.foreach.across.modules.web.resource.WebResource} for constants.
	 * @param data Data to register.
	 * @deprecated since 3.2.0 - replaced by {@link WebResourceRule#add(ViewElementBuilder)}
	 */
	@Deprecated
	public void add( String type, Object data ) {
		add( type, data, getDefaultLocation() );
	}

	/**
	 * Registers a resource with the location specified.
	 * Since there is no key, any other resource of the same type with the same data will be replaced.
	 *
	 * @param type     Type of the resource, see {@link com.foreach.across.modules.web.resource.WebResource} for constants.
	 * @param data     Data to register.
	 * @param location Where the data is available.
	 * @deprecated since 3.2.0 - replaced by {@link WebResourceRule#add(ViewElementBuilder)}
	 */
	@Deprecated
	public void add( String type, Object data, String location ) {
		addWithKey( type, null, data, location );
	}

	/**
	 * Registers a resource under the given key.  For complex interactions, it is often better to provide a key.
	 * Existing resources of this type with the same key will be replaced.
	 *
	 * @param type Type of the resource, see {@link com.foreach.across.modules.web.resource.WebResource} for constants.
	 * @param key  Unique key under which to register a resource.
	 * @param data Data to register.
	 * @deprecated since 3.2.0 - replaced by {@link WebResourceRule#add(ViewElementBuilder)}
	 */
	@Deprecated
	public void addWithKey( String type, String key, Object data ) {
		addWithKey( type, key, data, getDefaultLocation() );
	}

	/**
	 * Registers a resource under the given key.  For complex interactions, it is often better to provide a key.
	 * Existing resources of this type with the same key will be replaced.
	 *
	 * @param type Type of the resource, see {@link com.foreach.across.modules.web.resource.WebResource} for constants.
	 * @param key  Unique key under which to register a resource.
	 * @param data Data to register.
	 * @deprecated since 3.2.0 - replaced by {@link WebResourceRule#add(ViewElementBuilder)}
	 */
	@Deprecated
	public void addWithKey( String type, String key, Object data, String location ) {
		WebResource existing = findResource( type, key, data );

		if ( existing == null ) {
			WebResource resource = new WebResource( type, key, data, location );

			addResourceToBucket(
					new WebResourceReference( createViewElementBuilderForWebResource( resource ), key, null, null, null, resource ),
					type
			);
		}
		else {
			existing.setKey( key );
			existing.setData( data );
			existing.setLocation( location );

			// replace the reference
			addResourceToBucket(
					new WebResourceReference( createViewElementBuilderForWebResource( existing ), key, null, null, null, existing ),
					type,
					true
			);
		}
	}

	@SuppressWarnings( "deprecation" )
	private ViewElementBuilder createViewElementBuilderForWebResource( WebResource webResource ) {
		switch ( webResource.getType() ) {
			case CSS:
				switch ( webResource.getLocation() ) {
					case INLINE:
					case DATA:
						return WebResource.css().inline( Objects.toString( webResource.getData() ) );
					case EXTERNAL:
						return WebResource.css( "!" + webResource.getData() );
					case VIEWS:
						return WebResource.css( "@resource:" + webResource.getData() );
					default:
						return WebResource.css( Objects.toString( webResource.getData() ) );
				}
			case JAVASCRIPT:
			case JAVASCRIPT_PAGE_END:
				switch ( webResource.getLocation() ) {
					case INLINE:
						return WebResource.javascript().inline( Objects.toString( webResource.getData() ) );
					case DATA:
						return WebResource.globalJsonData( "Across." + webResource.getKey(), webResource.getData() );
					case EXTERNAL:
						return WebResource.javascript( "!" + webResource.getData() );
					case VIEWS:
						return WebResource.javascript( "@resource:" + webResource.getData() );
					default:
						return WebResource.javascript( Objects.toString( webResource.getData() ) );
				}
			default:
				return WebResource.javascript().inline( Objects.toString( webResource.getData() ) );
		}
	}

	@SuppressWarnings( "deprecation" )
	private WebResource findResource( String type, String key, Object data ) {
		WebResource matchOnKey = null, matchOnData = null;

		List<WebResourceReference> references = webResources.get( type );
		if ( references != null ) {
			for ( WebResourceReference reference : references ) {
				WebResource resource = reference.getResource();
				if ( resource != null ) {
					// We are interested in resources with the same key
					if ( key != null && StringUtils.equals( key, resource.getKey() ) ) {
						matchOnKey = resource;
					}

					// A resource without key but the same data will always match
					if ( !resource.hasKey() && Objects.equals( data, resource.getData() ) ) {
						matchOnData = resource;
					}
				}
			}
		}

		return matchOnKey != null ? matchOnKey : matchOnData;
	}

	/**
	 * Will remove all registered resources with the given content.
	 * Requires that the resource data equals() the requested data.
	 *
	 * @param data Content the resource should have.
	 * @deprecated since 3.2.0 - removing by "data" is not supported anymore
	 */
	@Deprecated
	public void removeResource( Object data ) {
		for ( Map.Entry<String, List<WebResourceReference>> references : webResources.entrySet() ) {
			// Only for old style references
			for ( WebResourceReference reference : references.getValue() ) {
				WebResource resource = reference.getResource();
				if ( resource != null ) {
					if ( Objects.equals( data, resource.getData() ) ) {
						references.getValue().remove( reference );
					}
				}
			}
		}
	}

	/**
	 * Will remove all registered resources of that type with the given content.
	 *
	 * @param type Type of the resource, see {@link com.foreach.across.modules.web.resource.WebResource} for constants.
	 * @param data Content the resource should have.
	 * @deprecated since 3.2.0 - removing by "data" is not supported anymore
	 */
	@Deprecated
	public void removeResource( String type, Object data ) {
		List<WebResourceReference> references = webResources.get( type );
		if ( references != null ) {
			// Only for old style references
			for ( WebResourceReference reference : references ) {
				WebResource resource = reference.getResource();
				if ( resource != null ) {
					if ( Objects.equals( data, resource.getData() ) ) {
						references.remove( reference );
					}
				}
			}
		}
	}

	/**
	 * Will remove all resources registered under the key specified.
	 *
	 * @param key Key the resource is registered under.
	 */
	public void removeResourceWithKey( @NonNull String key ) {
		for ( List<WebResourceReference> resources : webResources.values() ) {
			resources.removeIf( resource -> StringUtils.equals( key, resource.getKey() ) );
		}
	}

	/**
	 * Will remove the resource with a specific key from the bucket
	 *
	 * @param key    Key the resource is registered under.
	 * @param bucket Bucket name, see {@link com.foreach.across.modules.web.resource.WebResource} for constants.
	 * @return reference that was removed
	 */
	public Optional<WebResourceReference> removeResourceWithKeyFromBucket( @NonNull String key, @NonNull String bucket ) {
		List<WebResourceReference> resources = webResources.getOrDefault( bucket, Collections.emptyList() );
		int index = findPosition( key, resources );
		return index >= 0 ? Optional.of( resources.remove( index ) ) : Optional.empty();
	}

	/**
	 * Installs all resources attached to the packages with the names specified.
	 * This requires the packages to be registered in the attached {@link WebResourcePackageManager}.
	 * <p>
	 * Note that a package will only be installed the first time, if it has been installed previously, it will be skipped.
	 *
	 * @param packageNames Names of the packages to install.
	 */
	public void addPackage( @NonNull String... packageNames ) {
		if ( packageManager == null ) {
			throw new IllegalStateException( "A WebResourcePackageManager is required for using named packages" );
		}

		for ( String packageName : packageNames ) {
			if ( !installedPackages.contains( packageName ) ) {
				WebResourcePackage webResourcePackage = packageManager.getPackage( packageName );

				if ( webResourcePackage == null ) {
					throw new IllegalArgumentException( "No WebResourcePackage found with name " + packageName );
				}

				installedPackages.add( packageName );
				webResourcePackage.install( this );
			}
		}
	}

	/**
	 * Will call the {@link WebResourcePackage#uninstall(WebResourceRegistry)} methods of the packages specified,
	 * if they are installed in the current registry. Note that the uninstalling should be used with care
	 * as it is hard to predict the exact behaviour.
	 *
	 * @param packageNames Names of the packages.
	 * @deprecated since 3.2.0 - might be removed in a future release as behaviour is hard to get consistent
	 */
	@Deprecated
	public void removePackage( @NonNull String... packageNames ) {
		for ( String packageName : packageNames ) {
			if ( installedPackages.contains( packageName ) ) {
				WebResourcePackage webResourcePackage = packageManager.getPackage( packageName );

				// Package not found is ignored
				if ( webResourcePackage != null ) {
					webResourcePackage.uninstall( this );
					installedPackages.remove( packageName );
				}
			}
		}
	}

	/**
	 * Clears the entire registry, for all buckets.
	 */
	public void clear() {
		webResources.values().clear();
	}

	/**
	 * Removes all resources in the given bucket.
	 *
	 * @param bucket Name of the bucket.
	 */
	public void clear( @NonNull String bucket ) {
		List<WebResourceReference> references = webResources.get( bucket );
		if ( references != null ) {
			references.clear();
		}
	}

	/**
	 * Lists all resources for a given type.
	 *
	 * @param type Type of the resource.
	 * @return Collection of WebResource instances.
	 * @deprecated since 3.2.0 - replaced by {@link #getResourcesForBucket(String)}
	 */
	@Deprecated
	public Collection<WebResource> getResources( String type ) {
		List<WebResource> filtered = new LinkedList<>();

		List<WebResourceReference> resources = webResources.get( type );
		if ( resources != null ) {
			for ( WebResourceReference resource : resources ) {
				WebResource webResource = resource.getResource();
				if ( webResource != null ) {
					filtered.add( webResource );
				}
			}
		}

		return filtered;
	}

	/**
	 * Lists all resources in this registry.
	 *
	 * @return Collection of WebResource instances.
	 * @deprecated since 3.2.0 - replaced by {@link #getResourcesForBucket(String)}
	 */
	@Deprecated
	public Collection<WebResource> getResources() {
		List<WebResource> items = new LinkedList<>();
		for ( Map.Entry<String, List<WebResourceReference>> webResources : webResources.entrySet() ) {
			for ( WebResourceReference reference : webResources.getValue() ) {
				if ( reference.getResource() != null ) {
					items.add( reference.getResource() );
				}
			}
		}
		return items;
	}

	/**
	 * Return all bucket names in this registry.
	 */
	public Set<String> getBuckets() {
		return Collections.unmodifiableSet( webResources.keySet() );
	}

	/**
	 * Return a {@link WebResourceReferenceCollection} from all resources in this registry for a specific bucket.
	 *
	 * @param bucket The bucket name.
	 */
	public WebResourceReferenceCollection getResourcesForBucket( @NonNull String bucket ) {
		List<WebResourceReference> filtered = new LinkedList<>();

		List<WebResourceReference> items = webResources.get( bucket );
		if ( items != null ) {
			filtered.addAll( items );
		}

		return new WebResourceReferenceCollection( filtered );
	}

	/**
	 * Merges all resources of the other registry in this one.
	 *
	 * @param registry Registry containing resource to be copied.
	 */
	public void merge( @NonNull WebResourceRegistry registry ) {
		for ( Map.Entry<String, List<WebResourceReference>> references : registry.webResources.entrySet() ) {
			for ( WebResourceReference reference : references.getValue() ) {
				addResourceToBucket( reference, references.getKey() );
			}
		}
	}

	/**
	 * Will apply the set of {@link com.foreach.across.modules.web.resource.WebResourceRule} items to the registry.
	 */
	public void apply( @NonNull WebResourceRule... webResourceRules ) {
		apply( Arrays.asList( webResourceRules ) );
	}

	/**
	 * Will apply the set of {@link com.foreach.across.modules.web.resource.WebResourceRule} items to the registry.
	 */
	public void apply( @NonNull Collection<WebResourceRule> webResourceRules ) {
		webResourceRules.forEach( wr -> wr.applyTo( this ) );
	}

	/**
	 * Adds a specific {@link WebResourceReference} to the specified bucket, creating the bucket if it does not exist.
	 * If there is already a reference with that key in the bucket, it will be replaced, see
	 * {@link #addResourceToBucket(WebResourceReference, String, boolean)} if you want to control this behaviour.
	 */
	public boolean addResourceToBucket( @NonNull WebResourceReference webResourceReference, @NonNull String bucket ) {
		return addResourceToBucket( webResourceReference, bucket, true );
	}

	/**
	 * Add a {@link WebResourceReference} to the specified bucket, creating the bucket if it does not exist.
	 * The value of {@code replaceIfExists} will determine if a resource should be replaced if it is already
	 * present in the bucket. Replacing a resource will remove the original and put the new value in exactly
	 * the same spot. If {@code replaceIfExists} is {@code false} but a resource already has this key, the
	 * new resource will <strong>not be added at all</strong> and {@code false} will be returned.
	 *
	 * @param webResourceReference resource to add
	 * @param bucket               to which to add the resource
	 * @param replaceIfExists      true if the resource should be replaced
	 * @return true if resource was added or replaced, false if it already existed and has not been replaced
	 */
	public boolean addResourceToBucket( @NonNull WebResourceReference webResourceReference, @NonNull String bucket, boolean replaceIfExists ) {
		String key = webResourceReference.getKey();

		List<WebResourceReference> resources = webResources.computeIfAbsent( bucket, w -> new LinkedList<>() );

		if ( key == null ) {
			resources.add( webResourceReference );
		}
		else {
			int index = findPosition( key, resources );
			if ( index >= 0 ) {
				if ( replaceIfExists ) {
					resources.set( index, webResourceReference );
				}
				else {
					return false;
				}
			}
			else {
				resources.add( webResourceReference );
			}
		}

		return true;
	}

	/**
	 * Find the resource reference registered under a specific key in a bucket.
	 *
	 * @param key    of the resource
	 * @param bucket name
	 * @return reference
	 */
	public Optional<WebResourceReference> findResourceWithKeyInBucket( @NonNull String key, @NonNull String bucket ) {
		List<WebResourceReference> resources = webResources.getOrDefault( bucket, Collections.emptyList() );
		return resources.stream().filter( r -> StringUtils.equals( r.getKey(), key ) ).findFirst();
	}

	private int findPosition( String key, List<WebResourceReference> references ) {
		for ( int i = 0; i < references.size(); i++ ) {
			if ( StringUtils.equals( key, references.get( i ).getKey() ) ) {
				return i;
			}
		}

		return -1;
	}
}
