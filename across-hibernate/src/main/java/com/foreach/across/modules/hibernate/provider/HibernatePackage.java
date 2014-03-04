package com.foreach.across.modules.hibernate.provider;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

/**
 * Represents a set of configuration items for Hibernate.
 */
public class HibernatePackage
{
	private Collection<String> packagesToScan = new HashSet<String>();
	private Collection<Class<?>> annotatedClasses = new HashSet<Class<?>>();
	private Collection<String> mappingResources = new HashSet<String>();

	public Class<?>[] getAnnotatedClasses() {
		return annotatedClasses.toArray( new Class[annotatedClasses.size()] );
	}

	public String[] getPackagesToScan() {
		return packagesToScan.toArray( new String[packagesToScan.size()] );
	}

	public String[] getMappingResources() {
		return mappingResources.toArray( new String[mappingResources.size()] );
	}

	public void add( HibernatePackageProvider provider ) {
		packagesToScan.addAll( Arrays.asList( provider.getPackagesToScan() ) );
		annotatedClasses.addAll( Arrays.asList( provider.getAnnotatedClasses() ) );
		mappingResources.addAll( Arrays.asList( provider.getMappingResources() ) );
	}
}
