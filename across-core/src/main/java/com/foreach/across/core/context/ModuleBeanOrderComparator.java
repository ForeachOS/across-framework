package com.foreach.across.core.context;

import com.foreach.across.core.OrderedInModule;
import com.foreach.across.core.annotations.OrderInModule;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.Order;

import java.util.*;

/**
 * <p>
 * A multi-level comparator for sorting beans scanned from the modules in an AcrossContext.
 * Sorts on the following:
 * <ul>
 * <li>global order (Ordered interface or @Order annotation if none)</li>
 * <li>index of the module the bean belongs to</li>
 * <li>order of the bean in the module (@OrderInModule if any)</li>
 * </ul>
 * </p>
 * <p>To achieve the sorting, the comparator must hold the index values for all the beans.</p>
 */
public class ModuleBeanOrderComparator implements Comparator<Object>
{
	/**
	 * If no order specified, the default is less than lowest priority so it would be
	 * possible to define beans that need to come after all module beans.
	 */
	private static final int DEFAUL_GLOBAL_ORDER = Ordered.LOWEST_PRECEDENCE - 1000;

	private Map<Object, Integer> moduleIndexMap = new HashMap<>();
	private Map<Object, Integer> orderMap = new HashMap<>();
	private Map<Object, Integer> orderInModuleMap = new HashMap<>();

	public void register( Object bean, int moduleIndex ) {
		moduleIndexMap.put( bean, moduleIndex );
		orderMap.put( bean, lookupOrder( bean ) );
		orderInModuleMap.put( bean, lookupOrderInModule( bean ) );
	}

	private int lookupOrder( Object bean ) {
		if ( bean instanceof Ordered ) {
			return ( (Ordered) bean ).getOrder();
		}
		if ( bean != null ) {
			Class<?> clazz = ( bean instanceof Class ? (Class<?>) bean : bean.getClass() );
			Order order = AnnotationUtils.findAnnotation( clazz, Order.class );
			if ( order != null ) {
				return order.value();
			}
		}
		return DEFAUL_GLOBAL_ORDER;
	}

	private int lookupOrderInModule( Object bean ) {
		if ( bean instanceof OrderedInModule ) {
			return ( (OrderedInModule) bean ).getOrderInModule();
		}
		if ( bean != null ) {
			Class<?> clazz = ( bean instanceof Class ? (Class<?>) bean : bean.getClass() );
			OrderInModule order = AnnotationUtils.findAnnotation( clazz, OrderInModule.class );
			if ( order != null ) {
				return order.value();
			}
		}
		return Ordered.LOWEST_PRECEDENCE;
	}

	@Override
	public int compare( Object left, Object right ) {
		Integer leftOrder = orderMap.get( left );
		Integer rightOrder = orderMap.get( right );

		int comparison = leftOrder.compareTo( rightOrder );

		if ( comparison == 0 ) {
			Integer leftModuleIndex = moduleIndexMap.get( left );
			Integer rightModuleIndex = moduleIndexMap.get( right );

			comparison = leftModuleIndex.compareTo( rightModuleIndex );

			if ( comparison == 0 ) {
				Integer leftOrderInModule = orderInModuleMap.get( left );
				Integer rightOrderInModule = orderInModuleMap.get( right );

				comparison = leftOrderInModule.compareTo( rightOrderInModule );
			}
		}

		return comparison;
	}

	/**
	 * Sorts the list of instances according to the comparator.
	 * @param beans List of instances to sort.
	 */
	public void sort( List<?> beans ) {
		if ( beans.size() > 1 ) {
			Collections.sort( beans, this );
		}
	}
}
