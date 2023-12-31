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

package com.foreach.across.modules.web.menu;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Comparator;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

@SuppressWarnings("unchecked")
class TestPathBasedMenuBuilder
{
	private PathBasedMenuBuilder builder;

	@BeforeEach
	void createBuilder() {
		builder = Menu.builder();
	}

	@Test
	void javadocExample() {
		Menu menu = builder.item( "/my-group/item-1" ).and()
		                   .item( "/my-group" ).and()
		                   .item( "/my-item" ).and()
		                   .item( "/my-group/item-2" ).and()
		                   .item( "/my-other-group/single-item" ).and()
		                   .item( "/my-group:item-3" ).and()
		                   .build();

		assertThat( menu.size() ).isEqualTo( 4 );

		assertThat( menu.getItems().get( 0 ) )
				.satisfies( group -> {
					assertThat( group.getPath() ).isEqualTo( "/my-group" );
					assertThat( group.getItems().get( 0 ).getPath() ).isEqualTo( "/my-group/item-1" );
					assertThat( group.getItems().get( 1 ).getPath() ).isEqualTo( "/my-group/item-2" );
				} );
		assertThat( menu.getItems().get( 1 ).getPath() ).isEqualTo( "/my-group:item-3" );
		assertThat( menu.getItems().get( 2 ).getPath() ).isEqualTo( "/my-item" );
		assertThat( menu.getItems().get( 3 ).getPath() ).isEqualTo( "/my-other-group/single-item" );
	}

	@Test
	void itemBuilder() {
		builder.item( "child", "Child", "url" );
		Menu menu = builder.build();

		assertEquals( 1, menu.size() );
		verify( menu.getFirstItem(), "child", "Child", "url" );

		menu = builder.item( "child" ).title( "test" ).and().build();
		assertEquals( 1, menu.size() );
		verify( menu.getFirstItem(), "child", "test", "url" );
	}

	@Test
	@SuppressWarnings("unchecked")
	void comparatorOnItems() {
		Comparator<Menu> comparator = mock( Comparator.class );
		Comparator<Menu> otherComparator = mock( Comparator.class );

		Menu menu = builder.item( "no-comp" ).and()
		                   .item( "comp" ).comparator( comparator, true ).and()
		                   .item( "comp/sub" ).and()
		                   .item( "comp/sub2" ).comparator( otherComparator, false ).and()
		                   .build();

		assertThat( menu.getItemWithPath( "no-comp" ) )
				.satisfies( item -> {
					assertThat( item.getComparator() ).isNull();
					assertThat( item.isComparatorInheritable() ).isFalse();
				} );
		assertThat( menu.getItemWithPath( "comp" ) )
				.satisfies( item -> {
					assertThat( item.getComparator() ).isSameAs( comparator );
					assertThat( item.isComparatorInheritable() ).isTrue();
				} );
		assertThat( menu.getItemWithPath( "comp/sub" ) )
				.satisfies( item -> {
					assertThat( item.getComparator() ).isNull();
					assertThat( item.isComparatorInheritable() ).isFalse();
				} );
		assertThat( menu.getItemWithPath( "comp/sub2" ) )
				.satisfies( item -> {
					assertThat( item.getComparator() ).isSameAs( otherComparator );
					assertThat( item.isComparatorInheritable() ).isFalse();
				} );
	}

	@Test
	void requestMatcherAttributes() {
		Menu menu = builder.item( "item" ).matchRequests( "/test/", "/other" ).and().build();

		Collection<String> matchers = menu.getFirstItem().getAttribute( RequestMenuSelector.ATTRIBUTE_MATCHERS );
		assertEquals( 2, matchers.size() );
		assertTrue( matchers.contains( "/test/" ) );
		assertTrue( matchers.contains( "/other" ) );
	}

	@Test
	void buildItems() {
		builder.item( "1" ).title( "Title one" ).url( "URL one" );
		builder.item( "2" ).title( "Title two" ).url( "URL two" ).disable();
		builder.item( "3" ).title( "Title three" ).url( "URL three" ).order( 33 ).options( "option-one", "option-two" );
		builder.item( "4" ).title( "Title four" ).group( true )
		       .attribute( "attribute-one", "value" )
		       .attribute( "attribute-two", "value2" )
		       .attribute( attrs -> {
			       attrs.remove( "attribute-two" );
			       attrs.put( "attribute-three", "value3" );
		       } )
		       .options( "test-option", "test-option-2" )
		       .removeAttributes( "test-option-2" );

		Menu menu = builder.build();
		assertEquals( 4, menu.size() );

		Menu one = menu.getItems().get( 0 );
		assertEquals( "1", one.getPath() );
		assertEquals( "Title one", one.getTitle() );
		assertEquals( "URL one", one.getUrl() );
		assertFalse( one.isDisabled() );
		assertFalse( one.isGroup() );
		assertTrue( one.getAttributes().isEmpty() );

		Menu two = menu.getItems().get( 1 );
		assertEquals( "2", two.getPath() );
		assertEquals( "Title two", two.getTitle() );
		assertEquals( "URL two", two.getUrl() );
		assertTrue( two.isDisabled() );
		assertFalse( two.isGroup() );
		assertTrue( two.getAttributes().isEmpty() );

		Menu three = menu.getItems().get( 2 );
		assertEquals( "3", three.getPath() );
		assertEquals( "Title three", three.getTitle() );
		assertEquals( "URL three", three.getUrl() );
		assertFalse( three.isDisabled() );
		assertFalse( three.isGroup() );
		assertEquals( 33, three.getOrder() );
		assertEquals( 2, three.getAttributes().size() );
		assertTrue( three.hasAttribute( "option-one" ) );
		assertTrue( three.hasAttribute( "option-two" ) );

		Menu four = menu.getItems().get( 3 );
		assertEquals( "4", four.getPath() );
		assertEquals( "Title four", four.getTitle() );
		assertFalse( four.hasUrl() );
		assertFalse( four.isDisabled() );
		assertTrue( four.isGroup() );
		assertEquals( 3, four.getAttributes().size() );
		assertTrue( four.hasAttribute( "test-option" ) );
		assertFalse( four.hasAttribute( "test-option-2" ) );
		assertEquals( "value", four.getAttribute( "attribute-one" ) );
		assertFalse( four.hasAttribute( "attribute-two" ) );
		assertEquals( "value3", four.getAttribute( "attribute-three" ) );
	}

	@Test
	void concatenatingItems() {
		builder.root( "home" ).title( "Home" ).url( "/home" )
		       .and()
		       .group( "/news", "News" ).url( "http://news-section" )
		       .and()
		       .item( "/news/international/australia", "Australia", "aussies" )
		       .and()
		       .item( "/news/international", "International news", "intnl" )
		       .and()
		       .item( "/news/national", "National news", "national" )
		       .and()
		       .item( "/news/nationalAlmost", "Almost national news", "nationalAlmost" );

		Menu cursor = builder.build();
		assertNotNull( cursor );

		cursor = verify( cursor, "home", "Home", "/home" );
		cursor = verify( cursor.getFirstItem(), "/news", "News", "http://news-section" );

		Menu news = cursor;
		assertTrue( news.isGroup() );
		cursor = verify( news.getItems().get( 0 ), "/news/international", "International news", "intnl" );
		verify( cursor.getFirstItem(), "/news/international/australia", "Australia", "aussies" );

		verify( news.getItems().get( 1 ), "/news/national", "National news", "national" );
		verify( news.getItems().get( 2 ), "/news/nationalAlmost", "Almost national news", "nationalAlmost" );
	}

	@Test
	void changeItemPathOfUnknownItemsDoesNothing() {
		assertThat( builder.changeItemPath( "/one", "/two" ).build() ).isNotNull();
		assertThat( builder.changeItemPath( "/one", "/two", false ).build() ).isNotNull();
	}

	@Test
	void changeItemPathOfSeparateItems() {
		builder.item( "/list/servers", "Servers" ).and()
		       .item( "/list/laptops", "Laptops", "/custom/url" );

		Menu menu = builder.build();
		assertEquals( 2, menu.size() );

		verify( menu.getItems().get( 0 ), "/list/laptops", "Laptops", "/custom/url" );
		verify( menu.getItems().get( 1 ), "/list/servers", "Servers", "/list/servers" );

		builder.group( "/myinfra", "Infra" ).and()
		       .changeItemPath( "/list/laptops", "/myinfra/laptops" )
		       .changeItemPath( "/list/servers", "/myinfra/servers" );

		menu = builder.build();
		assertEquals( 1, menu.size() );

		verify( menu.getItems().get( 0 ), "/myinfra", "Infra", "/myinfra" );
		verify( menu.getItems().get( 0 ).getItems().get( 0 ), "/myinfra/laptops", "Laptops", "/custom/url" );
		verify( menu.getItems().get( 0 ).getItems().get( 1 ), "/myinfra/servers", "Servers", "/myinfra/servers" );
	}

	@Test
	void changeItemPathOfItemAndChildren() {
		Menu menu = builder.group( "/group" ).title( "Group title" ).and()
		                   .item( "/group/one" ).and()
		                   .item( "group/two" ).and()
		                   .changeItemPath( "/group", "/new-group" )
		                   .build();

		assertThat( menu.size() ).isEqualTo( 2 );
		assertThat( menu.getItemWithPath( "/new-group" ) )
				.isNotNull()
				.satisfies( group -> {
					assertThat( group.getTitle() ).isEqualTo( "Group title" );
					assertThat( group.size() ).isEqualTo( 1 );
					assertThat( group.getFirstItem() ).isSameAs( menu.getItemWithPath( "/new-group/one" ) );
				} );
		assertThat( menu.getItemWithPath( "group/two" ) ).isNotNull();
		assertThat( menu.getItemWithPath( "/group" ) ).isNull();
		assertThat( menu.getItemWithPath( "/group/one" ) ).isNull();
	}

	@Test
	void changeItemPathOfItemOnly() {
		Menu menu = builder.group( "/group" ).title( "Group title" ).and()
		                   .item( "/group/one" ).and()
		                   .item( "group/two" ).and()
		                   .changeItemPath( "/group", "/new-group", false )
		                   .build();

		assertThat( menu.size() ).isEqualTo( 3 );
		assertThat( menu.getItemWithPath( "/new-group" ) )
				.isNotNull()
				.satisfies( group -> {
					assertThat( group.getTitle() ).isEqualTo( "Group title" );
					assertThat( group.isEmpty() ).isTrue();
				} );
		assertThat( menu.getItemWithPath( "group/two" ) ).isNotNull();
		assertThat( menu.getItemWithPath( "/group/one" ) ).isNotNull();
		assertThat( menu.getItemWithPath( "/group" ) ).isNull();
	}

	@Test
	void changeItemPathOfChildrenOnly() {
		Menu menu = builder.group( "group" ).title( "Group title" ).and()
		                   .item( "group/one" ).and()
		                   .item( "group/two" ).and()
		                   .changeItemPath( "group/", "new-group/" )
		                   .build();

		assertThat( menu.size() ).isEqualTo( 3 );
		assertThat( menu.getItemWithPath( "group" ) )
				.isNotNull()
				.satisfies( group -> {
					assertThat( group.getTitle() ).isEqualTo( "Group title" );
					assertThat( group.isEmpty() ).isTrue();
				} );
		assertThat( menu.getItemWithPath( "new-group/two" ) ).isNotNull();
		assertThat( menu.getItemWithPath( "new-group/one" ) ).isNotNull();
		assertThat( menu.getItemWithPath( "group/one" ) ).isNull();
		assertThat( menu.getItemWithPath( "group/two" ) ).isNull();
	}

	@Test
	void changePathOfItemWithChildrenReturnsSameItem() {
		Menu menu = builder.group( "/group" ).url( "Group url" ).and()
		                   .item( "/group/one" ).and()
		                   .item( "group/two" ).and()
		                   .item( "/group" ).changePathTo( "/new-group" ).title( "Group title" ).and()
		                   .build();

		assertThat( menu.size() ).isEqualTo( 2 );
		assertThat( menu.getItemWithPath( "/new-group" ) )
				.isNotNull()
				.satisfies( group -> {
					assertThat( group.getTitle() ).isEqualTo( "Group title" );
					assertThat( group.getUrl() ).isEqualTo( "Group url" );
					assertThat( group.size() ).isEqualTo( 1 );
					assertThat( group.getFirstItem() ).isSameAs( menu.getItemWithPath( "/new-group/one" ) );
				} );
		assertThat( menu.getItemWithPath( "group/two" ) ).isNotNull();
		assertThat( menu.getItemWithPath( "/group" ) ).isNull();
		assertThat( menu.getItemWithPath( "/group/one" ) ).isNull();
	}

	@Test
	void changePathOfItemOnlyReturnsSameItem() {
		Menu menu = builder.group( "/group" ).url( "Group url" ).and()
		                   .item( "/group/one" ).and()
		                   .item( "group/two" ).and()
		                   .item( "/group" ).changePathTo( "/new-group", false ).title( "Group title" ).and()
		                   .build();

		assertThat( menu.size() ).isEqualTo( 3 );
		assertThat( menu.getItemWithPath( "/new-group" ) )
				.isNotNull()
				.satisfies( group -> {
					assertThat( group.getTitle() ).isEqualTo( "Group title" );
					assertThat( group.getUrl() ).isEqualTo( "Group url" );
					assertThat( group.isEmpty() ).isTrue();
				} );
		assertThat( menu.getItemWithPath( "group/two" ) ).isNotNull();
		assertThat( menu.getItemWithPath( "/group/one" ) ).isNotNull();
		assertThat( menu.getItemWithPath( "/group" ) ).isNull();
	}

	@Test
	void removeItemAndChildrenOnItem() {
		Menu menu = builder.group( "/group" ).title( "Group title" ).and()
		                   .item( "/group/one" ).and()
		                   .item( "group/two" ).and()
		                   .item( "/group" ).remove( true )
		                   .build();

		assertThat( menu.size() ).isEqualTo( 1 );
		assertThat( menu.getItemWithPath( "group/two" ) ).isNotNull();
	}

	@Test
	void removeItemOnlyOnItem() {
		Menu menu = builder.group( "/group" ).title( "Group title" ).and()
		                   .item( "/group/one" ).and()
		                   .item( "group/two" ).and()
		                   .item( "/group" ).remove( false )
		                   .build();

		assertThat( menu.size() ).isEqualTo( 2 );
		assertThat( menu.getItemWithPath( "group/two" ) ).isNotNull();
		assertThat( menu.getItemWithPath( "/group/one" ) ).isNotNull();
	}

	@Test
	void removeItemAndChildren() {
		Menu menu = builder.group( "/group" ).title( "Group title" ).and()
		                   .item( "/group/one" ).and()
		                   .item( "group/two" ).and()
		                   .removeItems( "/group", true )
		                   .build();

		assertThat( menu.size() ).isEqualTo( 1 );
		assertThat( menu.getItemWithPath( "group/two" ) ).isNotNull();
	}

	@Test
	void removeItemOnly() {
		Menu menu = builder.group( "/group" ).title( "Group title" ).and()
		                   .item( "/group/one" ).and()
		                   .item( "group/two" ).and()
		                   .removeItems( "/group", false )
		                   .build();

		assertThat( menu.size() ).isEqualTo( 2 );
		assertThat( menu.getItemWithPath( "group/two" ) ).isNotNull();
		assertThat( menu.getItemWithPath( "/group/one" ) ).isNotNull();
	}

	@Test
	void removeChildrenOnly() {
		Menu menu = builder.group( "/group" ).title( "Group title" ).and()
		                   .item( "/group/one" ).and()
		                   .item( "/group/two" ).and()
		                   .removeItems( "/group/", true )
		                   .build();

		assertThat( menu.size() ).isEqualTo( 1 );
		assertThat( menu.getItemWithPath( "/group" ) ).isNotNull().satisfies( Menu::isEmpty );
	}

	@Test
	void optionalItemDoesNothingIfOriginalDoesNotExist() {
		Menu menu = builder.optionalItem( "/test" ).title( "Updated test" ).and()
		                   .item( "/other/child" ).and()
		                   .item( "/my/child" ).and()
		                   .optionalItem( "/my" ).changePathTo( "/updated" ).and()
		                   .optionalItem( "/other" ).remove( true )
		                   .build();

		assertThat( menu.size() ).isEqualTo( 2 );
		assertThat( menu.getItemWithPath( "/other/child" ) ).isNotNull();
		assertThat( menu.getItemWithPath( "/my/child" ) ).isNotNull();
	}

	@Test
	void optionalItemModifiesExisting() {
		Menu menu = builder.item( "/test" ).title( "Original test" ).and()
		                   .group( "/other" ).and()
		                   .optionalItem( "/test" ).title( "Updated test" ).and()
		                   .item( "/my" ).and()
		                   .optionalItem( "/my" ).changePathTo( "/my/updated/child" ).and()
		                   .optionalItem( "/other" ).remove( true )
		                   .build();

		assertThat( menu.size() ).isEqualTo( 2 );
		assertThat( menu.getItemWithPath( "/my/updated/child" ) ).isNotNull();
		assertThat( menu.getItemWithPath( "/test" ) )
				.isNotNull()
				.satisfies( item -> assertThat( item.getTitle() ).isEqualTo( "Updated test" ) );
	}

	@Test
	@SuppressWarnings("all")
	public void consumersAfterInitialConfiguration() {
		Consumer<PathBasedMenuBuilder> first = mock( Consumer.class );
		Consumer<PathBasedMenuBuilder> second = mock( Consumer.class );

		builder.andThen( first ).andThen( second ).build();

		InOrder inOrder = Mockito.inOrder( first, second );
		inOrder.verify( first ).accept( builder );
		inOrder.verify( second ).accept( builder );
	}

	@Test
	@SuppressWarnings("all")
	public void consumersCanAddMoreConsumersThatWillBeCalledAfterTheInitialSet() {
		Consumer<PathBasedMenuBuilder> third = mock( Consumer.class );
		Consumer<PathBasedMenuBuilder> fourth = menu -> menu.item( "test" ).url( "4" ).attribute( "4", true );
		Consumer<PathBasedMenuBuilder> first = menu -> menu.item( "test" ).url( "1" ).attribute( "1", true ).and().andThen( third ).andThen( fourth );
		Consumer<PathBasedMenuBuilder> second = mock( Consumer.class );

		Menu menu = builder.andThen( first ).andThen( second ).item( "test" ).title( "Test title" ).and().build();

		InOrder inOrder = Mockito.inOrder( second, third );
		inOrder.verify( second ).accept( builder );
		inOrder.verify( third ).accept( builder );

		Menu firstItem = menu.getFirstItem();
		assertThat( firstItem.getPath() ).isEqualTo( "test" );
		assertThat( firstItem.getTitle() ).isEqualTo( "Test title" );
		assertThat( firstItem.getUrl() ).isEqualTo( "4" );
		assertThat( (Boolean) firstItem.getAttribute( "1" ) ).isTrue();
		assertThat( (Boolean) firstItem.getAttribute( "4" ) ).isTrue();
	}

	@Test
	void moveToWithItemToOriginalPlaceRetainsProperties() {
		builder.item( "/item/a" ).title( "Item A" ).disable().order( 33 );

		builder.group( "/item" ).and()
		       .changeItemPath( "/item/a", "/item/b" )
		       .changeItemPath( "/item/b", "/item/a" );
		Menu menu = builder.build();

		verify( menu.getItems().get( 0 ), "/item", null, "/item" );
		verify( menu.getItems().get( 0 ).getItems().get( 0 ), "/item/a", "Item A", "/item/a" );
		assertEquals( 33, menu.getItems().get( 0 ).getItems().get( 0 ).getOrder() );
	}

	@Test
	void changeItemPathForSubtreeMovesAllPrefixingItems() {
		builder.item( "/list/servers", "Servers" ).and()
		       .item( "/list/laptops", "Laptops" ).and()
		       .item( "/list/screens", "Screens" ).and()
		       .item( "/listSomethingElse", "List of something else" );

		Menu menu = builder.build();
		assertEquals( 4, menu.size() );

		builder.group( "/allstuff", "All Stuff" ).and().changeItemPath( "/list", "/allstuff" );

		menu = builder.build();
		assertEquals( 2, menu.size() );

		verify( menu.getItems().get( 0 ), "/allstuff", "All Stuff", "/allstuff" );
		verify( menu.getItems().get( 1 ), "/listSomethingElse", "List of something else", "/listSomethingElse" );

		verify( menu.getItems().get( 0 ).getItems().get( 0 ), "/allstuff/laptops", "Laptops", "/allstuff/laptops" );
		verify( menu.getItems().get( 0 ).getItems().get( 1 ), "/allstuff/screens", "Screens", "/allstuff/screens" );
		verify( menu.getItems().get( 0 ).getItems().get( 2 ), "/allstuff/servers", "Servers", "/allstuff/servers" );
	}

	@Test
	void movingOnlyMovesNestedPath() {
		builder.item( "/list/business", "Business" ).and()
		       .item( "/list/business/details", "Details" ).and()
		       .item( "/list/businessDescription", "Business description" );

		Menu menu = builder.build();
		assertEquals( 2, menu.size() );

		verify( menu.getItems().get( 0 ), "/list/business", "Business", "/list/business" );
		verify( menu.getItems().get( 0 ).getFirstItem(), "/list/business/details", "Details", "/list/business/details" );
		verify( menu.getItems().get( 1 ), "/list/businessDescription", "Business description", "/list/businessDescription" );

		builder.group( "/mybusiness", "My Business" ).and()
		       .changeItemPath( "/list/business", "/mybusiness/business" );

		menu = builder.build();
		assertEquals( 2, menu.size() );

		verify( menu.getItems().get( 0 ), "/list/businessDescription", "Business description", "/list/businessDescription" );
		verify( menu.getItems().get( 1 ), "/mybusiness", "My Business", "/mybusiness" );
		verify( menu.getItems().get( 1 ).getItems().get( 0 ), "/mybusiness/business", "Business", "/mybusiness/business" );
		verify( menu.getItems().get( 1 ).getItems().get( 0 ).getFirstItem(), "/mybusiness/business/details", "Details", "/mybusiness/business/details" );
	}

	@Test
	void itemProcessors() {
		builder = new PathBasedMenuBuilder( new PrefixTitleProcessor( "processed:" ) );

		builder.item( "test1", "One", "urlOne" )
		       .and()
		       .withProcessor(
				       new PrefixTitleProcessor( "prefixed:" ),
				       builder ->
						       builder.item( "test2", "Two", "urlTwo" )
						              .and()
						              .item( "test3", "Three", "urlThree" )
		       )
		       .item( "test4", "Four", "urlFour" );

		Menu menu = builder.build();
		assertEquals( 4, menu.size() );

		assertEquals( "processed:One", menu.getItemWithPath( "test1" ).getTitle() );
		assertEquals( "prefixed:Two", menu.getItemWithPath( "test2" ).getTitle() );
		assertEquals( "prefixed:Three", menu.getItemWithPath( "test3" ).getTitle() );
		assertEquals( "processed:Four", menu.getItemWithPath( "test4" ).getTitle() );
	}

	@Test
	void buildIntoExisting() {
		Menu existing = new Menu( "one" );
		existing.addItem( "two" ).addItem( "three" );

		builder.item( "four" ).and().build( existing.getFirstItem() );

		assertEquals( "one", existing.getPath() );
		assertEquals( "two", existing.getFirstItem().getPath() );
		assertEquals( "four", existing.getFirstItem().getFirstItem().getPath() );
		assertNull( existing.getItemWithPath( "three" ) );
	}

	@Test
	void mergeIntoExistingWithoutRoot() {
		Menu existing = new Menu( "one" );
		existing.addItem( "two" ).addItem( "three" );

		builder.item( "four" ).and().merge( existing.getFirstItem() );

		assertEquals( "one", existing.getPath() );
		assertEquals( "two", existing.getFirstItem().getPath() );
		assertEquals( "three", existing.getFirstItem().getFirstItem().getPath() );
		assertEquals( "four", existing.getFirstItem().getItems().get( 1 ).getPath() );
	}

	@Test
	@SuppressWarnings("unchecked")
	void mergeIntoExistingWithRoot() {
		Menu existing = new Menu( "one" );
		assertThat( existing.getName() ).isEqualTo( "one" );
		assertThat( existing.getPath() ).isEqualTo( "one" );

		Comparator<Menu> comparator = mock( Comparator.class );
		builder.root( "/my-root" ).title( "My title" ).comparator( comparator, true ).and().merge( existing );

		assertThat( existing.getName() ).isEqualTo( "one" );
		assertThat( existing.getTitle() ).isEqualTo( "My title" );
		assertThat( existing.getPath() ).isEqualTo( "/my-root" );
		assertThat( existing.getComparator() ).isSameAs( comparator );
		assertThat( existing.isComparatorInheritable() ).isTrue();
	}

	private Menu verify( Menu item, String path, String title, String url ) {
		assertNotNull( item );
		assertEquals( path, item.getPath() );
		assertEquals( title, item.getTitle() );
		assertEquals( url, item.getUrl() );

		return item;
	}

	/**
	 * Prefix the menu title with a string.
	 */
	public static class PrefixTitleProcessor implements MenuItemBuilderProcessor
	{
		private final String prefix;

		PrefixTitleProcessor( String prefix ) {
			this.prefix = prefix;
		}

		@Override
		public Menu process( Menu menu ) {
			menu.setTitle( prefix + menu.getTitle() );
			return menu;
		}
	}
}
