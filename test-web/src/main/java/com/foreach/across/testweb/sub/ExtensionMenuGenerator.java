package com.foreach.across.testweb.sub;

import com.foreach.across.core.annotations.AcrossEventHandler;
import com.foreach.across.core.annotations.Refreshable;
import com.foreach.across.modules.web.menu.Menu;
import com.foreach.across.modules.web.menu.MenuConstructEvent;
import com.foreach.across.testweb.SpecificUiContext;
import net.engio.mbassy.listener.Handler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
@Refreshable
@AcrossEventHandler
public class ExtensionMenuGenerator
{
	@Autowired(required=false)
	private SpecificUiContext uiContext;

	@Handler
	public void onApplicationEvent( MenuConstructEvent menuConstructEvent ) {
		Menu menu = menuConstructEvent.getMenu();
		menu.addItem( "item 3" );
		menu.addItem( uiContext.toString() );
	}
}
