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
package com.foreach.across.modules.web.ui.elements.thymeleaf;

import com.foreach.across.modules.web.thymeleaf.HtmlIdStore;
import com.foreach.across.modules.web.thymeleaf.ProcessableModel;
import com.foreach.across.modules.web.thymeleaf.ThymeleafModelBuilder;
import com.foreach.across.modules.web.thymeleaf.ViewElementNodeFactory;
import com.foreach.across.modules.web.ui.elements.ViewElementGenerator;
import com.foreach.across.modules.web.ui.thymeleaf.ViewElementThymeleafBuilder;
import org.thymeleaf.context.ITemplateContext;

/**
 * @author Arne Vandamme
 */
public class ViewElementGeneratorThymeleafBuilder implements ViewElementThymeleafBuilder<ViewElementGenerator<?, ?>>
{
	@Override
	public void writeModel( ViewElementGenerator<?, ?> generator, ThymeleafModelBuilder writer ) {
		HtmlIdStore idStore = HtmlIdStore.fetch( writer.getTemplateContext() );

		generator.forEach( child -> {
			if ( child != null ) {
				if ( !generator.isBuilderItemTemplate() ) {
					idStore.increaseLevel();

					try {
						writer.addViewElement( child );
					}
					finally {
						idStore.decreaseLevel();
					}
				}
				else {
					writer.addViewElement( child );
				}
			}
		} );
	}

	@Override
	public ProcessableModel buildModel( ViewElementGenerator<?, ?> container,
	                                    ITemplateContext context,
	                                    ViewElementNodeFactory componentElementProcessor ) {
//
//		IModel model = context.getModelFactory().createModel();
//		HtmlIdStore originalIdStore = HtmlIdStore.fetch( context );
//
//		try {
//			for ( ViewElement child : container ) {
//				if ( child != null ) {
//					if ( !container.isBuilderItemTemplate() ) {
//						HtmlIdStore.store( originalIdStore.createNew(), context );
//					}
//
//					ProcessableModel processableModel = componentElementProcessor.buildModel( child, context );
//					model.addModel( processableModel.getModel() );
//				}
//			}
//		}
//		finally {
//			// Put back the original id store
//			HtmlIdStore.store( originalIdStore, context );
//
//		}
		return new ProcessableModel( null, true );
	}
}
