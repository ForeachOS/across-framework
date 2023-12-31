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
package test.installers;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * @author Arne Vandamme
 */
@ExtendWith(SpringExtension.class)
@DirtiesContext
@ContextConfiguration(classes = TestSingleDataSourceAndDefaultSchema.Config.class)
public class TestSingleDataSourceAndDefaultSchema extends AbstractInstallerDataSourceTest
{
	@Override
	protected String coreSchema() {
		return "PUBLIC";
	}

	@Override
	protected String dataSchema() {
		return "PUBLIC";
	}

	@Configuration
	static class Config
	{
		@Bean(name = { "acrossDataSource", "dataDataSource", MODULE_DS, MODULE_INSTALLER_DS })
		public EmbeddedDatabase acrossDataSource() {
			return new EmbeddedDatabaseBuilder()
					.setType( EmbeddedDatabaseType.HSQL )
					.setName( "core" )
					.addScript( "liquibase/hsqldb-create-test_schema.sql" )
					.build();
		}
	}
}
