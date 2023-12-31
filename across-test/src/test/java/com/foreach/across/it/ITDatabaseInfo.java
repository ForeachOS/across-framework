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

package com.foreach.across.it;

import com.foreach.across.core.database.DatabaseInfo;
import com.foreach.across.test.AcrossTestConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Arne Vandamme
 */
@ExtendWith(SpringExtension.class)
@DirtiesContext
@ContextConfiguration
public class ITDatabaseInfo
{
	private static final Logger LOG = LoggerFactory.getLogger( ITDatabaseInfo.class );

	@Autowired
	private DataSource dataSource;

	@Test
	public void dataSourceShouldBeOneOfKnown() {
		DatabaseInfo databaseInfo = DatabaseInfo.retrieve( dataSource );

		assertNotNull( databaseInfo );
		assertNotNull( databaseInfo.getProductName() );
		assertNotNull( databaseInfo.getProductVersion() );

		LOG.info( "DatabaseInfo detected database {} - version: {}", databaseInfo.getProductName(),
		          databaseInfo.getProductVersion() );

		assertTrue(
				databaseInfo.isHsql()
						|| databaseInfo.isH2()
						|| databaseInfo.isOracle()
						|| databaseInfo.isSqlServer()
						|| databaseInfo.isMySQL()
						|| databaseInfo.isPostgres()
		);
	}

	@AcrossTestConfiguration
	static class Config
	{
	}
}
