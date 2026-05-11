/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.tool.schema.internal;

import org.hibernate.tool.schema.internal.exec.JdbcContext;
import org.hibernate.tool.schema.spi.GenerationTarget;

/**
 * A {@link org.hibernate.tool.schema.spi.SchemaManagementTool} for Spanner that uses
 * {@link SpannerGenerationTarget} to batch DDL statements.
 */
public class SpannerSchemaManagementTool extends HibernateSchemaManagementTool {

	@Override
	protected GenerationTarget buildDatabaseTarget(JdbcContext jdbcContext, boolean needsAutoCommit) {
		return new SpannerGenerationTarget( getDdlTransactionIsolator( jdbcContext ) );
	}
}
