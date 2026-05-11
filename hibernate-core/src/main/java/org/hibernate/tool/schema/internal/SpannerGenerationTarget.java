/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.tool.schema.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.engine.jdbc.internal.FormatStyle;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.resource.transaction.spi.DdlTransactionIsolator;
import org.hibernate.tool.schema.spi.CommandAcceptanceException;
import org.hibernate.tool.schema.spi.GenerationTarget;
import org.hibernate.tool.schema.spi.ScriptSourceInput;

/**
 * A {@link GenerationTarget} for Spanner that batches DDL statements.
 */
public class SpannerGenerationTarget implements GenerationTarget {

	private final DdlTransactionIsolator ddlTransactionIsolator;
	private final List<String> commands = new ArrayList<>();

	public SpannerGenerationTarget(DdlTransactionIsolator ddlTransactionIsolator) {
		this.ddlTransactionIsolator = ddlTransactionIsolator;
	}

	private SqlStatementLogger getSqlStatementLogger() {
		return ddlTransactionIsolator.getJdbcContext().getSqlStatementLogger();
	}

	@Override
	public void prepare() {
	}

	@Override
	public void beforeScript(ScriptSourceInput scriptSource) {
	}

	@Override
	public void accept(String command) {
		System.out.println("SpannerGenerationTarget.accept: " + command);
		if ( isDdl( command ) ) {
			commands.add( command );
		}
		else {
			// Non-DDL (likely DML from import scripts)
			// 1. Flush any pending DDL batch first
			flushDdlBatch();
			// 2. Execute this command immediately
			executeImmediately( command );
		}
	}

	private boolean isDdl(String command) {
		String trimmed = command.trim().toUpperCase(java.util.Locale.ROOT);
		return trimmed.startsWith("CREATE") || trimmed.startsWith("DROP") || trimmed.startsWith("ALTER");
	}

	private void flushDdlBatch() {
		if ( !commands.isEmpty() ) {
			final SqlStatementLogger statementLogger = getSqlStatementLogger();
			try {
				final Connection conn = ddlTransactionIsolator.getIsolatedConnection();
				try (Statement stmt = conn.createStatement()) {
					statementLogger.logStatement( "START BATCH DDL", FormatStyle.NONE.getFormatter() );
					stmt.execute( "START BATCH DDL" );

					for ( String command : commands ) {
						statementLogger.logStatement( command, FormatStyle.NONE.getFormatter() );
						stmt.execute( command );
					}

					statementLogger.logStatement( "RUN BATCH", FormatStyle.NONE.getFormatter() );
					stmt.execute( "RUN BATCH" );
				}
				commands.clear();
			}
			catch (SQLException e) {
				throw new CommandAcceptanceException( "Error executing Spanner DDL batch", e );
			}
		}
	}

	private void executeImmediately(String command) {
		final SqlStatementLogger statementLogger = getSqlStatementLogger();
		statementLogger.logStatement( command, FormatStyle.NONE.getFormatter() );
		try {
			final Connection conn = ddlTransactionIsolator.getIsolatedConnection();
			try (Statement stmt = conn.createStatement()) {
				stmt.execute( command );
			}
		}
		catch (SQLException e) {
			throw new CommandAcceptanceException( "Error executing statement immediately", e );
		}
	}

	@Override
	public void release() {
		flushDdlBatch();
		ddlTransactionIsolator.release();
	}
}
