/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.temptable;

import org.hibernate.dialect.Dialect;

public class SpannerTemporaryTableExporter extends StandardTemporaryTableExporter {

	private final Dialect dialect;

	public SpannerTemporaryTableExporter(Dialect dialect) {
		super(dialect);
		this.dialect = dialect;
	}

	@Override
	public String getSqlCreateCommand(TemporaryTable temporaryTable) {

		final TemporaryTableStrategy temporaryTableStrategy = dialect.getPersistentTemporaryTableStrategy();

		if (temporaryTableStrategy == null) {
			throw new IllegalStateException("Dialect returned null for PersistentTemporaryTableStrategy");
		}

		final StringBuilder buffer = new StringBuilder(dialect.getCreateTableString()).append(' ');
		buffer.append(temporaryTable.getQualifiedTableName());
		buffer.append('(');

		boolean hasColumns = false;

		// Generate Column Definitions
		for (TemporaryTableColumn column : temporaryTable.getColumnsForExport()) {
			hasColumns = true;
			buffer.append(column.getColumnName()).append(' ');
			final int sqlTypeCode = column.getJdbcMapping().getJdbcType().getDdlTypeCode();
			final String databaseTypeName = column.getSqlTypeDefinition();

			buffer.append(databaseTypeName);

			final String columnAnnotation = temporaryTableStrategy.getCreateTemporaryTableColumnAnnotation(sqlTypeCode);
			if (!columnAnnotation.isEmpty()) {
				buffer.append(' ').append(columnAnnotation);
			}

			if (temporaryTableStrategy.supportsTemporaryTableNullConstraint()) {
				if (column.isNullable()) {
					final String nullColumnString = dialect.getNullColumnString(databaseTypeName);
					if (!databaseTypeName.contains(nullColumnString)) {
						buffer.append(nullColumnString);
					}
				} else {
					buffer.append(" not null");
				}
			}
			buffer.append(", ");
		}

		// Remove the last comma if columns existed
		if (hasColumns) {
			buffer.setLength(buffer.length() - 2);
		}

		// Close the table column definition parenthesis
		buffer.append(')');

		// Spanner needs Primary Key OUTSIDE the parentheses
		if (temporaryTableStrategy.supportsTemporaryTablePrimaryKey()) {
			StringBuilder pkBuffer = new StringBuilder();
			boolean first = true;
			for (TemporaryTableColumn column : temporaryTable.getColumnsForExport()) {
				if (column.isPrimaryKey()) {
					if (first) {
						pkBuffer.append(" primary key (");
						first = false;
					} else {
						pkBuffer.append(", ");
					}
					pkBuffer.append(column.getColumnName());
				}
			}

			if (!first) {
				pkBuffer.append(')');
				buffer.append(pkBuffer);
			}
		}
		return buffer.toString();
	}
}
