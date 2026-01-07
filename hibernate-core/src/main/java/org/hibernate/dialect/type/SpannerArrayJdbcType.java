/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.type;

import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.BasicPluralJavaType;
import org.hibernate.type.descriptor.jdbc.ArrayJdbcType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;
import org.hibernate.type.descriptor.jdbc.JdbcType;

import java.sql.SQLException;
import java.sql.Types;

public class SpannerArrayJdbcType extends ArrayJdbcType {

	public SpannerArrayJdbcType(JdbcType elementJdbcType) {
		super( elementJdbcType );
	}

	@Override
	protected <T, E> Object[] convertToArray(
			BasicBinder<T> binder,
			ValueBinder<E> elementBinder,
			BasicPluralJavaType<E> pluralJavaType,
			T value,
			WrapperOptions options) throws SQLException {

		// 1. Let Hibernate create the standard array (e.g., Integer[])
		Object[] originalArray = super.convertToArray( binder, elementBinder, pluralJavaType, value, options );

		// 2. Check if we need to widen the array to Long[] for Spanner
		if ( originalArray != null && requiresWideningToLong( getElementJdbcType() ) ) {
			return widenToLongArray( originalArray );
		}

		return originalArray;
	}

	private boolean requiresWideningToLong(JdbcType elementJdbcType) {
		int code = elementJdbcType.getJdbcTypeCode();
		// Spanner maps TINYINT, SMALLINT, and INTEGER to INT64.
		// The JDBC driver expects Long[] for INT64 arrays.
		return code == Types.INTEGER || code == Types.SMALLINT || code == Types.TINYINT;
	}

	private Object[] widenToLongArray(Object[] originalArray) {
		Long[] longArray = new Long[originalArray.length];
		for ( int i = 0; i < originalArray.length; i++ ) {
			Number number = (Number) originalArray[i];
			// Convert the Number (Integer, Short, Byte) to Long
			longArray[i] = number == null ? null : number.longValue();
		}
		return longArray;
	}
}
