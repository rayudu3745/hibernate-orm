/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.type;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.JsonArrayJdbcType;

/**
 * Spanner requires binding JSON_ARRAY values using a specific vendor type (100011).
 */
public class SpannerJsonArrayJdbcType extends JsonArrayJdbcType {

	private static final int VENDOR_TYPE_NUMBER = 100011;

	public static final SpannerJsonArrayJdbcType INSTANCE = new SpannerJsonArrayJdbcType( null );

	public SpannerJsonArrayJdbcType(JdbcType elementJdbcType) {
		super( elementJdbcType );
	}

	@Override
	public <X> ValueBinder<X> getBinder(JavaType<X> javaType) {
		return new BasicBinder<>( javaType, this ) {
			@Override
			protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options) throws SQLException {
				final String json = SpannerJsonArrayJdbcType.this.toString( value, getJavaType(), options );
				st.setObject( index, json, VENDOR_TYPE_NUMBER );
			}

			@Override
			protected void doBind(CallableStatement st, X value, String name, WrapperOptions options) throws SQLException {
				final String json = SpannerJsonArrayJdbcType.this.toString( value, getJavaType(), options );
				st.setObject( name, json, VENDOR_TYPE_NUMBER );
			}

			@Override
			protected void doBindNull(PreparedStatement st, int index, WrapperOptions options) throws SQLException {
				st.setNull( index, VENDOR_TYPE_NUMBER );
			}

			@Override
			protected void doBindNull(CallableStatement st, String name, WrapperOptions options) throws SQLException {
				st.setNull( name, VENDOR_TYPE_NUMBER );
			}
		};
	}
}
