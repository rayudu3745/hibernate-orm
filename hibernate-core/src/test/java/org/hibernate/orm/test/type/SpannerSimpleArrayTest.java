/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.orm.test.type;

import java.util.Arrays;
import java.util.List;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.testing.orm.junit.DomainModel;
import org.hibernate.testing.orm.junit.SessionFactory;
import org.hibernate.testing.orm.junit.SessionFactoryScope;
import org.hibernate.type.SqlTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@DomainModel(annotatedClasses = SpannerSimpleArrayTest.TestEntity.class)
@SessionFactory
public class SpannerSimpleArrayTest {

	@AfterEach
	public void tearDown(SessionFactoryScope scope) {
		scope.inTransaction( session -> {
			session.createMutationQuery( "delete from TestEntity" ).executeUpdate();
		} );
	}

	@Test
	public void testInsertAndSelect(SessionFactoryScope scope) {
		Integer[] arrayData = { 1, 2, 3, null };
		List<Integer> listData = Arrays.asList( 10, 20, 30, null );

		// 1. Test Insert (Exercises SpannerArrayJdbcType#convertToArray)
		scope.inTransaction( session -> {
			TestEntity entity = new TestEntity( 1L, arrayData, listData );
			session.persist( entity );
		} );

		// 2. Test Select (Exercises SpannerArrayJdbcType#getExtractor)
		scope.inSession( session -> {
			TestEntity entity = session.find( TestEntity.class, 1L );

			// Verifies that Long[] from Spanner was correctly converted back to Integer[]
			assertThat( entity.getIntArray(), is( arrayData ) );

			// Verifies List support works via the same mechanism
			assertThat( entity.getIntList(), is( listData ) );
		} );
	}

	@Entity(name = "TestEntity")
	@Table(name = "test_entity")
	public static class TestEntity {

		@Id
		private Long id;

		// Maps to ARRAY<INT64> in Spanner.
		// Hibernate treats this as SqlTypes.ARRAY by default for array fields.
		private Integer[] intArray;

		// Maps to ARRAY<INT64> in Spanner.
		// We use @JdbcTypeCode(SqlTypes.ARRAY) to force mapping to a single column
		// instead of a separate element collection table.
		@JdbcTypeCode(SqlTypes.ARRAY)
		private List<Integer> intList;

		public TestEntity() {
		}

		public TestEntity(Long id, Integer[] intArray, List<Integer> intList) {
			this.id = id;
			this.intArray = intArray;
			this.intList = intList;
		}

		public Long getId() {
			return id;
		}

		public Integer[] getIntArray() {
			return intArray;
		}

		public List<Integer> getIntList() {
			return intList;
		}
	}
}
