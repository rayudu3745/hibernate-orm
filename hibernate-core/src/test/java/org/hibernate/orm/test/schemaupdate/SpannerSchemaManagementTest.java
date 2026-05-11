/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.orm.test.schemaupdate;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.dialect.SpannerDialect;
import org.hibernate.testing.orm.junit.EntityManagerFactoryScope;
import org.hibernate.testing.orm.junit.Jpa;
import org.hibernate.testing.orm.junit.RequiresDialect;
import org.hibernate.testing.orm.junit.Setting;
import org.hibernate.tool.hbm2ddl.SchemaExport;
import org.hibernate.tool.hbm2ddl.SchemaUpdate;
import org.hibernate.tool.schema.TargetType;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;


@RequiresDialect(SpannerDialect.class)
@Jpa(
		annotatedClasses = {SpannerSchemaManagementTest.SimpleEntityV1.class},
		properties = {
				@Setting(name = "hibernate.hbm2ddl.import_files", value = "org/hibernate/orm/test/schemaupdate/import-spanner-ddl-batch-test.sql"),
				@Setting(name = "hibernate.schema_management_tool", value = "org.hibernate.tool.schema.internal.SpannerSchemaManagementTool"),
				@Setting(name = "hibernate.hbm2ddl.auto", value = "none") // We control it manually
		}
)
public class SpannerSchemaManagementTest {

	// V1 mapping
	@Entity(name = "SimpleEntity")
	@Table(name = "simple_entity")
	public static class SimpleEntityV1 {
		@Id
		private Long id;
		private String name;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	// V2 mapping with extra column
	@Entity(name = "SimpleEntity")
	@Table(name = "simple_entity")
	public static class SimpleEntityV2 {
		@Id
		private Long id;
		private String name;
		private String newColumn;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getNewColumn() {
			return newColumn;
		}

		public void setNewColumn(String newColumn) {
			this.newColumn = newColumn;
		}
	}

	@Test
	public void testCreate(EntityManagerFactoryScope scope) {
		MetadataImplementor metadata = buildMetadata( scope, SimpleEntityV1.class );
		SchemaExport schemaExport = new SchemaExport();
		schemaExport.setHaltOnError( true );

		// Verify batching in logs for create
		schemaExport.create( EnumSet.of( TargetType.DATABASE ), metadata );
	}

	@Test
	public void testDrop(EntityManagerFactoryScope scope) {
		MetadataImplementor metadata = buildMetadata( scope, SimpleEntityV1.class );
		SchemaExport schemaExport = new SchemaExport();
		schemaExport.setHaltOnError( true );

		// Ensure table exists first
		schemaExport.create( EnumSet.of( TargetType.DATABASE ), metadata );

		// Verify batching in logs for drop
		schemaExport.drop( EnumSet.of( TargetType.DATABASE ), metadata );
	}

	@Test
	public void testUpdate(EntityManagerFactoryScope scope) {
		SchemaExport schemaExport = new SchemaExport();
		schemaExport.setHaltOnError( true );

		// 1. Create with V1
		MetadataImplementor metadataV1 = buildMetadata( scope, SimpleEntityV1.class );
		schemaExport.create( EnumSet.of( TargetType.DATABASE ), metadataV1 );

		// 2. Update with V2
		MetadataImplementor metadataV2 = buildMetadata( scope, SimpleEntityV2.class );
		SchemaUpdate schemaUpdate = new SchemaUpdate();
		schemaUpdate.setHaltOnError( true );

		// Verify batching in logs for alter table
		schemaUpdate.execute( EnumSet.of( TargetType.DATABASE ), metadataV2 );
	}

	private MetadataImplementor buildMetadata(EntityManagerFactoryScope scope, Class<?>... annotatedClasses) {
		org.hibernate.engine.spi.SessionFactoryImplementor sessionFactory = (org.hibernate.engine.spi.SessionFactoryImplementor) scope.getEntityManagerFactory();
		org.hibernate.boot.registry.StandardServiceRegistry ssr = (org.hibernate.boot.registry.StandardServiceRegistry) sessionFactory.getServiceRegistry().getParentServiceRegistry();
		MetadataSources metadataSources = new MetadataSources( ssr );
		for ( Class<?> clazz : annotatedClasses ) {
			metadataSources.addAnnotatedClass( clazz );
		}
		return (MetadataImplementor) metadataSources.buildMetadata();
	}
}
