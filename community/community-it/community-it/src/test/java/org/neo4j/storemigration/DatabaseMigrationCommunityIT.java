/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.storemigration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_LABEL;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAMESPACE_PROPERTY;
import static org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAME_PROPERTY;
import static org.neo4j.graphdb.schema.IndexType.LOOKUP;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_LABEL;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.ZippedStore;
import org.neo4j.kernel.ZippedStoreCommunity;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class DatabaseMigrationCommunityIT extends DatabaseMigrationITBase {

    public static Stream<Arguments> migrations() {
        List<Arguments> permutations = new ArrayList<>();

        // Stores with special node label index
        permutations.add(Arguments.arguments(ZippedStoreCommunity.AF430_V42_INJECTED_NLI, "aligned"));
        permutations.add(Arguments.arguments(ZippedStoreCommunity.AF430_V43D4_PERSISTED_NLI, "aligned"));
        // 4.3 stores
        permutations.add(Arguments.arguments(ZippedStoreCommunity.SF430_V43D4_ALL_NO_BTREE, "standard"));
        permutations.add(Arguments.arguments(ZippedStoreCommunity.SF430_V43D4_ALL_NO_BTREE, "aligned"));
        permutations.add(Arguments.arguments(ZippedStoreCommunity.AF430_V43D4_ALL_NO_BTREE, "aligned"));
        // 4.4 stores
        permutations.add(Arguments.arguments(ZippedStoreCommunity.SF430_V44_ALL, "standard"));
        permutations.add(Arguments.arguments(ZippedStoreCommunity.SF430_V44_ALL, "aligned"));
        permutations.add(Arguments.arguments(ZippedStoreCommunity.AF430_V44_ALL, "aligned"));

        return permutations.stream();
    }

    public static Stream<SystemDbMigration> systemDbMigrations() {
        List<SystemDbMigration> permutations = new ArrayList<>();

        // Stores with different system dbs
        permutations.add(new SystemDbMigration(ZippedStoreCommunity.AF430_V42_INJECTED_NLI, false));
        permutations.add(new SystemDbMigration(ZippedStoreCommunity.AF430_V43D4_PERSISTED_NLI, false));
        permutations.add(new SystemDbMigration(ZippedStoreCommunity.AF430_V43D4_ALL_NO_BTREE, true));
        permutations.add(new SystemDbMigration(ZippedStoreCommunity.AF430_V44_ALL, true));

        return permutations.stream();
    }

    @ParameterizedTest
    @MethodSource("migrations")
    void shouldMigrateDatabase(ZippedStore zippedStore, String toRecordFormat)
            throws IOException, ConsistencyCheckIncompleteException {
        doShouldMigrateDatabase(zippedStore, toRecordFormat);
    }

    @ParameterizedTest
    @MethodSource("systemDbMigrations")
    void shouldMigrateSystemDatabase(SystemDbMigration systemDbMigration)
            throws IOException, ConsistencyCheckIncompleteException {
        doShouldMigrateSystemDatabase(systemDbMigration);
    }

    @Override
    protected TestDatabaseManagementServiceBuilder newDbmsBuilder(Path homeDir) {
        return new TestDatabaseManagementServiceBuilder(homeDir);
    }

    @Override
    protected void verifySystemDbSchema(GraphDatabaseService system, SystemDbMigration systemDbMigration) {
        try (Transaction tx = system.beginTx()) {
            List<ConstraintDefinition> constraints =
                    Iterables.asList(tx.schema().getConstraints());
            verifyHasUniqueConstraint(constraints, DATABASE_NAME_LABEL, NAME_PROPERTY, NAMESPACE_PROPERTY);
            verifyHasUniqueConstraint(constraints, DATABASE_LABEL, NAME_PROPERTY);
            verifyHasUniqueConstraint(constraints, USER_LABEL, "id");
            verifyHasUniqueConstraint(constraints, USER_LABEL, "name");
            assertThat(constraints).hasSize(4);

            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            verifyHasIndex(indexes, DATABASE_NAME_LABEL, NAME_PROPERTY, NAMESPACE_PROPERTY);
            verifyHasIndex(indexes, DATABASE_LABEL, NAME_PROPERTY);
            verifyHasIndex(indexes, USER_LABEL, "id");
            verifyHasIndex(indexes, USER_LABEL, "name");

            assertThat(indexes).anySatisfy(indexDefinition -> {
                assertThat(indexDefinition.getIndexType()).isEqualTo(LOOKUP);
                assertThat(indexDefinition.isNodeIndex()).isEqualTo(true);
            });

            if (systemDbMigration.hasRelationshipTypeIndex()) {
                assertThat(indexes).anySatisfy(indexDefinition -> {
                    assertThat(indexDefinition.getIndexType()).isEqualTo(LOOKUP);
                    assertThat(indexDefinition.isRelationshipIndex()).isEqualTo(true);
                });
                assertThat(indexes).hasSize(6);
            } else {
                assertThat(indexes).hasSize(5);
            }
            tx.commit();
        }
    }
}
