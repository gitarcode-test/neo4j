/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.schema.vector;

import java.util.Locale;
import java.util.Set;
import org.neo4j.kernel.api.vector.VectorCandidate;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;

public class VectorSimilarityFunctions {
    // TODO VECTOR: perhaps some unrolling and/or vector api (when available) could be used here
    //              perhaps investigate some more accurate normalisation techniques

    abstract static class LuceneVectorSimilarityFunction implements VectorSimilarityFunction {
        @Override
        public String toString() {
            return name();
        }

        @Override
        public float compare(float[] vector1, float[] vector2) {
            return toLucene().compare(vector1, vector2);
        }

        abstract org.apache.lucene.index.VectorSimilarityFunction toLucene();
    }

    public static final VectorSimilarityFunction EUCLIDEAN = new LuceneVectorSimilarityFunction() {

        @Override
        public String name() {
            return "EUCLIDEAN";
        }

        @Override
        public org.apache.lucene.index.VectorSimilarityFunction toLucene() {
            return org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
        }

        @Override
        public float[] maybeToValidVector(VectorCandidate candidate) {
            final int dimensions;
            if (candidate == null || (dimensions = candidate.dimensions()) == 0) {
                return null;
            }

            final var vector = new float[dimensions];
            for (int i = 0; i < dimensions; i++) {
                final var element = candidate.floatElement(i);
                if (!Float.isFinite(element)) {
                    return null;
                }
                vector[i] = element;
            }
            return vector;
        }

        @Override
        public float[] toValidVector(VectorCandidate candidate) {
            final var vector = maybeToValidVector(candidate);
            if (vector == null) {
                throw new IllegalArgumentException("Vector must contain finite values. Provided: " + candidate);
            }
            return vector;
        }
    };

    public static final VectorSimilarityFunction COSINE = new LuceneVectorSimilarityFunction() {

        @Override
        public String name() {
            return "COSINE";
        }

        @Override
        public org.apache.lucene.index.VectorSimilarityFunction toLucene() {
            return org.apache.lucene.index.VectorSimilarityFunction.COSINE;
        }

        @Override
        public float[] maybeToValidVector(VectorCandidate candidate) {
            final int dimensions;
            if (candidate == null || (dimensions = candidate.dimensions()) == 0) {
                return null;
            }

            float square = 0.f;
            final var vector = new float[dimensions];
            for (int i = 0; i < dimensions; i++) {
                final var element = candidate.floatElement(i);
                if (!Float.isFinite(element)) {
                    return null;
                }
                square += element * element;
                vector[i] = element;
            }

            if (square <= 0.f || !Float.isFinite(square)) {
                return null;
            }

            return vector;
        }

        @Override
        public float[] toValidVector(VectorCandidate candidate) {
            final var vector = maybeToValidVector(candidate);
            if (vector == null) {
                throw new IllegalArgumentException(
                        "Vector must contain finite values, and have positive and finite l2-norm. Provided: "
                                + candidate);
            }
            return vector;
        }
    };

    public static final Set<VectorSimilarityFunction> SUPPORTED = Set.of(EUCLIDEAN, COSINE);

    public static VectorSimilarityFunction fromName(String name) {
        final var normalizedName = name.toUpperCase(Locale.ROOT);
        for (final var similarityFunction : SUPPORTED) {
            if (similarityFunction.name().equals(normalizedName)) {
                return similarityFunction;
            }
        }
        throw new IllegalArgumentException(
                "'%s' is an unsupported vector similarity function. Supported: %s".formatted(name, SUPPORTED));
    }
}