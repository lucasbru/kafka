/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group.streams.assignor;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TaskIdTest {

    @Test
    public void shouldOrderBySubtopologyThenPartition() {
        final TaskId a0 = new TaskId("a", 0);
        final TaskId a1 = new TaskId("a", 1);
        final TaskId a10 = new TaskId("a", 10);
        final TaskId b0 = new TaskId("b", 0);

        assertEquals(List.of(a0, a1, a10, b0), List.copyOf(new TreeSet<>(List.of(b0, a10, a1, a0))));
        assertTrue(a1.compareTo(b0) < 0);
        assertTrue(a10.compareTo(a1) > 0);
        assertEquals(0, a1.compareTo(new TaskId("a", 1)));
    }
}
