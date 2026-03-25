/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.functions.unload;

import com.google.common.collect.ImmutableList;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestUnloadFunction
        extends AbstractTestQueryFramework
{
    @TempDir
    private Path tempDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return io.trino.plugin.hive.HiveQueryRunner.builder()
                .setInitialTables(ImmutableList.of(NATION))
                .setWorkerCount(0)
                .build();
    }

    @Test
    public void testUnloadWithDefaultFormat()
    {
        String outputDir = tempDir.resolve("unload_default").toUri().toString();
        assertQuerySucceeds(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "'))");
    }

    @Test
    public void testUnloadWithParquetFormat()
    {
        String outputDir = tempDir.resolve("unload_parquet").toUri().toString();
        assertQuerySucceeds(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
    }

    @Test
    public void testUnloadWithOrcFormat()
    {
        String outputDir = tempDir.resolve("unload_orc").toUri().toString();
        assertQuerySucceeds(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'ORC'))");
    }

    @Test
    public void testUnloadReturnsFileMetadata()
    {
        String outputDir = tempDir.resolve("unload_metadata").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(25); // 25 nations
        assertThat((long) result.getMaterializedRows().get(0).getField(2)).isGreaterThan(0);
    }

    @Test
    public void testUnloadWithEmptyResult()
    {
        String outputDir = tempDir.resolve("unload_empty").toUri().toString();
        MaterializedResult result = computeActual(
                "SELECT path, rows_written, bytes_written FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey, name FROM " + HIVE_CATALOG + ".tpch.nation WHERE nationkey < 0), " +
                        "location => '" + outputDir + "', " +
                        "format => 'PARQUET'))");
        // When no input data is provided, the result may be empty
        if (result.getRowCount() > 0) {
            assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(0);
        }
    }

    @Test
    public void testUnloadWithInvalidFormat()
    {
        String outputDir = tempDir.resolve("unload_invalid").toUri().toString();
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => '" + outputDir + "', " +
                        "format => 'INVALID_FORMAT'))",
                ".*Unknown format.*");
    }

    @Test
    public void testUnloadWithEmptyLocation()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(" +
                        "input => TABLE(SELECT nationkey FROM " + HIVE_CATALOG + ".tpch.nation), " +
                        "location => ''))",
                ".*location must not be empty.*");
    }
}
