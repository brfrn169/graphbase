package com.github.brfrn169.graphbase.hbase;

import com.github.brfrn169.graphbase.GraphServiceTest;
import com.github.brfrn169.graphbase.GraphStorage;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class HBaseGraphServiceTest extends GraphServiceTest {

    private static GraphStorage graphStorage;

    @BeforeClass public static void beforeClass() throws Exception {
        initialize(conf -> graphStorage = new HBaseGraphStorage(conf));
    }

    @AfterClass public static void afterClass() throws Exception {
        graphStorage.close();
        GraphServiceTest.shutdown();
    }
}
