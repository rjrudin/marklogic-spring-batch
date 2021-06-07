package com.marklogic.spring.batch.test;

import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.helper.DatabaseClientProvider;
import com.marklogic.junit.NamespaceProvider;
import com.marklogic.junit.spring.AbstractSpringTest;
import com.marklogic.spring.batch.config.MarkLogicConfiguration;
import com.marklogic.xcc.template.XccTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

/**
 * Base class for any "core" test.
 */
@ContextConfiguration(classes = { MarkLogicConfiguration.class })
@TestPropertySource("classpath:job.properties")
public abstract class AbstractSpringBatchTest extends AbstractSpringTest {

    protected ApplicationContext applicationContext;
    protected XccTemplate xccTemplate;

    @Override
    protected NamespaceProvider getNamespaceProvider() {
        return new SpringBatchNamespaceProvider();
    }

    @Override
    protected ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        setDatabaseClientProvider(applicationContext.getBean("databaseClientProvider", DatabaseClientProvider.class));
        setXccTemplate(xccTemplate);
    }

    @Autowired
    public XccTemplate xccTemplate(DatabaseClientConfig batchDatabaseClientConfig,
                                   @Value("${marklogic.database:Documents}") String databaseName) {
        this.xccTemplate = new XccTemplate(
                String.format("xcc://%s:%s@%s:%s/%s",
                        batchDatabaseClientConfig.getUsername(),
                        batchDatabaseClientConfig.getPassword(),
                        batchDatabaseClientConfig.getHost(),
                        batchDatabaseClientConfig.getPort(),
                        databaseName));
        return xccTemplate;
    }

    protected void assertCollectionSize(String message, String collection, int count) {
        Integer actualCount = Integer.parseInt(getXccTemplate().executeAdhocQuery(
                format("xdmp:estimate(fn:collection('%s'))", collection)));
        assertEquals(message, count, (int)actualCount);
    }
}
