package com.example.atomikos;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.persistence.EntityManager;
import javax.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest
@Transactional
@Slf4j
class AtomikosApplicationTests {

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private EntityManager entityManager;

    @Test
    void testJTA() {

        ExecutorService executorService = null;

        executorService = Executors.newFixedThreadPool(1);

        try {

            List<BatchItem> batchItems = new ArrayList<>();

            batchItems.add(() -> {
                entityManager.createNativeQuery("select 1").getResultList().get(0);
                try {
                    Thread.sleep(10000);
                }
                catch (InterruptedException e) {
                    log.debug("", e);
                    //restore thread interrupt state / emulate not responding DB query
                    Thread.currentThread().interrupt();
                }
                return true;
            });
            batchItems.add(() -> {
                entityManager.createNativeQuery("select 1").getResultList().get(0);
                return true;
            });
            batchItems.add(() -> {
                entityManager.createNativeQuery("select 2").getResultList().get(0);
                return true;
            });
            batchItems.add(() -> {
                entityManager.createNativeQuery("select 3").getResultList().get(0);
                return true;
            });

            List<Future<FutureResult<Boolean>>> futures = new ArrayList<>();

            for (BatchItem batchEntry : batchItems) {

                futures.add(
                        executorService.submit(() -> {
                            List<Throwable> exceptions = new ArrayList<>();
                            Boolean result = null;

                            try {
                                log.info("processing batch entry {}", batchEntry);

                                result = transactionTemplate.execute(status -> batchEntry.run());

                                log.info("batch successfully processed");
                            }
                            catch (RuntimeException e) {
                                exceptions.add(e);
                                log.error("unexpected error: ", e);
                            }

                            FutureResult<Boolean> futureResult = new FutureResult<>();
                            futureResult.exceptions = exceptions;
                            futureResult.result = result;

                            return futureResult;

                        })
                );
            }

            List<Throwable> exceptions = new ArrayList<>();
            for (Future<FutureResult<Boolean>> future : futures) {
                try {
                    FutureResult<Boolean> futureResult;
                    futureResult = future.get(1, TimeUnit.SECONDS);
                    exceptions.addAll(futureResult.exceptions);
                }

                catch (InterruptedException e) {
                    log.error("Thread was interrupted", e);
                    exceptions.add(e);
                    // Restore interrupted state...
                    Thread.currentThread().interrupt();
                }
                catch (TimeoutException e) {
                    log.error("Exception while waiting for the batch job future task completion", e);
                    exceptions.add(e);
                    // cancel the task and flag its thread as interrupted
                    future.cancel(true);
                }
                catch (ExecutionException e) {
                    log.error("Exception while the batch job future task execution", e);
                    exceptions.add(e);
                }
                catch (CancellationException e) {
                    log.error("The task was canceled", e);
                    exceptions.add(e);
                }
            }

            if (exceptions.isEmpty()) {
                log.info("successfully processed batch with {}", batchItems);
            }
            else {
                log.error("batch processing finished with {} exceptions", exceptions.size());
            }

            Assertions.assertEquals(1, exceptions.size());

        }
        finally {
            shutDownExecutorService(executorService);
        }

    }

    /**
     * Shuts down an ExecutorService in two phases
     */
    private void shutDownExecutorService(ExecutorService executorService) {

        if (executorService == null) {
            return;
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            executorService.shutdownNow();
            // Restore interrupted state...
            Thread.currentThread().interrupt();
        }
    }

    @FunctionalInterface
    interface BatchItem {

        Boolean run();
    }

    private static class FutureResult<Boolean> {

        private List<Throwable> exceptions = new ArrayList<>();
        private Boolean result;
    }

}
