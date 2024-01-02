/*
 * Copyright 2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.foreach.across.it.concurrent;

import com.foreach.across.core.AcrossContext;
import com.foreach.across.core.context.AcrossContextUtils;
import com.foreach.across.core.support.AcrossContextBuilder;
import com.foreach.across.test.AcrossTestConfiguration;
import com.foreach.across.test.support.config.TestDataSourceConfigurer;
import com.foreach.common.concurrent.locks.distributed.DistributedLock;
import com.foreach.common.concurrent.locks.distributed.DistributedLockRepository;
import liquibase.Scope;
import liquibase.SingletonScopeManager;
import liquibase.ThreadLocalScopeManager;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.sql.DataSource;
import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Arne Vandamme
 */
@ExtendWith(SpringExtension.class)
@DirtiesContext
@ContextConfiguration
public class ITDistributedLockRepository
{
	private static final int BATCHES = 3;
	private static final int LOCKS_PER_BATCH = 50;
	private static final int EXECUTORS_PER_LOCK = 10;

	@Autowired
	private DataSource testDataSource;

	@Autowired
	private Environment environment;

	private final Map<String, Integer> resultsByLock = Collections.synchronizedMap( new HashMap<>() );

	@BeforeEach
	public void setup() {
		resultsByLock.clear();

		// This test started failing systematically on Eindhoven GitLab with a VM with this CPU configuration (output of lscpu):
		// Architecture:                    x86_64
		// CPU op-mode(s):                  32-bit, 64-bit
		// Address sizes:                   46 bits physical, 48 bits virtual
		// Byte Order:                      Little Endian
		// CPU(s):                          2
		// On-line CPU(s) list:             0,1
		// Vendor ID:                       GenuineIntel
		// Model name:                      Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz
		// CPU family:                      6
		// Model:                           85
		// Thread(s) per core:              2
		// Core(s) per socket:              1
		// Socket(s):                       1
		// Stepping:                        7
		// BogoMIPS:                        4999.98
		// Flags:                           fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ss ht syscall nx pdpe1gb rdtscp lm constant_tsc rep_good nopl xtopology nonstop_tsc cpuid tsc_known_freq pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm abm 3dnowprefetch invpcid_single pti fsgsbase tsc_adjust bmi1 avx2 smep bmi2 erms invpcid mpx avx512f avx512dq rdseed adx smap clflushopt clwb avx512cd avx512bw avx512vl xsaveopt xsavec xgetbv1 xsaves ida arat pku ospke
		// Hypervisor vendor:               KVM
		// Virtualization type:             full
		// L1d cache:                       32 KiB (1 instance)
		// L1i cache:                       32 KiB (1 instance)
		// L2 cache:                        1 MiB (1 instance)
		// L3 cache:                        35.8 MiB (1 instance)
		// NUMA node(s):                    1
		// NUMA node0 CPU(s):               0,1
		// Vulnerability Itlb multihit:     KVM: Mitigation: VMX unsupported
		// Vulnerability L1tf:              Mitigation; PTE Inversion
		// Vulnerability Mds:               Vulnerable: Clear CPU buffers attempted, no microcode; SMT Host state unknown
		// Vulnerability Meltdown:          Mitigation; PTI
		// Vulnerability Spec store bypass: Vulnerable
		// Vulnerability Spectre v1:        Mitigation; usercopy/swapgs barriers and __user pointer sanitization
		// Vulnerability Spectre v2:        Mitigation; Retpolines, STIBP disabled, RSB filling
		// Vulnerability Srbds:             Not affected
		// Vulnerability Tsx async abort:   Not affected

		// The exception was:
		// Nested exception is liquibase.exception.LiquibaseException: java.lang.RuntimeException: Cannot end scope ztpizvundx when currently at scope wrwedqqnfc
		// 	at java.util.concurrent.FutureTask.report(FutureTask.java:122)
		// 	at java.util.concurrent.FutureTask.get(FutureTask.java:192)
		// 	at com.foreach.across.it.concurrent.ITDistributedLockRepository.executeBatch(ITDistributedLockRepository.java:91)
		// There are a bunch of reports about that in Liquibase, and they introduced the ThreadLocalScopeManager in 4.17.0 for this:
		//   https://github.com/liquibase/liquibase/pull/3240
		// Just upgrading isn't sufficient, you do have to explicitly configure this:
		Scope.setScopeManager(new ThreadLocalScopeManager());
	}

	@AfterEach
	public void tearDown() {
		Scope.setScopeManager( new SingletonScopeManager() );
	}

	@Test
	public void executeBatch() throws Exception {
		int batchSize = BATCHES;
		int totalResults = BATCHES * LOCKS_PER_BATCH * EXECUTORS_PER_LOCK;
		int totalLocks = LOCKS_PER_BATCH;
		int resultsPerLock = EXECUTORS_PER_LOCK * BATCHES;

		ExecutorService batchExecutorService = Executors.newFixedThreadPool( batchSize );

		Set<Future<Integer>> results = new HashSet<>();

		for ( int i = 0; i < batchSize; i++ ) {
			ExecutorBatch batch = new ExecutorBatch( i * 4000 );
			results.add( batchExecutorService.submit( batch ) );
		}

		batchExecutorService.shutdown();
		batchExecutorService.awaitTermination( 3, TimeUnit.MINUTES );

		int actualResults = 0;

		for ( Future<Integer> result : results ) {
			actualResults += result.get();
		}

		assertEquals( totalResults, actualResults );

		JdbcTemplate jdbcTemplate = new JdbcTemplate( testDataSource );
		assertEquals( Integer.valueOf( totalLocks ),
		              jdbcTemplate.queryForObject( "SELECT count(*) FROM across_locks", Integer.class ) );

		// Check synchronization was correct
		assertEquals( totalLocks, resultsByLock.size() );
		for ( Map.Entry<String, Integer> entry : resultsByLock.entrySet() ) {
			assertEquals( Integer.valueOf( resultsPerLock ), entry.getValue() );
		}
	}

	protected class ExecutorBatch implements Callable<Integer>
	{
		private final int startDelay;

		public ExecutorBatch( int startDelay ) {
			this.startDelay = startDelay;
		}

		@Override
		public Integer call() throws Exception {
			if ( startDelay > 0 ) {
				Thread.sleep( startDelay );
			}

			DataSource dataSource = uniqueDataSource();
			AcrossContext context = new AcrossContextBuilder().dataSource( dataSource ).build();

			try {
				context.bootstrap();

				DistributedLockRepository distributedLockRepository
						= AcrossContextUtils.getBeanRegistry( context )
						                    .getBeanOfType( DistributedLockRepository.class );

				ExecutorService fixedThreadPool = Executors.newFixedThreadPool( 50 );

				List<Executor> executors = new ArrayList<>( LOCKS_PER_BATCH * EXECUTORS_PER_LOCK );

				for ( int i = 0; i < LOCKS_PER_BATCH; i++ ) {
					DistributedLock lock = distributedLockRepository.getLock( "batch-lock-" + i );

					for ( int j = 0; j < EXECUTORS_PER_LOCK; j++ ) {
						executors.add( new Executor( lock, 10 ) );
					}
				}

				for ( Executor executor : executors ) {
					fixedThreadPool.submit( executor );
				}

				fixedThreadPool.shutdown();
				fixedThreadPool.awaitTermination( 3, TimeUnit.MINUTES );

				int totalSucceeded = 0;

				for ( Executor executor : executors ) {
					if ( executor.isFinished() ) {
						totalSucceeded++;
					}
				}

				return totalSucceeded;
			}
			finally {
				context.destroy();
				if ( dataSource instanceof Closeable ) {
					( (Closeable) dataSource ).close();
				}
			}
		}
	}

	private DataSource uniqueDataSource() {
		TestDataSourceConfigurer factory = new TestDataSourceConfigurer();
		factory.setEnvironment( environment );
		factory.setDataSourceName( "it-distributed-lock-repository" );
		return factory.testDataSource();
	}

	protected class Executor implements Runnable
	{
		private final int sleepTime;
		private final DistributedLock lock;
		private final StopWatch stopWatch = new StopWatch();
		private boolean failed;
		private boolean finished;

		public Executor( DistributedLock lock, int sleepTime ) {
			if ( !resultsByLock.containsKey( lock.getKey() ) ) {
				resultsByLock.put( lock.getKey(), 0 );
			}

			this.lock = lock;
			this.sleepTime = sleepTime;
		}

		public boolean isFinished() {
			return finished && !isFailed();
		}

		public boolean isFailed() {
			return failed;
		}

		public long getExecutionTime() {
			return stopWatch.getTime();
		}

		@Override
		public void run() {
			try {
				stopWatch.start();
				lock.lock();

				if ( sleepTime > 0 ) {
					Thread.sleep( sleepTime );
				}

				Integer currentCount = resultsByLock.get( lock.getKey() );
				resultsByLock.put( lock.getKey(), currentCount + 1 );

				finished = true;

				stopWatch.stop();
			}
			catch ( Exception ie ) {
				failed = true;
				ie.printStackTrace();
			}
			finally {
				lock.unlock();
			}
		}
	}

	/**
	 * Instantiate an Across context so we find access to the test datasource.
	 */
	@AcrossTestConfiguration
	protected static class Config
	{
		@Autowired
		public void setDataSourceName( TestDataSourceConfigurer testDataSourceConfigurer ) {
			testDataSourceConfigurer.setDataSourceName( "it-distributed-lock-repository" );
		}
	}
}
