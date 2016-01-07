/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.plan.logical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.validation.ValidationException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.DefaultDelayOperator;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.support.StramTestSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests for topologies with delay operator
 */
public class DelayOperatorTest
{
  private static Lock sequential = new ReentrantLock();

  @Before
  public void setup()
  {
    sequential.lock();
  }

  @After
  public void teardown()
  {
    sequential.unlock();
  }

  @Test
  public void testInvalidDelayDetection()
  {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator opB = dag.addOperator("B", GenericTestOperator.class);
    GenericTestOperator opC = dag.addOperator("C", GenericTestOperator.class);
    GenericTestOperator opD = dag.addOperator("D", GenericTestOperator.class);
    DefaultDelayOperator opDelay = dag.addOperator("opDelay", DefaultDelayOperator.class);

    dag.addStream("BtoC", opB.outport1, opC.inport1);
    dag.addStream("CtoD", opC.outport1, opD.inport1);
    dag.addStream("CtoDelay", opC.outport2, opDelay.input);
    dag.addStream("DelayToD", opDelay.output, opD.inport2);

    List<List<String>> invalidDelays = new ArrayList<>();
    dag.findInvalidDelays(dag.getMeta(opB), invalidDelays);
    assertEquals("operator invalid delay", 1, invalidDelays.size());

    try {
      dag.validate();
      fail("validation should fail");
    } catch (ValidationException e) {
      // expected
    }

    dag = new LogicalPlan();

    opB = dag.addOperator("B", GenericTestOperator.class);
    opC = dag.addOperator("C", GenericTestOperator.class);
    opD = dag.addOperator("D", GenericTestOperator.class);
    opDelay = dag.addOperator("opDelay", DefaultDelayOperator.class);
    dag.setAttribute(opDelay, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 2);
    dag.addStream("BtoC", opB.outport1, opC.inport1);
    dag.addStream("CtoD", opC.outport1, opD.inport1);
    dag.addStream("CtoDelay", opC.outport2, opDelay.input);
    dag.addStream("DelayToC", opDelay.output, opC.inport2);

    invalidDelays = new ArrayList<>();
    dag.findInvalidDelays(dag.getMeta(opB), invalidDelays);
    assertEquals("operator invalid delay", 1, invalidDelays.size());

    try {
      dag.validate();
      fail("validation should fail");
    } catch (ValidationException e) {
      // expected
    }

    dag = new LogicalPlan();

    opB = dag.addOperator("B", GenericTestOperator.class);
    opC = dag.addOperator("C", GenericTestOperator.class);
    opD = dag.addOperator("D", GenericTestOperator.class);
    opDelay = dag.addOperator("opDelay", DefaultDelayOperator.class);
    dag.addStream("BtoC", opB.outport1, opC.inport1);
    dag.addStream("CtoD", opC.outport1, opD.inport1);
    dag.addStream("CtoDelay", opC.outport2, opDelay.input).setLocality(DAG.Locality.THREAD_LOCAL);
    dag.addStream("DelayToC", opDelay.output, opC.inport2).setLocality(DAG.Locality.THREAD_LOCAL);

    try {
      dag.validate();
      fail("validation should fail");
    } catch (ValidationException e) {
      // expected
    }
  }

  @Test
  public void testValidDelay()
  {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator opA = dag.addOperator("A", TestGeneratorInputOperator.class);
    GenericTestOperator opB = dag.addOperator("B", GenericTestOperator.class);
    GenericTestOperator opC = dag.addOperator("C", GenericTestOperator.class);
    GenericTestOperator opD = dag.addOperator("D", GenericTestOperator.class);
    DefaultDelayOperator opDelay = dag.addOperator("opDelay", DefaultDelayOperator.class);

    dag.addStream("AtoB", opA.outport, opB.inport1);
    dag.addStream("BtoC", opB.outport1, opC.inport1);
    dag.addStream("CtoD", opC.outport1, opD.inport1);
    dag.addStream("CtoDelay", opC.outport2, opDelay.input);
    dag.addStream("DelayToB", opDelay.output, opB.inport2);
    dag.validate();
  }

  public static final Long[] FIBONACCI_NUMBERS = new Long[]{
      1L, 1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L, 89L, 144L, 233L, 377L, 610L, 987L, 1597L, 2584L, 4181L, 6765L,
      10946L, 17711L, 28657L, 46368L, 75025L, 121393L, 196418L, 317811L, 514229L, 832040L, 1346269L, 2178309L,
      3524578L, 5702887L, 9227465L, 14930352L, 24157817L, 39088169L, 63245986L, 102334155L
  };

  public static class FibonacciOperator extends BaseOperator
  {
    public static List<Long> results = new ArrayList<>();
    public long currentNumber = 1;
    private transient long tempNum;

    public transient DefaultInputPort<Object> dummyInputPort = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
      }
    };
    public transient DefaultInputPort<Long> input = new DefaultInputPort<Long>()
    {
      @Override
      public void process(Long tuple)
      {
        tempNum = tuple;
      }
    };
    public transient DefaultOutputPort<Long> output = new DefaultOutputPort<>();

    @Override
    public void endWindow()
    {
      output.emit(currentNumber);
      results.add(currentNumber);
      currentNumber += tempNum;
    }

  }

  public static class FailableFibonacciOperator extends FibonacciOperator implements Operator.CheckpointListener
  {
    private boolean committed = false;
    private int simulateFailureWindows = 0;
    private boolean simulateFailureAfterCommit = false;
    private int windowCount = 0;
    public static boolean failureSimulated = false;

    @Override
    public void beginWindow(long windowId)
    {
      if (simulateFailureWindows > 0 && ((simulateFailureAfterCommit && committed) || !simulateFailureAfterCommit) &&
          !failureSimulated) {
        if (windowCount++ == simulateFailureWindows) {
          failureSimulated = true;
          throw new RuntimeException("simulating failure");
        }
      }
    }

    @Override
    public void checkpointed(long windowId)
    {
    }

    @Override
    public void committed(long windowId)
    {
      committed = true;
    }

    public void setSimulateFailureWindows(int windows, boolean afterCommit)
    {
      this.simulateFailureAfterCommit = afterCommit;
      this.simulateFailureWindows = windows;
    }
  }

  public static class FailableDelayOperator extends DefaultDelayOperator implements Operator.CheckpointListener
  {
    private boolean committed = false;
    private int simulateFailureWindows = 0;
    private boolean simulateFailureAfterCommit = false;
    private int windowCount = 0;
    private static boolean failureSimulated = false;

    @Override
    public void beginWindow(long windowId)
    {
      super.beginWindow(windowId);
      if (simulateFailureWindows > 0 && ((simulateFailureAfterCommit && committed) || !simulateFailureAfterCommit) &&
          !failureSimulated) {
        if (windowCount++ == simulateFailureWindows) {
          failureSimulated = true;
          throw new RuntimeException("simulating failure");
        }
      }
    }

    @Override
    public void checkpointed(long windowId)
    {
    }

    @Override
    public void committed(long windowId)
    {
      committed = true;
    }

    public void setSimulateFailureWindows(int windows, boolean afterCommit)
    {
      this.simulateFailureAfterCommit = afterCommit;
      this.simulateFailureWindows = windows;
    }
  }


  @Test
  public void testFibonacci() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator dummyInput = dag.addOperator("DUMMY", TestGeneratorInputOperator.class);
    FibonacciOperator fib = dag.addOperator("FIB", FibonacciOperator.class);
    DefaultDelayOperator opDelay = dag.addOperator("opDelay", DefaultDelayOperator.class);

    dag.addStream("dummy_to_operator", dummyInput.outport, fib.dummyInputPort);
    dag.addStream("operator_to_delay", fib.output, opDelay.input);
    dag.addStream("delay_to_operator", opDelay.output, fib.input);
    FibonacciOperator.results.clear();
    final StramLocalCluster localCluster = new StramLocalCluster(dag);
    localCluster.setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return FibonacciOperator.results.size() >= 10;
      }
    });
    localCluster.run(10000);
    Assert.assertArrayEquals(Arrays.copyOfRange(FIBONACCI_NUMBERS, 0, 10),
        FibonacciOperator.results.subList(0, 10).toArray());
  }

  @Test
  public void testFibonacciRecovery1() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator dummyInput = dag.addOperator("DUMMY", TestGeneratorInputOperator.class);
    FailableFibonacciOperator fib = dag.addOperator("FIB", FailableFibonacciOperator.class);
    DefaultDelayOperator opDelay = dag.addOperator("opDelay", DefaultDelayOperator.class);

    fib.setSimulateFailureWindows(3, true);

    dag.addStream("dummy_to_operator", dummyInput.outport, fib.dummyInputPort);
    dag.addStream("operator_to_delay", fib.output, opDelay.input);
    dag.addStream("delay_to_operator", opDelay.output, fib.input);
    dag.getAttributes().put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 2);
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 300);
    FailableFibonacciOperator.results.clear();
    FailableFibonacciOperator.failureSimulated = false;
    final StramLocalCluster localCluster = new StramLocalCluster(dag);
    localCluster.setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return FailableFibonacciOperator.results.size() >= 30;
      }
    });
    localCluster.run(60000);
    System.out.println("RESULT: " + FailableFibonacciOperator.results);
    Assert.assertTrue("failure should be invoked", FailableFibonacciOperator.failureSimulated);
    Assert.assertArrayEquals(Arrays.copyOfRange(new TreeSet<>(Arrays.asList(FIBONACCI_NUMBERS)).toArray(), 0, 20),
        Arrays.copyOfRange(new TreeSet<>(FibonacciOperator.results).toArray(), 0, 20));
  }

  @Test
  public void testFibonacciRecovery2() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator dummyInput = dag.addOperator("DUMMY", TestGeneratorInputOperator.class);
    FibonacciOperator fib = dag.addOperator("FIB", FibonacciOperator.class);
    FailableDelayOperator opDelay = dag.addOperator("opDelay", FailableDelayOperator.class);

    opDelay.setSimulateFailureWindows(5, true);

    dag.addStream("dummy_to_operator", dummyInput.outport, fib.dummyInputPort);
    dag.addStream("operator_to_delay", fib.output, opDelay.input);
    dag.addStream("delay_to_operator", opDelay.output, fib.input);
    dag.getAttributes().put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 2);
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 300);
    FibonacciOperator.results.clear();
    FailableDelayOperator.failureSimulated = false;
    final StramLocalCluster localCluster = new StramLocalCluster(dag);
    localCluster.setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return FibonacciOperator.results.size() >= 30;
      }
    });
    localCluster.run(60000);

    System.out.println("RESULT: " + FibonacciOperator.results);
    Assert.assertTrue("failure should be invoked", FailableDelayOperator.failureSimulated);
    Assert.assertArrayEquals(Arrays.copyOfRange(new TreeSet<>(Arrays.asList(FIBONACCI_NUMBERS)).toArray(), 0, 20),
        Arrays.copyOfRange(new TreeSet<>(FibonacciOperator.results).toArray(), 0, 20));
  }


}
