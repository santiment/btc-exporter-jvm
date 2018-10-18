package net.santiment.btc.blockprocessor

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
  * Computes the transaction stack changes from account changes.
  */
class TransactionStackFlatMap
  extends FlatMapFunction[ReducedAccountChange, InternalAccountModelChange]()
  with CheckpointedFunction
    with LazyLogging {

  @transient private var stack:MapState[Int, Segment] = _

  // We have both stackSize and size for performance. We'll have to access and change the size often. So to avoid IO we
  // will use 'size' and will serialize to 'stackSize' only once per flatMap invocation
  @transient private var stackSize: ValueState[Integer] = _
  private var size: Int = _

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val stackDescriptor = new MapStateDescriptor[Int, Segment](
      "transaction-stacks", implicitly[TypeInformation[Int]], implicitly[TypeInformation[Segment]])

    val stackSizeDescriptor = new ValueStateDescriptor[Integer](
      "stack-size", implicitly[TypeInformation[Integer]])


    stack = context.getKeyedStateStore.getMapState(stackDescriptor)
    stackSize = context.getKeyedStateStore.getState(stackSizeDescriptor)

    logger.trace("Connected")
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {}

  def begin(): Unit = {
    val sizeInteger = stackSize.value()
    if(sizeInteger == null) {
      stackSize.update(0)
      size = 0
    } else {
      size = sizeInteger.intValue()
    }
  }

  def commit(): Unit = {
    stackSize.update(size)
  }

  /**
    * Push a new segment in the stack
    * @param s - the segment to be pushed
    */
  def push(s:Segment): Unit = {
    stack.put(size, s)
    size += 1
  }

  /**
    * Pops an element from the stack. If the stack is empty - returns None
    * @return - The segment at the top of the stack
    */
  def pop(): Segment = {
    if(size == 0) {
      throw new IllegalStateException("Stack is empty")
    } else {
      val  result = stack.get(size-1)
      stack.remove(size-1)
      size -= 1
      result
    }
  }

  /**
    *
    * @param value
    * @param out
    */
  override def flatMap(value: ReducedAccountChange, out: Collector[InternalAccountModelChange]): Unit = {

    begin()
    logger.trace(s"processing $value stack size: $size")
    if (value.value < 0L) {
      // The account change is an input. This means we need to remove segments from old stacks,
      // and possibly add remainders

      var rem = Math.abs(value.value) //Our convention is that amounts for credits are negative
      var last_segment: Segment = null
      while (rem > 0 && size > 0) {
        val s = pop()
        last_segment = s
        logger.trace(s"rem=$rem, s.value=${s.value}")
        rem -= s.value

        out.collect(InternalAccountModelChange(
          sign = -1,  //This is a deletion
          ts = value.ts,
          height = value.height,
          txPos = value.txPos,
          ots = s.ots,
          oheight = s.oheight,
          otxPos = s.otxPos,
          address = value.address,
          value = s.value
        ))
      }

      // It could happen that the size is 0, but there is still remainder left. This can happen for the virtual "mint"
      // address in Bitcoin. For Ethereum and ERC20 tokens we might not have data for minting during the genesis of the,
      // asset which can also lead to negative values. Right now we have chosen to proceed as follows: If there are no
      // more segments in the stack left, we add a new special 'liability' segment with negative value equal to the
      // remainder, and emit the corresponding stack change.

      if (rem > 0L && size == 0) {
        push(Segment(
          ots = 0,
          oheight = 0,
          otxPos = 0,
          value = -rem
        ))

        out.collect(InternalAccountModelChange(
          sign = 1, //This is an addition (of an liability)
          ts = value.ts,
          height = value.height,
          txPos = value.txPos,
          ots = 0,
          oheight = 0,
          otxPos = 0,
          address = value.address,
          value = -rem
        ))

        rem = 0L
      }

      if (rem<0) {
        //There is a remainder. We have to add a new segment to account for it
        val remSegment = Segment(
          ots = last_segment.ots,
          oheight = last_segment.oheight,
          otxPos = last_segment.otxPos,
          value = Math.abs(rem)
        )
        push(remSegment)
        out.collect(InternalAccountModelChange(
          sign = 1,  //This is an addition
          ts = value.ts,
          height = value.height,
          txPos = value.txPos,
          ots = remSegment.ots,
          oheight = remSegment.oheight,
          otxPos = remSegment.otxPos,
          address = value.address,
          value = remSegment.value
        ))
      }
    } else {
      // The account change is an output. In that case it's easy - just add a single new segment

      push(Segment(
        ots = value.ts,
        oheight = value.height,
        otxPos = value.txPos,
        value = value.value //The output's value is positive by our convention - no need to take absolute
      ))

      out.collect(InternalAccountModelChange(
        sign = 1,
        ts = value.ts,
        height = value.height,
        txPos = value.txPos,
        ots = value.ts,
        oheight = value.height,
        otxPos = value.txPos,
        address = value.address,
        value = value.value
      ))
    }
    commit()
  }

}
