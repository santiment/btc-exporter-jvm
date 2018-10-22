package net.santiment.btc.blockprocessor

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.bitcoinj.core.Coin


/**
  * Computes the transaction stack changes from account changes.
  */
class TransactionStackFlatMap
  extends FlatMapFunction[ReducedAccountChange, AccountModelChange]()
  with CheckpointedFunction
    with LazyLogging {

  @transient private var stack:MapState[Int, Segment] = _

  // We have both stackSize and size for performance. We'll have to access and change the size often. So to avoid IO we
  // will use 'size' and will serialize to 'stackSize' only once per flatMap invocation
  @transient private var stackSize: ValueState[Integer] = _
  private var size: Int = _

  @transient private var nonceState: ValueState[Integer] = _
  private var nonce: Int = _

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val stackDescriptor = new MapStateDescriptor[Int, Segment](
      "transaction-stacks", implicitly[TypeInformation[Int]], implicitly[TypeInformation[Segment]])

    val stackSizeDescriptor = new ValueStateDescriptor[Integer](
      "stack-size", implicitly[TypeInformation[Integer]])

    val nonceDescriptor = new ValueStateDescriptor[Integer](
      "nonce", implicitly[TypeInformation[Integer]]
    )

    stack = context.getKeyedStateStore.getMapState(stackDescriptor)
    stackSize = context.getKeyedStateStore.getState(stackSizeDescriptor)
    nonceState = context.getKeyedStateStore.getState(nonceDescriptor)

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

    val tmpnonce = nonceState.value()
    if(tmpnonce == null) {
      nonceState.update(0)
      nonce = 0
    } else {
      nonce = tmpnonce.intValue()
    }
  }

  def commit(): Unit = {
    stackSize.update(size)
    nonceState.update(nonce)
  }

  /**
    * Push a new segment in the stack
    */
  def createAndPush(ots:Long, value: Long): Segment = {
    nonce += 1
    val result = Segment(
      nonce,
      ots,
      value
    )

    stack.put(size, result)
    size += 1
    result
  }

  /**
    * Pops an element from the stack. If the stack is empty - throws an exception
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
  override def flatMap(value: ReducedAccountChange, out: Collector[AccountModelChange]): Unit = {

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

        out.collect(AccountModelChange(
          sign = -1,  //This is a deletion
          ts = value.ts,
          height = value.height,
          txPos = value.txPos,
          nonce = s.nonce,
          ots = s.ots,
          address = value.address,
          value = Coin.valueOf(s.value).toPlainString.toDouble
        ))
      }

      // It could happen that the size is 0, but there is still remainder left. This can happen for the virtual "mint"
      // address in Bitcoin. For Ethereum and ERC20 tokens we might not have data for minting during the genesis of the,
      // asset which can also lead to negative values. Right now we have chosen to proceed as follows: If there are no
      // more segments in the stack left, we add a new special 'liability' segment with negative value equal to the
      // remainder, and emit the corresponding stack change.

      if (rem > 0L && size == 0) {
        val add = createAndPush(
          ots = 0,
          value = -rem
        )

        out.collect(AccountModelChange(
          sign = 1, //This is an addition (of an liability)
          ts = value.ts,
          height = value.height,
          txPos = value.txPos,
          nonce = add.nonce,
          ots = 0,
          address = value.address,
          value = Coin.valueOf(rem).negate().toPlainString.toDouble
        ))

        rem = 0L
      }

      if (rem<0) {
        //There is a remainder. We have to add a new segment to account for it
        val remSegment = createAndPush(
          ots = last_segment.ots,
          value = Math.abs(rem)
        )

        out.collect(AccountModelChange(
          sign = 1,  //This is an addition
          ts = value.ts,
          height = value.height,
          txPos = value.txPos,
          nonce = remSegment.nonce,
          ots = remSegment.ots,
          address = value.address,
          value = Coin.valueOf(remSegment.value).toPlainString.toDouble
        ))
      }
    } else {
      // The account change is an output. In that case it's easy - just add a single new segment

      val s = createAndPush(
        ots = value.ts,
        value = value.value //The output's value is positive by our convention - no need to take absolute
      )

      out.collect(AccountModelChange(
        sign = 1,
        ts = value.ts,
        height = value.height,
        txPos = value.txPos,
        nonce = s.nonce,
        ots = value.ts,
        address = value.address,
        value = Coin.valueOf(value.value).toPlainString.toDouble
      ))
    }
    commit()
  }

}
