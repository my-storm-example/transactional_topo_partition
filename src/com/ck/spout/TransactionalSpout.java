package com.ck.spout;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.ck.pojo.TransactionalMeta;

public class TransactionalSpout implements IPartitionedTransactionalSpout<TransactionalMeta> {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: TODO
   */
  private static final long serialVersionUID = 1L;

  private int _amount = 10;

  private Map<Integer, String> _logMap = new HashMap<Integer, String>();

  public Map<Integer, String> logSource() {
    Map<Integer, String> logMap = new HashMap<Integer, String>();
    for (int i = 0; i < 100; i++) {
      logMap.put(i, "log");
    }
    return logMap;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("txid", "log"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout.Coordinator getCoordinator(
      Map conf, TopologyContext context) {
    return new Coordinator();
  }

  @Override
  public backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout.Emitter<TransactionalMeta> getEmitter(
      Map conf, TopologyContext context) {
    return new Emmit(logSource());
  }

  /**
   * @author xuer
   * @date 2014-8-27 - 上午10:42:37
   * @Description 用numPartitions方法，返回此分区事务topo有多少个分区
   */
  class Coordinator implements IPartitionedTransactionalSpout.Coordinator {

    @Override
    public int numPartitions() {
      return 3;
    }

    @Override
    public boolean isReady() {
      Utils.sleep(2500);
      return true;
    }

    @Override
    public void close() {}

  }

  class Emmit implements IPartitionedTransactionalSpout.Emitter<TransactionalMeta> {

    public Emmit(Map<Integer, String> logMap) {
      _logMap = logMap;
    }

    @Override
    public void close() {}

    /**
     * @Title: emitPartitionBatchNew
     * @Description: 在分区里发起一个新的事务，同普通事务里的coordinater的initializeTransaction方法
     * @param tx
     * @param collector
     * @param partition
     * @param lastPartitionMeta
     * @return
     */
    @Override
    public TransactionalMeta emitPartitionBatchNew(TransactionAttempt tx,
        BatchOutputCollector collector, int partition, TransactionalMeta lastPartitionMeta) {
      long _index;
      if (lastPartitionMeta == null) {
        _index = 0;
        lastPartitionMeta = new TransactionalMeta();
      } else {
        _index = lastPartitionMeta.getIndex() + lastPartitionMeta.getAmount();
        _amount = lastPartitionMeta.getAmount();
      }
      System.out.println("事务开始的partition：" + partition + ",此事务的TransactionalId"
          + tx.getTransactionId() + "index:" + _index + ",amount:" + _amount);
      emitPartitionBatch(tx, collector, partition, new TransactionalMeta(_index, _amount));
      return new TransactionalMeta(_index, _amount);
    }

    /**
     * @Title: emitPartitionBatch
     * @Description: 批次里正真发送tuple的方法，同普通事务里的Emmit类的emitBatch方法
     * @param tx
     * @param collector
     * @param partition
     * @param partitionMeta
     */
    @Override
    public void emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector,
        int partition, TransactionalMeta partitionMeta) {
      System.err.println("emitPartitionBatch里的partition：" + partition);
      for (Long i = partitionMeta.getIndex(); i < partitionMeta.getIndex()
          + partitionMeta.getAmount(); i++) {
        if (_logMap.get(i.intValue()) != null) {
          collector.emit(new Values(tx, _logMap.get(i.intValue())));
        }
      }
    }

  }

}
