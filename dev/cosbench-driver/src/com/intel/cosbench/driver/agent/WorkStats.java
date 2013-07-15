package com.intel.cosbench.driver.agent;

import static com.intel.cosbench.bench.Mark.getMarkType;
import static com.intel.cosbench.bench.Mark.newMark;

import java.util.LinkedHashSet;
import java.util.Set;

import com.intel.cosbench.api.context.StatsContext;
import com.intel.cosbench.api.stats.StatsCollector;
import com.intel.cosbench.bench.Mark;
import com.intel.cosbench.bench.Metrics;
import com.intel.cosbench.bench.Report;
import com.intel.cosbench.bench.Result;
import com.intel.cosbench.bench.Sample;
import com.intel.cosbench.bench.Snapshot;
import com.intel.cosbench.bench.Status;
import com.intel.cosbench.config.Mission;
import com.intel.cosbench.driver.model.OperatorContext;
import com.intel.cosbench.driver.model.OperatorRegistry;
import com.intel.cosbench.driver.model.WorkerContext;
import com.intel.cosbench.driver.operator.OperationListener;


public class WorkStats extends StatsCollector implements OperationListener {
    private long start; /* agent startup time */
    private long begin; /* effective workload startup time */
    private long end; /* effective workload shut-down time */
    private long timeout; /* expected agent stop time */

    private long lop; /* last operation performed */
    private long lbegin; /* last sample emitted */
    private long lrsample; /* last sample collected during runtime */
    private long frsample; /* first sample emitted during runtime */

    private long curr; /* current time */
    private long lcheck; /* last check point time */

    private int totalOps; /* total operations to be performed */
    private long totalBytes; /* total bytes to be transferred */
	private long ltotalBytes;
	private int ltotalOps;
	
    private OperatorRegistry operatorRegistry;

    private boolean isFinished = false;

    private Status currMarks = new Status(); /* for snapshots */
	private Status currMarksCloned = new Status();/* for snapshots */
    private Status globalMarks = new Status(); /* for the final report */
    
    private WorkerContext workerContext;

    public void setWorkerContext(WorkerContext workerContext) {
    	this.workerContext = workerContext;
    	this.workerContext.setStatsCollector(this);
    }
    
    @Override
    public synchronized void onSampleCreated(Sample sample) {
        String type = getMarkType(sample.getOpType(), sample.getSampleType());
        currMarks.getMark(type).addToSamples(sample);
        if (lbegin >= begin && lbegin < end && curr > begin && curr <= end) {
            globalMarks.getMark(type).addToSamples(sample);
            setlTotalBytes(getlTotalBytes() + sample.getBytes());
            //operatorRegistry.getOperator(sample.getOpType()).addSample(sample);
            if (lbegin < frsample)
                frsample = lbegin; // first sample emitted during runtime
            lrsample = curr; // last sample collected during runtime
        }
    }
    
    @Override
    public synchronized void onOperationCompleted(Result result) {
        curr = result.getTimestamp().getTime();
        String type = getMarkType(result.getOpType(), result.getSampleType());
        currMarks.getMark(type).addOperation(result);
        if (lop >= begin && lop < end && curr > begin && curr <= end){
            globalMarks.getMark(type).addOperation(result);
			setlTotoalOps(getlTotalOps() + 1);
        }
        lop = curr; // last operation performed
        trySummary(); // make a summary report if necessary
    }    
    
    long getTimeout() {
    	return timeout;
    }
    
    private void trySummary() {
        if ((timeout <= 0 || curr < timeout) // timeout
                && (totalOps <= 0 || getlTotalOps() < totalOps) // operations
                && (totalBytes <= 0 || getlTotalBytes() < totalBytes)) // bytes
            return; // not finished
        doSummary();
        isFinished = true;
    }

    boolean isFinished() {
    	return isFinished;
    }
    
    void finished() {
    	isFinished = true;
    }
    
    void doSummary() {
    	if(lrsample > frsample)
    	{
	        long window = lrsample - frsample;
	        Report report = new Report();
	        for (Mark mark : globalMarks)
	            report.addMetrics(Metrics.convert(mark, window));
	        workerContext.setReport(report);
    	}
    }
    
    public void setOperatorRegistry(OperatorRegistry operatorRegistry) {
        this.operatorRegistry = operatorRegistry;
    }
    
	private void setlTotoalOps(int total) {
		this.ltotalOps = total;
	}

	private int getlTotalOps() {
		return this.ltotalOps;
	}

	private void setlTotalBytes(long totalBytes) {
		this.ltotalBytes = totalBytes;
	}

	private long getlTotalBytes() {
		return this.ltotalBytes;
	}

//    private int getTotalOps() {
//        int sum = 0;
//        for (Mark mark : globalMarks)
//            sum += mark.getTotalOpCount();
//        return sum;
//    }
//
//    private long getTotalBytes() {
//        long bytes = 0;
//        for (Mark mark : globalMarks)
//            bytes += mark.getByteCount();
//        return bytes;
//    }

    public void doSnapshot() {
		synchronized (currMarks) {
			for (Mark mark : currMarks) {
				currMarksCloned.addMark(mark.clone());
				mark.clear();
			}
		}

		long window = System.currentTimeMillis() - lcheck;
		Report report = new Report();
		for (Mark mark : currMarksCloned) {
			for (Sample sample : mark.getSamples()) {
				mark.addSample(sample);
			}
			report.addMetrics(Metrics.convert(mark, window));
			mark.clear();
		}

		Snapshot snapshot = new Snapshot(report);
		workerContext.setSnapshot(snapshot);
		lcheck = System.currentTimeMillis();
    }

    void initTimes() {
        lcheck = curr = start = System.currentTimeMillis();
        lbegin = begin = start;
        timeout = 0L;
        lop = lrsample = start;
        frsample = lbegin = end = Long.MAX_VALUE;
    }

    void initLimites() {
        Mission mission = workerContext.getMission();
        totalOps = mission.getTotalOps() / mission.getTotalWorkers();
        totalBytes = mission.getTotalBytes() / mission.getTotalWorkers();
        if (mission.getRuntime() == 0)
            return;
        begin = start + mission.getRampup() * 1000;
        end = begin + mission.getRuntime() * 1000;
        timeout = end + mission.getRampdown() * 1000;
    }

    void initMarks() {
        Set<String> types = new LinkedHashSet<String>();
        for (OperatorContext op : operatorRegistry)
            types.add(getMarkType(op.getOpType(), op.getSampleType()));
        for (String type : types)
            currMarks.addMark(newMark(type));
        for (String type : types)
            globalMarks.addMark(newMark(type));
    }

	@Override
	public void onStats(StatsContext context, boolean status) {
		System.out.println("Request is " + (status? "succeed" : "failed"));
//		this.ts_end = System.currentTimeMillis();
//		System.out.println("Request is " + (status? "succeed" : "failed") + " in " + (ts_end - ts_start) + " milliseconds.");
//	
//        String type = getMarkType(sample.getOpType(), sample.getSampleType());
//        currMarks.getMark(type).addToSamples(sample);
//        if (lbegin >= begin && lbegin < end && curr > begin && curr <= end) {
//            globalMarks.getMark(type).addToSamples(sample);
//            setlTotalBytes(getlTotalBytes() + sample.getBytes());
//            //operatorRegistry.getOperator(sample.getOpType()).addSample(sample);
//            if (lbegin < frsample)
//                frsample = lbegin; // first sample emitted during runtime
//            lrsample = curr; // last sample collected during runtime
//        }
	}
    
}
