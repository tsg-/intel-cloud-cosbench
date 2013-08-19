package com.intel.cosbench.driver.model;

//import static com.intel.cosbench.bench.Mark.getMarkType;
import static com.intel.cosbench.bench.Mark.newMark;

import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.intel.cosbench.api.context.ExecContext;
import com.intel.cosbench.api.stats.StatsListener;
import com.intel.cosbench.bench.Mark;
import com.intel.cosbench.bench.Metrics;
import com.intel.cosbench.bench.Report;
import com.intel.cosbench.bench.Sample;
import com.intel.cosbench.bench.Snapshot;
import com.intel.cosbench.bench.Status;
import com.intel.cosbench.config.Mission;
import com.intel.cosbench.log.LogFactory;
import com.intel.cosbench.log.Logger;
//import com.intel.cosbench.bench.Result;
//import com.intel.cosbench.driver.operator.OperationListener;


public class WorkStats extends StatsListener /* implements OperationListener */{
    private long start; /* agent startup time */
    private long begin; /* effective workload startup time */
    private long end; /* effective workload shut-down time */
    private long runtime; /* expected agent stop time */

//    private long lop; /* last operation performed */
    private long lbegin; /* last sample emitted */
    private long lrsample; /* last sample collected during runtime */
    private long frsample; /* first sample emitted during runtime */

    private long curr; /* current time */
    private long lcheck; /* last check point time */

    private int totalOps; /* total operations to be performed */
    private long totalBytes; /* total bytes to be transferred */
	private long totalBytes_performed; /* last performed bytes in total */
	private int totalOps_performed; /* last performed operations in total */
	private int totalOps_issued; /* total operations have issued */
	
    private OperatorRegistry operatorRegistry;

//    private static volatile boolean isFinished = false;

    private Status currMarks = new Status(); /* for snapshots */
    private Status globalMarks = new Status(); /* for the final report */
    
    private WorkerContext workerContext;
    /* Each worker has its private required version */
    private AtomicInteger version = new AtomicInteger(0);
    
    public WorkStats(WorkerContext workerContext) {
    	this.workerContext = workerContext;
    }
    
    private void addSamples(Status status, String type, Sample sample) {
    	if(status.getMark(type) !=null)
    		status.getMark(type).addSample(sample);
    }
    
    public long updateStats() {
    	lbegin = System.currentTimeMillis();
    	return ++totalOps_issued;
    }
    
    public boolean isRunning() {
        return ((runtime <= 0 || System.currentTimeMillis() < runtime) // timeout
                && (totalOps <= 0 || totalOps_issued < totalOps) // operations
                && (totalBytes <= 0 || totalBytes_performed < totalBytes)); // bytes
    }
    
    public synchronized void updateSampleList(Sample sample) {
    	curr = sample.getTimestamp().getTime();
        String type = sample.getOpType(); //getMarkType(sample.getOpType(), sample.getSampleType());
        addSamples(currMarks, type, sample);
        if (lbegin >= begin && lbegin < end && curr > begin && curr <= end) { /* if the completed operation is executed in the measured period, count it */
            addSamples(globalMarks, type, sample);
			totalOps_performed++;
			totalBytes_performed += sample.getBytes();
            operatorRegistry.getOperator(sample.getOpType()).addSample(sample);
            if (lbegin < frsample)
                frsample = lbegin; // the first sample emitted during runtime
            lrsample = curr; // last sample collected during runtime
        }
        
        trySummary(); // make a summary report if necessary
    }
    
    public synchronized void onSampleCreatedNEW(Sample sample) {
    	curr = sample.getTimestamp().getTime();
        String type = sample.getOpType(); //getMarkType(sample.getOpType(), sample.getSampleType());
        addSamples(currMarks, type, sample);
        if (lbegin >= begin && lbegin < end && curr > begin && curr <= end) {
            addSamples(globalMarks, type, sample);
			totalOps_performed++;
			totalBytes_performed += sample.getBytes();
            operatorRegistry.getOperator(sample.getOpType()).addSample(sample);
            if (lbegin < frsample)
                frsample = lbegin; // first sample emitted during runtime
            lrsample = curr; // last sample collected during runtime
        }
        
        trySummary(); // make a summary report if necessary
    }
    
    long getRuntime() {
    	return runtime;
    }
    
    private void trySummary() {
        if (isRunning())
            return; // not finished
        
        doSummary();
//        finished();
    }

    public void waitForCompletion(long interval) {
		while(totalOps_issued > totalOps_performed) // 
		{
			try{
				Thread.sleep(interval);
			}catch(InterruptedException ignore) {}
			
			workerContext.getLogger().debug("Outstanding operations = {}", (totalOps_issued - totalOps_performed));
		}
    }
    
    public boolean hasSamples() {
    	return lrsample > frsample;
    }
    
    public void doSummary() {
    	
    	if(hasSamples())
    	{
	        long window = lrsample - frsample;
	        Report report = new Report();
	        workerContext.getLogger().debug("Mark Count = " + globalMarks.getAllMarks().length);
	        System.out.println("Mark Count = " + globalMarks.getAllMarks().length + ", Type = " + globalMarks.getAllMarks()[0].getOpType());
	        for (Mark mark : globalMarks) {
	            report.addMetrics(Metrics.convert(mark, window));
	        }
	        
	        workerContext.setReport(report);
    	}
    }
    
    public void setOperatorRegistry(OperatorRegistry operatorRegistry) {
        this.operatorRegistry = operatorRegistry;
    }
    
    public Snapshot doSnapshot() {
		long window = System.currentTimeMillis() - lcheck;

		Report report = new Report();
		synchronized(currMarks) {
			for (Mark mark : currMarks) {
				report.addMetrics(Metrics.convert(mark, window));
				mark.clear();
			}
		}

		Snapshot snapshot = new Snapshot(report);
		int curVer = version.incrementAndGet();
	    snapshot.setVersion(curVer);
	    snapshot.setMinVersion(curVer);
	    snapshot.setMaxVersion(curVer);

		lcheck = System.currentTimeMillis();
		
		return snapshot;
    }

    void initTimes() {
//    	isFinished = false;
        runtime = 0L;
        lrsample = lbegin = begin = lcheck = curr = start = System.currentTimeMillis();
        frsample = end = Long.MAX_VALUE;
    }

    void initLimites() {
        Mission mission = workerContext.getMission();
        totalOps = mission.getTotalOps() / mission.getTotalWorkers();
        totalBytes = mission.getTotalBytes() / mission.getTotalWorkers();
        if (mission.getRuntime() == 0)
            return;
        begin = start + mission.getRampup() * 1000;
        end = begin + mission.getRuntime() * 1000;
        runtime = end + mission.getRampdown() * 1000;
    }

    void initMarks() {
        Set<String> types = new LinkedHashSet<String>();
        for (OperatorContext op : operatorRegistry)
            types.add(op.getOpType());  //  getMarkType(op.getOpType(), op.getSampleType()));
        for (String type : types)
            currMarks.addMark(newMark(type));
        for (String type : types)
            globalMarks.addMark(newMark(type));
    }

	@Override
	public synchronized void onStats(ExecContext context, boolean status) {
		ExecContext exCtx = (ExecContext)context;
		long duration = System.currentTimeMillis() - exCtx.timestamp;
		String opType = exCtx.getOpType();
		Date now = new Date();
		Sample sample = new Sample(now, opType, status, duration, exCtx.getLength());		
		
//		workerContext.getLogger().info("Request is {} in {} milliseconds.", (status? "succeed" : "failed"), duration);
	
		updateSampleList(sample);
	}
    
}
