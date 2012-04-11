package MovingAverage;

import java.util.LinkedList;

public class SlidingWindow {

	
	LinkedList<TimeSeriesDataPoint> oCurrentWindow; 
	
	long lWindowSize;
	long lSlideIncrement;
	long lCurrentTime;
	
	public SlidingWindow( long WindowSizeInMS, long SlideIncrement) {
	
		this.lWindowSize = WindowSizeInMS;
		this.lSlideIncrement = SlideIncrement;
		this.lCurrentTime = 0;
		
		this.oCurrentWindow = new LinkedList<TimeSeriesDataPoint>();
		
	}
	
	public long getWindowStepSize() {		
		return this.lSlideIncrement;		
	}
	
	public long getWindowSize() {
		return this.lWindowSize;
	}
	
	public boolean isWindowFull() {
		
		if ( this.getWindowDelta() >= this.lWindowSize ) {
			return true;
		}
		
		return false;
		
	}
	
	public long getWindowDelta() {
		
		if ( this.oCurrentWindow.size() > 0 ) {
			return this.oCurrentWindow.getLast().lTimestamp - this.oCurrentWindow.getFirst().lTimestamp + 1;
		}
		
		return 0;
		
	}
	
	public void addPoint( TimeSeriesDataPoint point ) throws Exception {

		if ( this.oCurrentWindow.size() > 0) {
			if ( point.lTimestamp <= this.oCurrentWindow.getLast().lTimestamp ) {
				throw new Exception( "Point out of order!" );
			}
		}
		
		this.oCurrentWindow.add( point );
		
	}
	
	public int getNumberPointsInWindow() {
		
		return this.oCurrentWindow.size();
		
	}

	public void SlideWindowForward() {
		
		long lCurrentFrontTS = this.oCurrentWindow.getFirst().lTimestamp; 
		this.lCurrentTime = lCurrentFrontTS + this.lSlideIncrement;
		
		while ( this.oCurrentWindow.getFirst().lTimestamp < this.lCurrentTime ) {
			this.oCurrentWindow.removeFirst();
		}				
	}	


	public LinkedList<TimeSeriesDataPoint> getCurrentWindow() {
		return this.oCurrentWindow;
	}
}