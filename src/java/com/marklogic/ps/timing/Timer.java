/*
 * Copyright (c)2005-2006 Mark Logic Corporation
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
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */

package com.marklogic.ps.timing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/*
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class Timer {
    private long errors = 0;

    private long bytes = 0;

    private long duration = -1;

    private ArrayList<TimedEvent> events = new ArrayList<TimedEvent>();

    private long start;

    private long eventCount;

    public Timer() {
        start = System.currentTimeMillis();
        eventCount = 0;
    }

    public void add(TimedEvent event) throws TimerEventException {
        // in case the user forgot to call stop(): note that bytes won't be
        // counted!
        event.stop();
        synchronized (events) {
            bytes += event.getBytes();
            if (event.isError()) {
                errors++;
            }
            events.add(event);
            eventCount++;
        }
    }

    /**
     * @param _timer
     */
    public void add(Timer _timer) {
        _timer.stop();
        synchronized (events) {
            bytes += _timer.getBytes();
            errors += _timer.getErrorCount();
            events.addAll(_timer.events);
            eventCount += _timer.eventCount;
        }
    }

    /**
     * @return
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * @return
     */
    public long getEventCount() {
        return eventCount;
    }

    /**
     * @return
     */
    public long getDuration() {
        if (duration < 0)
            return (System.currentTimeMillis() - start);

        return duration;
    }

    /**
     * @return
     */
    public long getMeanOfEvents() {
        if (eventCount < 1)
            return 0;

        long sum = 0;
        for (int i = 0; i < eventCount; i++) {
            sum += ((TimedEvent) events.get(i)).getDuration();
        }

        return Math.round((double) sum / eventCount);
    }

    /**
     * @return
     */
    public long getPercentileDuration(int p) {
        if (eventCount < 1)
            return 0;

        double size = eventCount;
        Comparator<TimedEvent> c = new TimedEventDurationComparator();
        Collections.sort(events, c);
        int pidx = (int) ((double) p * (double) size * (double) .01);
        return ((TimedEvent) events.get(pidx)).getDuration();
    }

    /**
     * @return
     */
    public long getMaxDuration() {
        long max = 0;
        for (int i = 0; i < eventCount; i++)
            max = Math.max(max, ((TimedEvent) events.get(i)).getDuration());
        return max;
    }

    /**
     * @return
     */
    public long getMinDuration() {
        long min = Integer.MAX_VALUE;
        for (int i = 0; i < eventCount; i++)
            min = Math.min(min, ((TimedEvent) events.get(i)).getDuration());
        return min;
    }

    /**
     * @return
     */
    public long getMeanOverall() {
        return getDuration() / eventCount;
    }

    /**
     * @return
     */
    public long getStart() {
        return start;
    }

    public double getThroughput() {
        // kB per second
        return (((double) bytes / 1024) / ((double) getDuration() / 1000));
    }

    public double getEventRate() {
        // events per second
        return (((double) eventCount) / ((double) getDuration() / 1000));
    }

    /**
     * @param l
     */
    public long stop() {
        return stop(System.currentTimeMillis());
    }

    /**
     * @param l
     */
    public synchronized long stop(long l) {
        if (duration < 0)
            duration = l - start;

        return duration;
    }

    /**
     * @return
     */
    public long getKiloBytes() {
        return (long) ((double) bytes / 1024);
    }

    /**
     * @return
     */
    public long getErrorCount() {
        return errors;
    }

    /**
     * 
     */
    public void incrementEventCount() {
        eventCount++;
    }

    /**
     * @param count
     */
    public void incrementEventCount(int count) {
        eventCount += count;
    }

}
