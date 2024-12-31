/**
 * @description Base event service class that must be extended by the child event service provider.
 */
public abstract inherited sharing class EventService {

    public class EventServiceException extends Exception {}

    // Exception thrown for a duplicated event with the same event Id.
    public class DuplicateEventException extends Exception {}
    // Exception thrown when the event payload exceeds the size limit (128KB).
    public class PayloadTooLargeException extends Exception {}
    // Exception thrown for an invalid event, including invalid event type, event payload, and other related issues.
    public class InvalidEventException extends Exception {}
    // Exception thrown for an event that can be ignored or skipped.
    public class IgnorableEventException extends Exception {}
    // Exception thrown for an event that is queued for batch process.
    public class QueuedEventException extends Exception {}
    // Exception thrown for an event to be retried later, with the most common reason being read-lock issues.
    public class RetriableEventException extends Exception {}
    // Exception thrown when an event needs to be placed under Pending status due to a dependency.
    public class PendingEventException extends Exception {
        public String dependencyMasterId;
        public String dependencyEventType;
        public PendingEventException(String masterId, String eventType, String errorMessage) {
            super.setMessage(errorMessage);
            this.dependencyMasterId = masterId;
            this.dependencyEventType = eventType;
        }
    }    

    public enum ProcessModeEnum { Realtime, ThrottledRealtime, Batch }

    public class EventResult {
        // Indicate if the process is success or failure
        public Boolean isSuccess;
        // Returns the incoming event Id from the message queue.
        public String eventId;
        // If an error occurred, returns the exception of the error. If no error occurred, returns null.
        public Exception error;
        // The event entity data element deserialized from the event resource JSON
        protected transient EventEntity event;
        // The event staging record

        public EventResult(String eventId) {
            isSuccess = true;
            this.eventId = eventId;
        }
    }

    public String eventType;
    public ProcessModeEnum processMode;
    public Integer batchSize;


    // // Exception thrown when an event needs to be placed under Pending status due to a dependency.
    // public class RetriableEventException extends Exception {
    //     public String dependencyMasterId;
    //     public String dependencyEventType;
    //     // public Boolean queuingFailed;
    //     // public Boolean skipFutureProcessing;
    // }
    
    
    // public class FatalErrorException extends Exception {}
    // public class NotImplementedException extends Exception {}
    // public class ResultMismatchException extends Exception{}
    
    
    public EventService() {
    }
    
    /**
     * @description Preprocess the payload and return the corresponding EventEntity record for the given event type.
     */
    public abstract EventEntity preprocessEvent(String eventType, String eventId, Datetime eventTime, String eventPayload);
    /**
     * @description ...
     */
    public abstract void processEvent(EventEntity event);

    /**
     * @description ...
     */
    public abstract EventResult[] processEvents(EventEntity[] events);

    /**
     * @description ...
     */
    public abstract String getMasterId(EventEntity event);
    /**
     * @description ...
     */
    public abstract String getTag(EventEntity event);

    // public class Result extends EventEntity{
        //         public string status; 
    //         public Exception thrownException;
    // }



//     /**
//      * @description process a single incoming event
//      */
//     public virtual void process(String eventType, String eventId, Datetime eventTime, String eventPayload) {
//         EventEntity evt = preprocessEvent(eventId, eventTime, eventType, eventPayload);

//         // Check the payload size
//         if (eventPayload.length() > MaxPayloadSize) {
//             throw new PayloadTooLargeException('The event payload exceeds the maximum size of ' + String.valueOf(MaxPayloadSize) + '.');
//         }

//         // Check the duplicated events with the same event Id
//         // TOTO

//         // Check

//         processEvent(evt);
//     }

//     // /**
//     //  * @description process the incoming events in batch mode
//     //  */
//     // public virtual EventResult[] processEvents(String eventType, String[] eventIds, Datetime[] eventTimes, String[] eventPayloads) {
//     //     EventResult[] erList = preprocessEvents(eventType, eventIds, eventTimes, eventPayloads);
//     //     // Check duplicated events

//     //     // Check dependent events

//     //     // Check igorable events???

//     //     EventEntity[] evts = new List<EventEntity>();
//     //     for (EventResult er : erList) {
//     //         if (er.isSuccess) {
//     //             evts.add(er.eventEntity);
//     //         }
//     //     }

//     //     if (evts.size() > 0) {
//     //         EventResult[] erList2 = processEvents(evts);
//     //     }
        
//     // }

//     protected abstract void processEvent(EventEntity evt);



//     protected virtual EventResult[] preprocessEvents(String eventType, String[] eventIds, Datetime[] eventTimes, String[] eventPayloads) {
//         EventResult[] erList = new List<EventResult>();
//         for (Integer i = 0; i < eventId.size(); i++) {
//             EventResult er = new EventResult(eventId);
//             try {
//                 EventEntity evt = processEvent(processEvent(eventType, eventId, eventTime, eventPayload));
//                 er.eventEntity = evt;
//             } catch (Exception ex) {
//                 er.error = ex;
//                 er.isSuccess = false;
//             }
//             erList.add(er);
//         }

//         return erList;
//     }

//     protected virtual EventResult[] processEvents(EventEntity[] evts) {
//         throw new EventServiceException('The batch mode is NOT implemented for the given event type.');
//     }





//     protected abstract void process(String eventId, Datetime eventTime, String eventType, String eventPayload, Map<String, Object> context);
//     protected virtual List<EventResult> process(List<EventEntity> events, List<String> payloads, Map<String, Object> context) {
//         throw new NotImplementedException('Event Service provider should provide the implementation');
//     }
   
    
//     protected abstract String getMasterId(Map<String, Object> context);
//     protected abstract String getTag(Map<String, Object> context);
//     public virtual String getTag(Map<String,Object> ctx, String masterId) {
//         throw new NotImplementedException('Event Service provider should provide the implementation');
//     }
//     protected virtual ProcessMode getSupportedMode(Map<String, Object> context) {
//         throw new NotImplementedException('Event Service provider should provide the implementation');
//     }
//     protected ProcessMode getSupportedMode(String process) {
//        return process == 'Realtime with Throttling' ? ProcessMode.REALTIME_WITH_THROTTLING : 
//        process == 'Batch' ? ProcessMode.BATCH : ProcessMode.REALTIME;
//     }

//     protected virtual String getPriority(Map<String, Object> context) {
//         throw new NotImplementedException('Event Service provider should provide the implementation');
//     }
//     protected virtual Integer getBatchSize(Map<String, Object> context) {
//         throw new NotImplementedException('Event Service provider should provide the implementation');
//     }
//     /**
//      * @description Returns the supported batch sie in Queueable mode
//      */
//     public virtual Integer getBatchSize() {
//         Integer batchSize = 1;
//         try {
//             batchSize = getBatchSize(context);
//         } catch (Exception ex) {
//             logException(ex);
//         }     
//         return batchSize;
//     }

//     /**
//      * @description Returns priority of event 
//      */
//     public virtual String getPriority() {
//         String priority = 'High';
//         try {
//             priority = getPriority(context);
//         } catch (Exception ex) {
//             logException(ex);
//         }     
//         return priority;
//     }

//     /**
//      * @description Returns supported process mode for the Event Service Provider
//     */
//     public ProcessMode getSupportedMode(){
//         ProcessMode mode = ProcessMode.REALTIME;
//         try {
//             mode = getSupportedMode(context);
//         } catch (Exception ex) {
//             logException(ex);
//         }     
//         return mode;
//     }
//     /**
//      * @description context variable for the event process
//      */
//     protected Map<String, Object> context = new map<String,Object>();
//     public Map<String, Object> getContext() {
//         return context;
//     }

//     public TfB_EventService() {
//     }

//     protected void logException(Exception ex) {
//         System.debug(ex.getTypeName() + ': ' + ex.getMessage() + '\n' + ex.getStackTraceString());
//     }
//     @TestVisible
//     protected void logMessage(String message) {
//         System.debug(message);
//     }


//     public void processEvent(String eventId, Datetime eventTime, String eventType, String eventPayload) {
//         processEvent(eventId, eventTime, eventType, eventPayload, false);
//     }
   
//     /**
//      * @description major entry method to process the event
//      */
//     public void processEvent(String eventId, Datetime eventTime, String eventType, String eventPayload, Boolean reprocess) {
//         context = new Map<String, Object> {
//             'event' => new EventEntity(eventId, eventType, eventTime),
//             'reprocess' => reprocess
//         };
//         Savepoint evtSp = setSavepoint();
//         try {
//             // preprocess the payload and return the masterId of the event
//             String masterId = preprocess(eventId, eventTime, eventType, eventPayload, context);
//             if (masterId != null && !reprocess) {
//                 TfB_EventNotificationEngine engine = new TfB_EventNotificationEngine(); 
//                 // Check if a latter event (by eventTime) of the same master Id has been process, if yes, then ignore the current event
//                 if (engine.checkOutOfOrderEvent(masterId, eventId, eventTime, eventType)) {
//                     throw new IgnorableEventException('This event has been ingored because it is an out-of-order event.');
//                 }
//             }

//             // execute the event process provided by the child EventProvider class
//             process(eventId, eventTime, eventType, eventPayload, context);
//         } catch (System.DmlException ex) {
//             if(evtSp != null) Database.rollback(evtSp);

//             // TfB_TEST_UNABLE_TO_LOCK_ROW used for internal test purpose only 
//             if (ex.getMessage() == 'TfB_TEST_UNABLE_TO_LOCK_ROW' || StatusCode.UNABLE_TO_LOCK_ROW == ex.getDmlType(0)) {
//                 throw new RetriableEventException('The record will be retried automatically three times.\n' + ex.getMessage(), ex);
//             } else {
//                 throw ex;
//             }   
//         } catch (Exception ex) {
//             // logException(ex);
//             if(evtSp != null) Database.rollback(evtSp);
//             throw ex;
//         } finally {
//             // Long d2 = Datetime.now().getTime();

//             // logMessage('PERF_DATA: ' + Limits.getCpuTime() + ', ' + Limits.getQueries() + ', ' + Limits.getDMLStatements() + ', ' + Limits.getHeapSize());
//         }

//     }
//     /**
//      * @description entry method to process events in bulk
//      */
//     public virtual List<Result> processEvents(List<EventEntity> events, List<String> payloads, Map<String, Object> context){
//         Savepoint evtSp = setSavepoint();
//         try {

//             List<Result> results = process(events, payloads, context);
//             if(results.size() != events.size()) {
//                 throw new ResultMismatchException('The number of results does not match the number of events sent for processing.');
//             }
//             return results;
//         } catch (Exception ex) {
//             if(evtSp != null) Database.rollback(evtSp);
//             TfB_EventNotificationEngine.logException(ex);
//             throw ex;
//         }
//     }

//     /**
//      * @description helper method to throw the PendingEventException
//      */
//    /* @testvisible
//     protected void throwPendingEventException(String relyOnId, Exception innerEx) {
//         PendingEventException ex;// = new PendingEventException(innerEx.getMessage(), innerEx);
//         if (innerEx != null) ex = new PendingEventException(innerEx.getMessage(), innerEx);
//         else 
//             ex = new PendingEventException();
//         ex.relyOnId = relyOnId;
//         throw ex;
//     }*/

//     /**
//      * @description return the master Id from the event
//      */
//     public String getMasterId() {
//         String masterId = ''; 
//         try {
//             masterId = getMasterId(context);
//             if (masterId.length() > 128) masterId = masterId.left(128);
//         } catch (Exception ex) {
//             logException(ex);
//         }     
//         return masterId;
//     }

//     /**
//      * @description return the tag from the event
//      */
//     public String getTag() {
//         String tag = ''; 
//         try {
//             tag = getTag(context);
//             if (tag.length() > 255) tag = tag.left(255);
//         } catch (Exception ex) {
//             logException(ex);
//         } finally {
//         }        
//         return tag;
//     }

//     /**
//      * @description get the memory dumps from the event processing
//      */
//     public String getMemoryDumps() {
//         String dumps = '';
//         try {
//             dumps = getMemoryDumps(context);
//             if (dumps.length() > TfB_EventNotificationEngine.MaxLength) dumps = dumps.left(TfB_EventNotificationEngine.MaxLength);
//         } catch (Exception ex) {
//             logException(ex);
//         }        
//         return dumps;            
//     }

//     /**
//      * @description default implementation of the memory dumps from the event processing
//      */
//     protected virtual String getMemoryDumps(Map<String, Object> context) {
//         return JSON.serialize(context);
//     }

//     protected virtual Savepoint setSavepoint() {
//         return  Database.setSavepoint();
//     }    
}