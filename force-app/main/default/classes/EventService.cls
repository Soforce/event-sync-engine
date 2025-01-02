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

    // public enum ProcessModeEnum { Realtime, Batch }

    public virtual class EventResult {
        // Indicate if the process is success or failure
        public Boolean isSuccess;
        // Returns the incoming event Id from the message queue.
        public String eventId;
        // If an error occurred, returns the exception of the error. If no error occurred, returns null.
        public Exception error;
        // The event entity data element deserialized from the event resource JSON
        public transient EventEntity event;
        // The event staging record

        public EventResult(String eventId) {
            isSuccess = true;
            this.eventId = eventId;
        }
    }

    public String eventType;
    // public ProcessModeEnum processMode;
    public Integer batchSize;

  
    
    public EventService() {
    }
    

    /**
     * @description
     */
    protected abstract EventEntity deserializeEventPayload(String eventType, String eventPayload);
    protected abstract void processEvent(String eventType, String eventId, Datetime eventTime, EventEntity event);
    protected abstract String getEventMasterId(EventEntity event);
    protected abstract String getEventTag(EventEntity event);
    /**
     * @description Preprocess the payload and return the corresponding EventEntity record for the given event type.
     */
    public virtual EventEntity preprocessEvent(String eventType, String eventId, Datetime eventTime, String eventPayload) {
        try {
            EventEntity event = deserializeEventPayload(eventType, eventPayload);
            event.setCharacteristicValue('MasterId', getEventMasterId(event));
            event.setCharacteristicValue('Tag', getEventTag(event));

            return event;
        } catch (Exception ex) {
            throw new InvalidEventException('The event payload is invalid. \n' + ex.getMessage(), ex);
        }
    }

    /**
     * @description ...
     */
    public virtual void processEvent(EventEntity event) {
        // Process the incoming event 
        Savepoint rollbackSp = Database.setSavepoint();
        try {
            processEvent(event.eventType, event.eventId, event.eventTime, event);
        } catch (QueryException ex) {
            // TODO...
            // Handle SELECT FOR UPDATE exception
            throw ex;
        } catch (System.DmlException ex) {
            Database.rollback(rollbackSp);
            if (StatusCode.UNABLE_TO_LOCK_ROW == ex.getDmlType(0)) {
                throw new EventService.RetriableEventException('The event that experienced a record-lock issue will be retried later.\n' + ex.getMessage(), ex);
            } else {
                throw ex;
            }
        } catch (Exception ex) {
            Database.rollback(rollbackSp);

            // re-throw to handle the event staging record after rolling back the changes from event service provider
            throw ex;   
        }
    }

    // /**
    //  * @description ...
    //  */
    // public abstract EventResult[] processEvents(EventEntity[] events);




    /**
     * @description ...
     */
    public String getMasterId(EventEntity event) { 
        return (String)event.getCharacteristicValue('MasterId'); 
    }

    /**
     * @description ...
     */
    public String getTag(EventEntity event) {
        return (String)event.getCharacteristicValue('Tag'); 
    }

}