public virtual without sharing class EventSyncEngine {
    // Maximum payload size is 128KB which is maximum lenght of the "Long Text Area" field. 
    public static final Integer MaxPayloadSize = 131072;
    // Maximum number of retries executed by the engine
    public static final Integer MaxRetryCount = 5;

    /**
     * @description 
     */
    public class EventResult extends EventService.EventResult {
        protected transient Event_Staging_Record__c stagingRecord;

        public EventResult(Event_Staging_Record__c stagingRecord) {
            super(stagingRecord.Event_Id__c);

            this.stagingRecord = stagingRecord;
        }
    }


     public EventSyncEngine() {

    }


    /**
     * @description helper method 
     */
    public static String getErrorMessage(Exception ex) {
        return ex.getTypeName() + ': ' + ex.getMessage() + '\n' + ex.getStackTraceString() + '\n';
    }

    /**
     * @description Helper method to retrieve Event Staging Record by Id
     */
    public Event_Staging_Record__c getEventRecord(String recordId) {
        return [
            SELECT Id, Event_Id__c, Event_Type__c, Event_Time__c, Status__c, Payload__c, Master_Id__c, Retry_Count__c, Error_Message__c 
            FROM Event_Staging_Record__c 
            WHERE Id = :recordId
        ];
    }

    public Event_Staging_Record__c getNextRetryEventRecord(String eventType) {
        try {
            return [
                SELECT Id, Event_Id__c, Event_Type__c, Event_Time__c, Status__c, Payload__c, Master_Id__c, Retry_Count__c, Error_Message__c 
                FROM Event_Staging_Record__c 
                WHERE Event_Type__c = :eventType AND Status__c = 'Retry'
                ORDER BY Retry_Count__c, Event_Time__c
                LIMIT 1
            ];        
        } catch (Exception ex) {
            return null;
        }
    }


    /**
     * @description Helper method to check if the specified event is outdated where later event with the same type and event master Id has been received.
     */
    private Boolean isOutDatedEvent(Event_Staging_Record__c event) {
        Integer laterEvtCount = [
            SELECT Count() FROM Event_Staging_Record__c 
            WHERE Master_Id__c = :event.Master_Id__c AND Event_Type__c = :event.Event_Type__c AND Event_Time__c > :event.Event_Time__c 
                AND Status__c IN ('Pending', 'Completed', 'Queued', 'Retry')
        ];


        // The current event is out-of-date if there are event(s) with same type and master Id arriving later
        return laterEvtCount > 0;
    }

    /**
     * @description Helper method to retrieve the pending events which has dependency on the master event
     */
    private List<Id> getDependentEventIds(Event_Staging_Record__c masterEvt) {
        if (masterEvt.Master_Id__c != null) {
            Event_Staging_Record__c[] dependentEvts = [
                SELECT Id, Event_Id__c, Event_Time__c, Event_Type__c, Dependency_Id__c, Dependency_Type__c 
                FROM Event_Staging_Record__c 
                WHERE Dependency_Id__c = :masterEvt.Master_Id__c AND Dependency_Type__c = :masterEvt.Event_Type__c AND Id <> :masterEvt.Id AND Status__c = 'Pending'
            ];
            if (dependentEvts.size() > 0) {
                return new List<Id>((new Map<Id, Event_Staging_Record__c>(dependentEvts)).keySet());
            }
        }

        return new List<Id>();
    }

    /**
     * @description Instantiate and return the EventService Apex class by the event type.
     */
    private EventService getEventServiceProvider(String eventType) {
        EventService provider;
        for (Event_Service_Provider__mdt providerMdt : Event_Service_Provider__mdt.getAll().values()) {
            if (providerMdt.Event_Type__c == eventType) {
                try {
                    Type t = Type.forName(providerMdt.Service_Provider__c);
                    provider = (EventService)t.newInstance();
                    provider.eventType = eventType;
                    provider.batchSize = providerMdt.Batch_Size__c == null ? 1 : (Integer)providerMdt.Batch_Size__c;
        
                    return provider;            
                } catch (Exception ex) {
                    throw new EventService.EventServiceException('Cannot instantiate the service provider for the "' + eventType + '" type of event.\n' + ex.getMessage(), ex);
                }
            }
        }

        throw new EventService.EventServiceException('The "' + eventType + '" event type is not supported.');
    }

    /**
     * @description Receive the event from the message queue and process. This method needs to be exposed as REST API endpoint. 
     */
    public Event_Staging_Record__c processEvent(String eventJSON) {
        try {
            EventEntity ee = (EventEntity)JSON.deserialize(eventJSON, EventEntity.class);

            // Initialize the event stage record
            Event_Staging_Record__c eventStagingObj = new Event_Staging_Record__c(
                Event_Id__c = ee.eventId,
                Event_Type__c = ee.eventType,
                Event_Time__c = ee.eventTime,
                Payload__c = eventJSON,
                Retry_Count__c = 0
            );

            return processEvent(eventStagingObj);
        } catch (Exception ex) {
            throw new EventService.InvalidEventException('Invalid event payload JSON.\n' + ex.getMessage(), ex);
        }
    }

    /**
     * @description Receive the event from the message queue and process. This method needs to be exposed as REST API endpoint. 
     */
    public Event_Staging_Record__c processEvent(String eventType, String eventId, Datetime eventTime, String eventPayload) {
        // Initialize the event stage record
        Event_Staging_Record__c eventStagingObj = new Event_Staging_Record__c(
            Event_Id__c = eventId,
            Event_Type__c = eventType,
            Event_Time__c = eventTime,
            Payload__c = eventPayload,
            Retry_Count__c = 0
        );

        return processEvent(eventStagingObj);
    }

    /**
     * @description Retry the event manually within Salesforce. 
     */
    public Event_Staging_Record__c retryEvent(Id retryEventId) {
        Event_Staging_Record__c retryEvent = getEventRecord(retryEventId);

        return processEvent(retryEvent);
    }

    /**
     * @description Process the event with pending status once the dependency of the given event has been resolved. 
     */
    protected Event_Staging_Record__c processPendingEvent(Id pendingEventId) {
        // The dependent events for the current Pending event
        List<Id> myDependentEventIds = new List<Id>();

        Event_Staging_Record__c pendingEvent = getEventRecord(pendingEventId);
        if (pendingEvent.Status__c != 'Pending') {
            // If the staging record status has been updated from other process, then we will skip the process.
            return pendingEvent;
        } else {
            return processEvent(pendingEvent);
        }
    }

    /**
     * @description
     */
    protected void processRetryEvent(Id retryEventId) {
        Event_Staging_Record__c retryEvent = getEventRecord(retryEventId);
        if (retryEvent.Status__c != 'Retry') {
            // The given retry event has been processed by other process.`
            return;
        } else {
            processEvent(retryEvent);
        }
    }

    /**
     * 
     */
    public Event_Staging_Record__c processEvent(Event_Staging_Record__c eventStagingRecord) {
        try {
            // Check payload size and throw PayloadTooLargeException if needed
            if (eventStagingRecord.Id != null && eventStagingRecord.Payload__c != null && eventStagingRecord.Payload__c.length() > MaxPayloadSize) {
                eventStagingRecord.Payload__c = eventStagingRecord.Payload__c.left(MaxPayloadSize); // Trim
                throw new EventService.PayloadTooLargeException('The event payload exceeds the maximum size of ' + String.valueOf(MaxPayloadSize) + '.');
            }

            // Get event service provider
            EventService provider = getEventServiceProvider(eventStagingRecord.Event_Type__c);

            // Preprocess the event
            EventEntity event = provider.preprocessEvent(eventStagingRecord.Event_Type__c, eventStagingRecord.Event_Id__c, eventStagingRecord.Event_Time__c, eventStagingRecord.Payload__c);

            // Store the event Master Id and Tag
            eventStagingRecord.Master_Id__c = provider.getMasterId(event);
            eventStagingRecord.Tag__c = provider.getTag(event);
            
            // Check duplicate event by the event Id only for new event
            if (eventStagingRecord.Id == null) {
                Integer dupEvtCount = [SELECT Count() FROM Event_Staging_Record__c WHERE Event_Id__c = :event.eventId];
                if (dupEvtCount > 0) {
                    throw new EventService.DuplicateEventException('The provided "' + event.eventId + '" event Id is duplicated.');
                }
            }            

            if (isOutDatedEvent(eventStagingRecord)) {
                throw new EventService.IgnorableEventException('This is an outdated event, as a later event of the same type and master ID has already been received.');
            }
            
            // Process the incoming event 
            provider.processEvent(event);

            eventStagingRecord.Status__c = 'Completed';
            eventStagingRecord.Status_Code__c = 200;

            List<Id> dependentPendingEvtIds = getDependentEventIds(eventStagingRecord);
            if (dependentPendingEvtIds.size() > 0) {
                EventDependencyQueueable.enqueueDependencyQueueableJob(dependentPendingEvtIds, 0);
            }
        } catch (EventService.PayloadTooLargeException ex) {
            eventStagingRecord.Error_Message__c = getErrorMessage(ex);
            eventStagingRecord.Status__c = 'Error';
            eventStagingRecord.Status_Code__c = 413;
        } catch (EventService.DuplicateEventException ex) {
            eventStagingRecord.Error_Message__c = getErrorMessage(ex);
            eventStagingRecord.Status__c = 'Error';            
            eventStagingRecord.Status_Code__c = 409;
        } catch (EventService.RetriableEventException ex) {
            eventStagingRecord.Retry_Count__c += 1;
            if (eventStagingRecord.Retry_Count__c <= MaxRetryCount) {
                eventStagingRecord.Error_Message__c = getErrorMessage(ex);
                eventStagingRecord.Status_Code__c = 202;
                eventStagingRecord.Status__c = 'Retry';
    
                EventRetryQueueable.enqueueRetryQueueableJob(eventStagingRecord.Event_Type__c);
            } else {
                eventStagingRecord.Error_Message__c = 'Maximum retry has been reached.\n' + getErrorMessage(ex);
                eventStagingRecord.Status__c = 'Error';
            }            
        } catch (EventService.PendingEventException ex) {
            eventStagingRecord.Error_Message__c = getErrorMessage(ex);
            eventStagingRecord.Status__c = 'Pending';
            eventStagingRecord.Status_Code__c = 202;
            eventStagingRecord.Dependency_Id__c = ex.dependencyMasterId;
            eventStagingRecord.Dependency_Type__c = ex.dependencyEventType;
        } catch (EventService.IgnorableEventException ex) {
            eventStagingRecord.Error_Message__c = getErrorMessage(ex);
            eventStagingRecord.Status__c = 'Ignored';
            eventStagingRecord.Status_Code__c = 200;            
        } catch (EventService.InvalidEventException ex) {
            eventStagingRecord.Error_Message__c = getErrorMessage(ex);
            eventStagingRecord.Status__c = 'Error';            
            eventStagingRecord.Status_Code__c = 400;            
        } catch (Exception ex) {
            eventStagingRecord.Error_Message__c = getErrorMessage(ex);
            eventStagingRecord.Status__c = 'Error';            
            eventStagingRecord.Status_Code__c = 500;
        } finally {
            if (eventStagingRecord.Id == null) {
                insert eventStagingRecord;
            } else {
                update eventStagingRecord;
            }

        }
        return eventStagingRecord;

    }
}