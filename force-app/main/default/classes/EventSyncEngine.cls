public with sharing class EventSyncEngine {
    // Maximum payload size is 128KB which is maximum lenght of the "Long Text Area" field. 
    public static final Integer MaxPayloadSize = 131072;

    /**
     * @description helper method 
     */
    public static String getErrorMessage(Exception ex) {
        return ex.getTypeName() + ': ' + ex.getMessage() + '\n' + ex.getStackTraceString() + '\n';
    }



    public EventSyncEngine() {

    }


    /**
     * @description Instantiate and return the EventService Apex class by the event type.
     */
    public static EventService getEventServiceProvider(String eventType) {
        EventService provider;
        for (Event_Service_Provider__mdt providerMdt : Event_Service_Provider__mdt.getAll().values()) {
            if (providerMdt.Event_Type__c == eventType) {
                try {
                    Type t = Type.forName(providerMdt.Service_Provider__c);
                    provider = (EventService)t.newInstance();
                    // Initialize provider attributes
                    provider.eventType = eventType;
                    provider.batchSize = providerMdt.Batch_Size__c == null ? 1 : (Integer)providerMdt.Batch_Size__c;
                    if (providerMdt.Process_Mode__c == 'Realtime') {
                        provider.processMode = EventService.ProcessModeEnum.Realtime;
                    } else if (providerMdt.Process_Mode__c == 'Batch') {
                        provider.processMode = EventService.ProcessModeEnum.Batch;
                    } else if (providerMdt.Process_Mode__c == 'Throttled Realtime') {
                        provider.processMode = EventService.ProcessModeEnum.ThrottledRealtime;
                    } else {
                        throw new EventService.EventServiceException('The "' + providerMdt.Process_Mode__c + '" process mode for the ' + eventType + ' is invalid.');
                    }
        
                    return provider;            
                } catch (Exception ex) {
                    throw new EventService.EventServiceException('Cannot instantiate the service provider for the "' + eventType + '" type of event.\n' + ex.getMessage(), ex);
                }
            }
        }

        if (provider == null) {
            throw new EventService.EventServiceException('The "' + eventType + '" event type is not supported.');
        }
    }

    /**
     * @description interface method to be called for new event
     */
    public Result processEvent(String eventType, String eventId, Datetime eventTime, String eventPayload) {
        Result result = new Result();
        Event_Staging_Record__c payloadObj = new Event_Staging_Record__c(
            Event_Id__c = eventId,
            Event_Type__c = eventType,
            Event_Time__c = eventTime,
            Payload__c = eventPayload
        );
        Savepoint rollbackSp;

        try {
            // Check payload size
            if (eventPayload != null && eventPayload.length() > MaxPayloadSize) {
                payloadObj.Payload__c = eventPayload.left(MaxPayloadSize); // Trim
                throw new EventService.PayloadTooLargeException('The event payload exceeds the maximum size of ' + String.valueOf(MaxPayloadSize) + '.');
            }

            // Get event service provider
            EventService provider = getEventServiceProvider(eventType);

            // Preprocess the event 
            EventEntity event = provider.preprocess(eventType, eventId, eventTime, eventPayload);
            String eventMasterId = provider.getMasterId(event);

            Event_Staging_Record__c[] stagingRecs = [
                SELECT Id, Event_Id__c, Event_Time__c, Master_Id__c FROM Event_Staging_Record__c 
                WHERE Event_Id__c = :eventId OR (
                    Master_Id__c = :eventMasterId AND Event_Type__c = :eventType AND Event_Time__c > :eventTime 
                    AND Status__c IN ('Pending', 'Completed', 'Queued', 'Retry')
                )
                ORDER BY Event_Time__c
            ];

            Boolean outOfOrderEvent = false;
            for (Event_Staging_Record__c stagingRec : stagingRecs) {
                if (stagingRec.Event_Id__c == eventId) {
                    // Check duplicate event by the event Id
                    throw new EventService.DuplicateEventException('The provided "' + eventId + '" event Id is duplicated.');
                } else {
                    outOfOrderEvent = true;
                }
            }

            // Out-of-order event by the event time
            if (outOfOrderEvent) {
                throw new EventService.IgnorableEventException('This is an out-of-order event, as a later event of the same type and master ID has already been received.');
            }

            
            // Process the incoming event 
            rollbackSp = Database.setSavepoint();
            provider.processEvent(event);

        } catch (EventService.PayloadTooLargeException ex) {
            payloadObj.Error_Message__c = getErrorMessage(ex);
        } catch (TfB_EventService.InvalidEventException ex) {
            // BAD_REQUEST
            logException(ex);
            result = new Result(400, ex); 
        } catch (DuplicateEventException ex) {
            logException(ex);
            result = new Result(409, ex);
        } catch (Exception ex) {
            logException(ex);
            result = new Result(500, ex);
        } finally {
            insert payloadObj;
        }

        return result;
    }

}