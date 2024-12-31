public virtual without sharing class EventSyncEngine {
    // Maximum payload size is 128KB which is maximum lenght of the "Long Text Area" field. 
    public static final Integer MaxPayloadSize = 131072;
    // Maximum number of retries executed by the engine
    public static final Integer MaxRetryCount = 5;

    /**
     * @description 
     */
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
        protected transient Event_Staging_Record__c stagingRecord;

        public EventResult(String eventId) {
            isSuccess = true;
            this.eventId = eventId;
        }

        public EventResult(Event_Staging_Record__c stagingRecord) {
            isSuccess = true;
            this.stagingRecord = stagingRecord;
            this.eventId = stagingRecord.Event_Id__c;
        }
    }


    /**
     * @description Dependency Queue Processor to process events whose dependency has been resolved.  
     */
    public without sharing class DependencyQueueProcessor implements Queueable {
        private List<Id> pendingEventIds;
        private Integer index;
        
        public DependencyQueueProcessor(List<String> pendingEventIds, Integer index) {
            this.pendingEventIds = pendingEventIds;
            this.index = index;
        }
        
        public void execute(QueueableContext context) {
            EventSyncEngine engine = new EventSyncEngine();
            Id pendingEvtId = pendingEventIds[index];

            List<Id> newPendingEvtIds = engine.processPendingEvent(pendingEvtId); 
            if (newPendingEvtIds.size() > 0) {
                // Attach the new resolved pending events to the end of the queue
                pendingEventIds.addAll(newPendingEvtIds); 
            }
            if (index + 1 < pendingEventIds.size()) {
                System.enqueueJob(new DependencyQueueProcessor(this.pendingEventIds, index + 1));
            }
        }
    } 

    /**
     * @description Retry queueable processor to retry the events with "Retriable" status
     */
    public without sharing class RetryQueueProcessor implements Queueable {
        private Id retryEventId;
        
        public RetryQueueProcessor(Id retryEventId) {
            this.retryEventId = retryEventId;
        } 

        public void execute(QueueableContext context) {
            EventSyncEngine engine = new EventSyncEngine();
            engine.processRetryEvent(retryEventId);
        }        
    }    

    /**
     * @description throttling queue processor to process queued events
     *  One queue per event type
     */
    public without sharing class ThrottlingQueueProcessor implements Finalizer, Queueable {
        private string transactionId;
        private string eventType;

        public List<Id> currentPayloadIds = new List<Id>();


        public ThrottlingQueueProcessor(String transactionId, String eventType) {
            // EventSyncEngine.InvokedviaSyncEngine = true; //Flag to skip trigger logic
            this.transactionId = transactionId;
            this.eventType = eventType;
        }

        public ThrottlingQueueProcessor(String transactionId, String eventType, List<Id> currentPayloadIds) {
            // EventSyncEngine.InvokedviaSyncEngine = true; //Flag to skip trigger logic
            this.transactionId = transactionId;
            this.eventType = eventType;
            this.currentPayloadIds = currentPayloadIds;
        }

        public void execute(QueueableContext context) {
            EventSyncEngine engine = new EventSyncEngine();

            //Attaching Finalizer
            System.attachFinalizer(new ThrottlingQueueProcessor(this.transactionId, this.eventType, this.currentPayloadIds));
            
            // public List<EventResult> processEvents(String eventType, String transactionId /*, List<Id> batchStagingRecIds */) {
            List<EventResult> erList = engine.processEvents(this.eventType, this.transactionId);
        }

        public void execute(System.FinalizerContext ctx) {
            EventSyncEngine engine = new EventSyncEngine();
            // System.debug('this.currentPayloadIds:'+JSON.serialize(this.currentPayloadIds));
            // Boolean StopQueue = false;
            // if (ctx.getResult() == System.ParentJobResult.UNHANDLED_EXCEPTION) {
            //     Exception unhandledEx = ctx.getException();
            //     StopQueue = unhandledEx instanceOf Tfb_EventService.IgnorableEventException;
            //     EventSyncEngine.logMessage(unhandledEx.getStackTraceString());
            //     List<TfB_Inbound_Payload__c> failedPayloads = new List<TfB_Inbound_Payload__c>();
            //     if(this.currentPayloadIds != null){
            //         for( Id payloadId: this.currentPayloadIds) {
            //             failedPayloads.add(new TfB_Inbound_Payload__c(Id=payloadId, Status__c = 'Error', Error_Message__c= '\n'+Datetime.now() +'Event was stopped due to\n'+ unhandledEx.getTypeName() +'\n'+unhandledEx.getMessage() + '\n'+ unhandledEx?.getStackTraceString()));
            //         }
            //         update failedPayloads;
            //     }
            // }
            // if(StopQueue) return; //Transaction has been ignored
            // //Success
            // List<TFB_Inbound_Payload__c> events = engine.getQueuedEventByEventType(this.eventType);
            // if(events == null || events.isEmpty() ){//|| this.currentPayloadIds == null || this.currentPayloadIds.isEmpty()) {
            //     EventSyncEngine.logMessage('Throttling Queue ended for event type:'+ this.eventType);
            //     return;
            // }else if(!Test.isRunningTest()){
            //     //Enqueue another queueable if there are more events to be processed
            //     System.enqueueJob(new ThrottlingQueueProcessor(this.transactionId, this.eventType));
            // }
        }
    }


    public EventSyncEngine() {

    }


    /**
     * @description helper method 
     */
    static String getErrorMessage(Exception ex) {
        return ex.getTypeName() + ': ' + ex.getMessage() + '\n' + ex.getStackTraceString() + '\n';
    }

    /**
     * @description Helper method to retrieve Event Staging Record by Id
     */
    static Event_Staging_Record__c getEventRecord(String recordId) {
        return [
            SELECT Id, Event_Id__c, Event_Type__c, Event_Time__c, Status__c, Payload__c, Master_Id__c, Retry_Count__c 
            FROM Event_Staging_Record__c 
            WHERE Id = :recordId
        ];
    }

    /**
     * @description Helper method to retrieve queued event records by event type
     */
    public List<Event_Staging_Record__c> getQueuedEventRecords(String eventType, Integer batchSize) {
        return [
            SELECT Id, Event_Id__c, Event_Type__c, Event_Time__c, Status__c, Payload__c, Master_Id__c, Retry_Count__c 
            FROM Event_Staging_Record__c 
            WHERE Event_Type__c = :eventType AND Status__c='Queued' AND Retry_Count__c < :MaxRetryCount // ??? Retry_Count__c < :MaxRetryCount
            ORDER BY Event_Time__c
            LIMIT :batchSize
        ];
    }

    /**
     * @description Helper method to retrieve queued event records by event type
     */
    public Integer getQueuedEventCount(String eventType) {
        return [SELECT Count() FROM Event_Staging_Record__c WHERE Event_Type__c = :eventType AND Status__c='Queued' AND Retry_Count__c < :MaxRetryCount];
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
     * @description Helper method to check if the throttling queue has been activated for the given event type
     */
    private Boolean isThrottlingQueueEnabled(String eventType) {
        // If the throttling queue has been activated for the given event type, the current event needs to put into the throttling queue.
        Integer queuedEvtCount = [SELECT Count() FROM Event_Staging_Record__c WHERE Event_Type__c = :eventType and Status__c = 'Queued'];
        return queuedEvtCount > 0;
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

        throw new EventService.EventServiceException('The "' + eventType + '" event type is not supported.');
    }

    /**
     * @description Helper method to handle record-lock exception
     */
    private Exception handleRecordLockException(Exception ex, EventService.ProcessModeEnum processMode) {
        if (processMode == EventService.ProcessModeEnum.ThrottledRealtime) {
            // The throttling queue is activated because the Record-lock issue is encountered.
            return new EventService.QueuedEventException('The throttling queue has been activated to queue the event due to a record-lock issue.');
        } else {
            return new EventService.RetriableEventException('The event experienced record-lock issue will be retried later.\n' + ex.getMessage(), ex);
        }

    }

    /**
     * @description Helper method to handle exception
     */
    private void handleQueuedEventException(EventService.QueuedEventException ex, Event_Staging_Record__c eventStagingObj) {
        eventStagingObj.Error_Message__c = getErrorMessage(ex);
        eventStagingObj.Status__c = 'Queued';
        eventStagingObj.Status_Code__c = 202;
    }

    /**
     * @description Helper method to handle exception
     *  if the exception is thrown within the DependencyQueueProcessor, the pending event has to be put it back to the DependencyQueue again instead of enqueue the RetryQueueProcessor immediately
     *  This is because ONLY one queueable is allowed in the asynchrous process.
     */
    private void handleRetriableEventException(EventService.RetriableEventException ex, Event_Staging_Record__c eventStagingObj) {
        if (eventStagingObj.Retry_Count__c < MaxRetryCount) {
            eventStagingObj.Error_Message__c = getErrorMessage(ex);
            eventStagingObj.Status_Code__c = 202;
            if (eventStagingObj.Status__c != 'Pending') {
                System.enqueueJob(new RetryQueueProcessor(eventStagingObj.Id), 1);
            } else {
                // This event will be retried by adding it back to the DependencyQueue processor
                eventStagingObj.Retry_Count__c += 1;
            }
            eventStagingObj.Status__c = 'Retry';
        } else {
            eventStagingObj.Error_Message__c = 'Maximum retry has been reached.\n' + getErrorMessage(ex);
            eventStagingObj.Status__c = 'Error';
        }

    }

    /**
     * @description Helper method to handle exception
     */
    private void handlePendingEventException(EventService.PendingEventException ex, Event_Staging_Record__c eventStagingObj) {
        eventStagingObj.Error_Message__c = getErrorMessage(ex);
        eventStagingObj.Status__c = 'Pending';
        eventStagingObj.Status_Code__c = 202;
        eventStagingObj.Dependency_Id__c = ex.dependencyMasterId;
        eventStagingObj.Dependency_Type__c = ex.dependencyEventType;
    }

    /**
     * @description Helper method to handle exception
     */
    private void handleIgnorableEventException(EventService.IgnorableEventException ex, Event_Staging_Record__c eventStagingObj) {
        eventStagingObj.Error_Message__c = getErrorMessage(ex);
        eventStagingObj.Status__c = 'Ignored';
        eventStagingObj.Status_Code__c = 200;
    }
    
    /**
     * @description Helper method to handle exception
     */
    private void handlePayloadTooLargeException(EventService.PayloadTooLargeException ex, Event_Staging_Record__c eventStagingObj) {
        eventStagingObj.Error_Message__c = getErrorMessage(ex);
        eventStagingObj.Status__c = 'Error';
        eventStagingObj.Status_Code__c = 413;
    }

    /**
     * @description Helper method to handle exception
     */
    private void handleInvalidEventException(EventService.InvalidEventException ex, Event_Staging_Record__c eventStagingObj) {
        eventStagingObj.Error_Message__c = getErrorMessage(ex);
        eventStagingObj.Status__c = 'Error';            
        eventStagingObj.Status_Code__c = 400;
    }

    /**
     * @description Helper method to handle exception
     */
    private void handleDuplicateEventException(EventService.DuplicateEventException ex, Event_Staging_Record__c eventStagingObj) {
        eventStagingObj.Error_Message__c = getErrorMessage(ex);
        eventStagingObj.Status__c = 'Error';            
        eventStagingObj.Status_Code__c = 409;
    }

    /**
     * @description Helper method to handle exception
     */
    private void handleException(Exception ex, Event_Staging_Record__c eventStagingObj) {
        eventStagingObj.Error_Message__c = getErrorMessage(ex);
        eventStagingObj.Status__c = 'Error';            
        eventStagingObj.Status_Code__c = 500;
    }

    /**
     * @description Process the events in batch asynchrously. The batch events have to be added to the Event Staging Records first
     * @param transactionId batch transaction Id
     */
    public List<EventResult> processEvents(String eventType, String transactionId /*, List<Id> batchStagingRecIds */) {
        EventResult[] erList = new List<EventResult>();

        try {
            Event_Throttling_Queue__c[] throttlingQueue = [SELECT Id FROM Event_Throttling_Queue__c WHERE Event_Type__c =: eventType /* and Transaction_Id__c = :transactionId */];
            if(throttlingQueue.isEmpty()) {
                // The input batch transaction Id does not match with the transaction Id in the Event_Throttling_Queue__c table. 
                // This is triggered from an outdated ThrottlineQueueProcessor and should be skipped.
                return erList; 
            }
            
            EventService provider = getEventServiceProvider(eventType);

            List<Event_Staging_Record__c> queuedStagingRecs = getQueuedEventRecords(eventType, provider.batchSize);
            if (queuedStagingRecs.isEmpty()) {
                // No more staging the records to process
                return erList;
            }

            Map<String, EventResult> eventResultMap = new Map<String, EventResult>();
            for (Event_Staging_Record__c stagingRec : queuedStagingRecs) {
                EventResult result = new EventResult(stagingRec);
                // Preprocess the event 
                EventEntity event = provider.preprocessEvent(eventType, stagingRec.Event_Id__c, stagingRec.Event_Time__c, stagingRec.Payload__c);
                result.event = event;

                // Get and store the event Master Id and Tag
                stagingRec.Master_Id__c = provider.getMasterId(event);
                stagingRec.Tag__c = provider.getTag(event);
                
                erList.add(result);

                // Detect and handle duplicate event in the same batch
                EventResult prevEvtResult = eventResultMap.put(result.eventId, result);
                if (prevEvtResult != null) {
                    prevEvtResult.isSuccess = false; // duplicate event Id in the same batch
                    prevEvtResult.error = new EventService.DuplicateEventException('The provided "' + result.eventId + '" event Id is duplicated.');
                    handleDuplicateEventException((EventService.DuplicateEventException)prevEvtResult.error, prevEvtResult.stagingRecord);
                }
            }

            // Pick up non-error entity records to be processed.
            List<EventResult> goodErList = new List<EventResult>();
            List<EventEntity> goodEvtList = new List<EventEntity>();
            for (EventResult er : erList) {
                if (er.isSuccess) {
                    goodErList.add(er);
                    goodEvtList.add(er.event);
                }
            }

            List<Id> completeEvtMasterIds = new List<Id>();
            // batch process the events
            SavePoint sp = Database.setSavepoint();
            try {
                List<EventService.EventResult> processedErList = provider.processEvents(goodEvtList);
                for (Integer i = 0; i < processedErList.size(); i++) {
                    goodErList[i].isSuccess = processedErList[i].isSuccess;
                    goodErList[i].error = processedErList[i].error;

                    if (goodErList[i].error == null) {
                        goodErList[i].stagingRecord.Status__c = 'Completed';
                        goodErList[i].stagingRecord.Status_Code__c = 202;

                        completeEvtMasterIds.add(goodErList[i].stagingRecord.Master_Id__c);
                    } else {
                        Exception ex = goodErList[i].error;
                        Event_Staging_Record__c eventStagingObj = goodErList[i].stagingRecord;
                        if (ex instanceOf EventService.IgnorableEventException) {
                            handleIgnorableEventException((EventService.IgnorableEventException)ex, eventStagingObj);
                        } else if (ex instanceOf EventService.PendingEventException) {
                            handlePendingEventException((EventService.PendingEventException)ex, eventStagingObj);                           
                        } else {
                            handleException(ex, eventStagingObj);
                        }
                    }
                }

            } catch (Exception ex) {
                Database.rollback(sp);

                for (EventResult er : goodErList) {
                    er.error = ex;
                    handleException(er.error, er.stagingRecord);
                }
            }
            
            // Update 
            update queuedStagingRecs;

            // Find the pend
            if (!completeEvtMasterIds.isEmpty()) {
                Event_Staging_Record__c[] dependentEvts = [
                    SELECT Id, Event_Id__c, Event_Time__c, Event_Type__c, Dependency_Id__c, Dependency_Type__c, Status__c 
                    FROM Event_Staging_Record__c 
                    WHERE Dependency_Id__c = :completeEvtMasterIds AND Dependency_Type__c = :eventType AND Status__c = 'Pending'
                ];
                for (Event_Staging_Record__c evt : dependentEvts) {
                    evt.Status__c = 'Queued';
                }

                // Send the dependent/pending events to the queued to be picked up 
                update dependentEvts;
            }
           
        } catch(exception ex) {
            // if( ex instanceof Tfb_EventService.IgnorableEventException) {
            //     throw ex;
            // }
            // result.statusCode = 500;
            // result.status = ex.getMessage();
            // logException(ex);
            // events = events == null ? [Select id,Status__c,Error_Message__c,Priority__c from TfB_Inbound_Payload__c where Id IN:currentPayloadIds] : events;
            // for(TfB_Inbound_Payload__c payloadObj: events) {
            //     payloadObj.Status__c = 'Error';
            //     payloadObj.Error_Message__c = (payloadObj.Error_Message__c ?? '' ) + '\n' + Datetime.now() + '\n' + ex.getMessage() + '\n' + ex.getStackTraceString();
            //     payloadObj.Priority__c = 'Low';
            // }
            // update events;
        } 
        return erList;
    }

    /**
     * 
     */
    public List<EventResult> processEvents(String eventType, List<Event_Staging_Record__c> stagingRecords) {
        return null;
    }

    /**
     * @description Receive the event from the message queue and process. This method needs to be exposed as REST API endpoint. 
     */
    public Event_Staging_Record__c processEvent(String eventType, String eventId, Datetime eventTime, String eventPayload) {
        // Initialize the event stage record
        Event_Staging_Record__c payloadObj = new Event_Staging_Record__c(
            Event_Id__c = eventId,
            Event_Type__c = eventType,
            Event_Time__c = eventTime,
            Payload__c = eventPayload,
            Retry_Count__c = 0
        );

        try {
            // Check payload size and throw PayloadTooLargeException if needed
            if (eventPayload != null && eventPayload.length() > MaxPayloadSize) {
                payloadObj.Payload__c = eventPayload.left(MaxPayloadSize); // Trim
                throw new EventService.PayloadTooLargeException('The event payload exceeds the maximum size of ' + String.valueOf(MaxPayloadSize) + '.');
            }
   

            // Get event service provider
            EventService provider = getEventServiceProvider(eventType);

            // Preprocess the event 
            EventEntity event = provider.preprocessEvent(eventType, eventId, eventTime, eventPayload);

            // Store the event Master Id and Tag
            payloadObj.Master_Id__c = provider.getMasterId(event);
            payloadObj.Tag__c = provider.getTag(event);

            // Check duplicate event by the event Id
            Integer dupEvtCount = [SELECT Count() FROM Event_Staging_Record__c WHERE Event_Id__c = :eventId];
            if (dupEvtCount > 0) {
                throw new EventService.DuplicateEventException('The provided "' + eventId + '" event Id is duplicated.');
            }

            if (!isOutDatedEvent(payloadObj)) {
                throw new EventService.IgnorableEventException('This is an outdated event, as a later event of the same type and master ID has already been received.');
            }

            if (provider.processMode == EventService.ProcessModeEnum.Batch) {
                throw new EventService.QueuedEventException('The event has been queued for batch process.');
            } else if (provider.processMode == EventService.ProcessModeEnum.ThrottledRealtime && isThrottlingQueueEnabled(payloadObj.Event_Type__c)) {
                // If the throttling queue has been activated for the given event type, the current event needs to put into the throttling queue.
                throw new EventService.QueuedEventException('The event has been queued because the throttling queue has been activated.');
            }

            
            // Process the incoming event 
            Savepoint rollbackSp = Database.setSavepoint();
            try {
                provider.processEvent(event);

                payloadObj.Status__c = 'Completed';
                payloadObj.Status_Code__c = 200;

                List<Id> dependentPendingEvtIds = getDependentEventIds(payloadObj);
                if (dependentPendingEvtIds.size() > 0) {
                    System.enqueueJob(new DependencyQueueProcessor(dependentPendingEvtIds, 0));
                }
            } catch (System.DmlException ex) {
                Database.rollback(rollbackSp);
                if (StatusCode.UNABLE_TO_LOCK_ROW == ex.getDmlType(0)) {
                    // remap to the EventService exception type
                    throw handleRecordLockException(ex, provider.processMode);
                } else {
                    throw ex;
                }
            } catch (Exception ex) {
                Database.rollback(rollbackSp);

                // re-throw to handle the event staging record after rolling back the changes from event service provider
                throw ex;   
            }

        } catch (EventService.QueuedEventException ex) {
            handleQueuedEventException(ex, payloadObj);
        } catch (EventService.RetriableEventException ex) {
            // Enqueue the RetryQueueProcessor
            handleRetriableEventException(ex, payloadObj);
        } catch (EventService.PendingEventException ex) {
            handlePendingEventException(ex, payloadObj);
        } catch (EventService.IgnorableEventException ex) {
            handleIgnorableEventException(ex, payloadObj);
        } catch (EventService.PayloadTooLargeException ex) {
            handlePayloadTooLargeException(ex, payloadObj);
        } catch (EventService.InvalidEventException ex) {
            handleInvalidEventException(ex, payloadObj);
        } catch (EventService.DuplicateEventException ex) {
            handleDuplicateEventException(ex, payloadObj);
        } catch (Exception ex) {
            handleException(ex, payloadObj);
        } finally {
            insert payloadObj;
        }

        return payloadObj;
    }

    /**
     * @description Retry the event manually within Salesforce. 
     */
    public Event_Staging_Record__c retryEvent(Id retryEventId) {
        Event_Staging_Record__c retryEvent;
        try {
            // Initialize the event stage record
            retryEvent = getEventRecord(retryEventId);

            // Get event service provider
            EventService provider = getEventServiceProvider(retryEvent.Event_Type__c);

            // Preprocess the event 
            EventEntity event = provider.preprocessEvent(retryEvent.Event_Type__c, retryEvent.Event_Id__c, retryEvent.Event_Time__c, retryEvent.Payload__c);

            if (provider.processMode == EventService.ProcessModeEnum.Batch) {
                throw new EventService.QueuedEventException('The event has been queued for batch process.');
            } 

            
            // Process the incoming event 
            Savepoint rollbackSp = Database.setSavepoint();
            try {
                provider.processEvent(event);

                retryEvent.Status__c = 'Completed';
                retryEvent.Status_Code__c = 200;

                List<Id> dependentPendingEvtIds = getDependentEventIds(retryEvent);
                if (dependentPendingEvtIds.size() > 0) {
                    System.enqueueJob(new DependencyQueueProcessor(dependentPendingEvtIds, 0));
                }
            } catch (Exception ex) {
                Database.rollback(rollbackSp);

                // re-throw to handle the event staging record after rolling back the changes from event service provider
                throw ex;   
            }
        } catch (Exception ex) {
            handleException(ex, retryEvent);
        } finally {
            update retryEvent;
        }

        return retryEvent;
    }

    /**
     * @description Process the event with pending status once the dependency of the given event has been resolved. 
     */
    protected List<Id> processPendingEvent(Id pendingEventId) {
        // The dependent events for the current Pending event
        List<Id> myDependentEventIds = new List<Id>();

        Event_Staging_Record__c pendingEvent;
        try {
            // Initialize the event stage record
            pendingEvent = getEventRecord(pendingEventId);
            if (pendingEvent.Status__c != 'Pending') {
                // If the staging record status has been updated from other process, then we will skip the process.
                return myDependentEventIds;
            }

            // Get event service provider
            EventService provider = getEventServiceProvider(pendingEvent.Event_Type__c);

            // Preprocess the event 
            EventEntity event = provider.preprocessEvent(pendingEvent.Event_Type__c, pendingEvent.Event_Id__c, pendingEvent.Event_Time__c, pendingEvent.Payload__c);

            if (!isOutDatedEvent(pendingEvent)) {
                throw new EventService.IgnorableEventException('This is an out-of-order event, as a later event of the same event type and resource master Id has already been received.');
            }

            if (provider.processMode == EventService.ProcessModeEnum.ThrottledRealtime && isThrottlingQueueEnabled(pendingEvent.Event_Type__c)) {
                // If the throttling queue has been activated for the given event type, the current event needs to put into the throttling queue.
                throw new EventService.QueuedEventException('The event has been queued because the throttling queue has been activated.');
            }

            
            // Process the incoming event 
            Savepoint rollbackSp = Database.setSavepoint();
            try {
                provider.processEvent(event);

                pendingEvent.Status__c = 'Completed';
                pendingEvent.Status_Code__c = 200;

                myDependentEventIds = getDependentEventIds(pendingEvent);
            } catch (System.DmlException ex) {
                Database.rollback(rollbackSp);
                if (StatusCode.UNABLE_TO_LOCK_ROW == ex.getDmlType(0)) {
                    // remap to the EventService exception type
                    throw handleRecordLockException(ex, provider.processMode);
                } else {
                    throw ex;
                }
            } catch (Exception ex) {
                Database.rollback(rollbackSp);

                // re-throw to handle the event staging record after rolling back the changes from event service provider
                throw ex;   
            }

        } catch (EventService.QueuedEventException ex) {
            handleQueuedEventException(ex, pendingEvent);
        } catch (EventService.RetriableEventException ex) {
            handleRetriableEventException(ex, pendingEvent);
            // The pendingEvent is added back to the PendingQueue to be reprocessed because of the RetriableEventException
            myDependentEventIds = new List<Id> { pendingEvent.Id };
        } catch (EventService.PendingEventException ex) {
            handlePendingEventException(ex, pendingEvent);
        } catch (EventService.IgnorableEventException ex) {
            handleIgnorableEventException(ex, pendingEvent);
        } catch (Exception ex) {
            handleException(ex, pendingEvent);
        } finally {
            update pendingEvent;
        }

        return myDependentEventIds;
    }

    /**
     * @description
     */
    protected void processRetryEvent(Id retryEventId) {
        Event_Staging_Record__c retryEvt;
        try {
            retryEvt = getEventRecord(retryEventId);
            retryEvt.Retry_Count__c += 1;

            if (retryEvt.Status__c != 'Retry') {
                // The given retry event has been processed by other process.`
                return;
            }

            // Get event service provider
            EventService provider = getEventServiceProvider(retryEvt.Event_Type__c);

            // Preprocess the event 
            EventEntity event = provider.preprocessEvent(retryEvt.Event_Type__c, retryEvt.Event_Id__c, retryEvt.Event_Time__c, retryEvt.Payload__c);

            // Out-of-order event by the event time
            if (!isOutDatedEvent(retryEvt)) {
                throw new EventService.IgnorableEventException('This is an out-of-order event, as a later event of the same event type and resource master Id has already been received.');
            }

            if (provider.processMode == EventService.ProcessModeEnum.ThrottledRealtime && isThrottlingQueueEnabled(retryEvt.Event_Type__c)) {
                // If the throttling queue has been activated for the given event type, the current event needs to put into the throttling queue.
                throw new EventService.QueuedEventException('The event has been queued because the throttling queue has been activated.');
            }

            
            // Process the incoming event 
            Savepoint rollbackSp = Database.setSavepoint();
            try {
                provider.processEvent(event);

                retryEvt.Status__c = 'Completed';
                retryEvt.Status_Code__c = 200;

                List<Id> dependentPendingEvtIds = getDependentEventIds(retryEvt);
                if (dependentPendingEvtIds.size() > 0) {
                    System.enqueueJob(new DependencyQueueProcessor(dependentPendingEvtIds, 0));
                }

            } catch (System.DmlException ex) {
                Database.rollback(rollbackSp);
                if (StatusCode.UNABLE_TO_LOCK_ROW == ex.getDmlType(0)) {
                    // remap to the EventService exception type
                    throw handleRecordLockException(ex, provider.processMode);
                } else {
                    throw ex;
                }
            } catch (Exception ex) {
                Database.rollback(rollbackSp);

                // re-throw to handle the event staging record after rolling back the changes from event service provider
                throw ex;   
            }

        } catch (EventService.QueuedEventException ex) {
            handleQueuedEventException(ex, retryEvt);
        } catch (EventService.RetriableEventException ex) {
            handleRetriableEventException(ex, retryEvt);           
        } catch (EventService.IgnorableEventException ex) {
            handleIgnorableEventException(ex, retryEvt);
        } catch (Exception ex) {
            handleException(ex, retryEvt);
        } finally {
            update retryEvt;
        }
    }
}