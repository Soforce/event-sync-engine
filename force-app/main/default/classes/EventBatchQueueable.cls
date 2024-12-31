/**
 * @description Read the queued events from the staging table and process them in batches asynchronously.
 * The batch is executed in Singleton mode per event type to avoid race conditions.
 */
public without sharing class EventBatchQueueable implements Finalizer, Queueable {
   
    // private string transactionId;
    private string eventType;
    // public List<Id> currentPayloadIds = new List<Id>();
 
    public EventBatchQueueable() {

    }
    public EventBatchQueueable(String eventType) {
        this.eventType = eventType;
    }

    // public EventBatchQueueable(String transactionId, String eventType) {
    //     // EventSyncEngine.InvokedviaSyncEngine = true; //Flag to skip trigger logic
    //     this.transactionId = transactionId;
    //     this.eventType = eventType;
    // }

    // public EventBatchQueueable(String transactionId, String eventType, List<Id> currentPayloadIds) {
    //     // EventSyncEngine.InvokedviaSyncEngine = true; //Flag to skip trigger logic
    //     this.transactionId = transactionId;
    //     this.eventType = eventType;
    //     this.currentPayloadIds = currentPayloadIds;
    // }




    public void execute(QueueableContext context) {
        // Enforce the EventBatchQueueable to be executed in the Singleton mode
        Event_Throttling_Queue__c throttlingQueue = [SELECT Id, Async_Job_Id__c, Event_Type__c FROM Event_Throttling_Queue__c WHERE Event_Type__c = :this.eventType];
        if (throttlingQueue.Async_Job_Id__c != context.getJobId()) {
            // This throttling AsyncApexJob needs to be skipped because there's another ApexJob enqueued with the Apex_Job_Id__c
            return;
        }

        EventSyncEngine engine = new EventSyncEngine();

        List<Event_Staging_Record__c> queuedRecords = null; // engine.getQueuedEventRecords(eventType);

        if (queuedRecords.isEmpty()) {
            // Needs to schedule a final job???
        } else {
            // List<EventSyncEngine.EventResult> erList = engine.processEvents(this.eventType, queuedRecords);

            
        }




        //Attaching Finalizer
        System.attachFinalizer(new EventBatchQueueable(this.eventType));
        
        // public List<EventResult> processEvents(String eventType, String transactionId /*, List<Id> batchStagingRecIds */) {
        // List<EventResult> erList = engine.processEvents(this.eventType, this.transactionId);
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
        //     System.enqueueJob(new EventBatchQueueable(this.transactionId, this.eventType));
        // }
    }    
}