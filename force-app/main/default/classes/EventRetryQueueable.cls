/**
 * @description Read the queued events from the staging table and process them one-by-one asynchronously.
 * The throttling queueable is executed in Singleton mode per event type to avoid race conditions.
 */
public without sharing class EventRetryQueueable implements Queueable, Finalizer {
    public static final String FutureRetryQueueableJobId = '707O900001TJO8JIAX';

    // Event Type of the notifications from the Deep.IO message queue
    private string eventType;
    // Indicate if the queuable is executing in the maintenance mode
    private Boolean maintenanceMode;
    //
    private Event_Staging_Record__c currentRetryEvent;


    /**
     * @description enqueue a throttle throttleing job for the given event type if there's no throttleing job before.
     * @param eventType
     * @return the Async ApexJob Id. If an existing throttling job exists, the existing AsyncApexJob Id is returned.
     */
    public static Id enqueueRetryQueueableJob(String eventType) {
        Id asyncJobId = getEnqueuedRetryQueueableJob(eventType);
        if (asyncJobId == null) {
            if (Limits.getQueueableJobs() < Limits.getLimitQueueableJobs()) {
                asyncJobId = enqueueRetryQueueableJob(eventType, 1);   
            } else {
                enqueueRetryQueueableJob(eventType, 1);
                asyncJobId = FutureRetryQueueableJobId;
            }
        }
        return asyncJobId;
    }
    



    /**
     * @description 
     * @return The active (queued, processing, preparing) job
     */
    private static Id getEnqueuedRetryQueueableJob(String eventType) {
        Id asyncJobId = null;
        try {
            Event_Throttling_Queue__c throttlingQueue = [SELECT Id, Async_Job_Id__c FROM Event_Throttling_Queue__c WHERE Event_Type__c = :eventType];
            AsyncApexJob asyncJob = [SELECT Id FROM AsyncApexJob WHERE Id=:throttlingQueue.Async_Job_Id__c];
            if (asyncJob.Status == 'Queued' || asyncJob.Status == 'Preparing' || asyncJob.Status == 'Processing') {
                // There is already an Queueable job scheduled for the given event type. 
                asyncJobId = (Id)asyncJob.Id;
            }
        } finally {}
        return asyncJobId;
    }



    /**
     * @description
     */
    private static Id enqueueRetryQueueableJob(String eventType, Integer delay) {
        Id apexJobId = System.enqueueJob(new EventRetryQueueable(eventType), delay);
        
        try {
            Event_Throttling_Queue__c eventBatchQueue = new Event_Throttling_Queue__c(
                Event_Type__c = eventType,
                Async_Job_Id__c = apexJobId
            );
            upsert eventBatchQueue Event_Type__c;
        } finally {}
        return apexJobId;
    }   

    @future
    public static void enqueueFutureRetryQueueableJob(String eventType, Integer delay) {
        enqueueRetryQueueableJob(eventType, delay);
        // Id apexJobId = System.enqueueJob(new EventRetryQueueable(eventType), delay);
        
        // try {
        //     Event_Throttling_Queue__c eventBatchQueue = new Event_Throttling_Queue__c(
        //         Event_Type__c = eventType,
        //         Async_Job_Id__c = apexJobId
        //     );
        //     upsert eventBatchQueue Event_Type__c;
        // } finally {}
        // return apexJobId;
    }  


    @future
    public static void enqueueFutureRetryQueueableJob(String eventType, Boolean maintenanceMode, Integer delay) {   
        System.enqueueJob(new EventRetryQueueable(eventType, maintenanceMode), delay);
    }


    /**
     * @description constructor
     */
    public EventRetryQueueable(String eventType) {
        this(eventType, false);
    }

    /**
     * @description constructor
     */
    public EventRetryQueueable(String eventType, Boolean maintenanceMode) {
        this.eventType = eventType;
        this.maintenanceMode = maintenanceMode;
    }

    public EventRetryQueueable(String eventType, Event_Staging_Record__c retryEvent) {
        this(eventType, false);
        this.currentRetryEvent = retryEvent;
    }

    /**
     * @description Method from Queueable interface
     */
    public void execute(QueueableContext context) {
        if (this.maintenanceMode) {
            // Check if there is any "retry" events but no active RetryQueueable job of the given type because the race condition
            Id asyncJobId = getEnqueuedRetryQueueableJob(eventType);
            if (asyncJobId == null) {
                Integer retryEvtCount = [SELECT Count() FROM Event_Staging_Record__c WHERE Event_Type__c = :this.eventType AND Status__c = 'Retry'];
                if (retryEvtCount > 0) {
                    // Idled Retry events without enqueued job has been found. Needs to start the RetryQueueable job again to process the left-overs
                    enqueueRetryQueueableJob(eventType);
                }
            }
        } else {
            // Enforce the EventBatchQueueable to be executed in the Singleton mode
            Event_Throttling_Queue__c throttlingQueue = [SELECT Id, Async_Job_Id__c, Event_Type__c FROM Event_Throttling_Queue__c WHERE Event_Type__c = :this.eventType];
            if (throttlingQueue.Async_Job_Id__c != context.getJobId()) {
                // This throttling AsyncApexJob needs to be skipped because there's another ApexJob enqueued with the Apex_Job_Id__c
                return;
            }

            
            
            EventSyncEngine engine = new EventSyncEngine();
            
            Event_Staging_Record__c retryEvent = engine.getNextRetryEventRecord(this.eventType);
            if (retryEvent == null) {
                // Kick-off maintence job
                if (Limits.getQueueableJobs() < Limits.getLimitQueueableJobs()) {
                    System.enqueueJob(new EventRetryQueueable(eventType, false), 1);
                } else {
                    enqueueFutureRetryQueueableJob(eventType, false, 1);
                }
            } else {
                //
                System.attachFinalizer(new EventRetryQueueable(this.eventType, retryEvent));

                engine.processEvent(retryEvent);

                // Kick-off the EventRetryQueueable for next retry event
                if (Limits.getQueueableJobs() < Limits.getLimitQueueableJobs()) {
                    System.enqueueJob(new EventRetryQueueable(eventType, true));
                } else {
                    enqueueFutureRetryQueueableJob(eventType, true, 0);
                }
            }
        }

    }   
    
    /**
     * @description Method from Finalizer Interface
     */
    public void execute(System.FinalizerContext ctx) {
        if (ctx.getResult() == System.ParentJobResult.UNHANDLED_EXCEPTION) {
            Exception unhandledEx = ctx.getException();
            try {
                // Update the errors of the failed pending event 
                this.currentRetryEvent.Error_Message__c += '\n' + EventSyncEngine.getErrorMessage(unhandledEx);
                update this.currentRetryEvent;
            } finally {
                // Kick-off the EventRetryQueueable for next retry event
                if (Limits.getQueueableJobs() < Limits.getLimitQueueableJobs()) {
                    System.enqueueJob(new EventRetryQueueable(this.eventType, true));
                } else {
                    enqueueFutureRetryQueueableJob(this.eventType, true, 0);
                }
            }
        }
    }
}