/**
 * @description Dependency Queue Processor to process events whose dependency has been resolved.  
 */
public with sharing class EventDependencyQueueable implements Queueable, Finalizer {
    public static final String FutureQueueableJobId = '707O900001TJO8JIAX';

    private List<Id> pendingEventIds;
    private Integer index;
        
    /**
     * @description enqueue a throttle throttleing job for the given event type if there's no throttleing job before.
     * @param eventType
     * @return the Async ApexJob Id. If an existing throttling job exists, the existing AsyncApexJob Id is returned.
     */
    public static Id enqueueDependencyQueueableJob(List<Id> pendingEventIds, Integer index) {
        if (Limits.getQueueableJobs() < Limits.getLimitQueueableJobs()) {
            return System.enqueueJob(new EventDependencyQueueable(pendingEventIds, index));
        } else {
            enqueueFutureDependencyQueueableJob(pendingEventIds, index);
            return FutureQueueableJobId;
        }
    }

    @future
    public static void enqueueFutureDependencyQueueableJob(List<Id> pendingEventIds, Integer index) {   
        System.enqueueJob(new EventDependencyQueueable(pendingEventIds, index));
    }



    public EventDependencyQueueable(List<String> pendingEventIds, Integer index) {
        this.pendingEventIds = pendingEventIds;
        this.index = index;
    }
    
    /**
     * @description Method from Queueable interface
     */
    public void execute(QueueableContext context) {
        System.attachFinalizer(new EventDependencyQueueable(this.pendingEventIds, this.index));

        EventSyncEngine engine = new EventSyncEngine();
        Id pendingEvtId = pendingEventIds[index];

        Event_Staging_Record__c pendingEvtRec = engine.getEventRecord(pendingEvtId);

        if (pendingEvtRec.Status__c != 'Pending') return; // Skip the record since the Pending record has been updated by other process

        engine.processEvent(pendingEvtRec);

        if (index + 1 < pendingEventIds.size()) {
            enqueueDependencyQueueableJob(this.pendingEventIds, this.index + 1);
        }
    }

    /**
     * @description Method from Finalizer Interface
     */
    public void execute(System.FinalizerContext ctx) {
        if (ctx.getResult() == System.ParentJobResult.UNHANDLED_EXCEPTION) {
            Exception unhandledEx = ctx.getException();

            try {
                EventSyncEngine engine = new EventSyncEngine();
                // Update the errors of the failed pending event 
                Id pendingEvtId = pendingEventIds[index];
                Event_Staging_Record__c pendingEvtRec = engine.getEventRecord(pendingEvtId);
                pendingEvtRec.Error_Message__c += '\n' + EventSyncEngine.getErrorMessage(unhandledEx);
                update pendingEvtRec;
            } finally {
                // Move to next pending event
                if (index + 1 < pendingEventIds.size()) {
                    enqueueDependencyQueueableJob(this.pendingEventIds, this.index + 1);
                }
            }
        }
    }    
}