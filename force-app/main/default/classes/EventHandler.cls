// Salesforce REST Endpoint: /services/apexrest/tmf-api/eventHandler
@RestResource(urlMapping='/tmf-api/eventHandler')
global without sharing class EventHandler {
    global EventHandler() {

    }

    @HttpPost
    global static void processEvent() {
        RestResponse resp = RestContext.response;

        EventSyncEngine evtEngine = new EventSyncEngine();
        String eventPayload = RestContext.Request.requestBody.toString();

        try {
            Event_Staging_Record__c stagingRec = evtEngine.processEvent(eventPayload);
            resp.statusCode = (Integer)stagingRec.Status_Code__c;
            if (resp.statusCode == 200 || resp.statusCode == 201 || resp.statusCode == 202) {
	            resp.responseBody = Blob.valueOf(stagingRec.Id);
            } else {
                resp.responseBody = Blob.valueOf(stagingRec.Error_Message__c);
            }
        } catch (Exception ex) {
            System.debug(ex.getMessage());
            resp.statusCode = 500;
            resp.responseBody = Blob.valueOf(EventSyncEngine.getErrorMessage(ex));
        }

    }
}