// Use .apex files to store anonymous Apex.
// You can execute anonymous Apex in VS Code by selecting the
//     apex text and running the command:
//     SFDX: Execute Anonymous Apex with Currently Selected Text
// You can also execute the entire file by running the command:
//     SFDX: Execute Anonymous Apex with Editor Contents

string tempvar = 'Enter_your_name_here';
System.debug('Hello World!');
System.debug('My name is ' + tempvar);

Map<String, String> sm = new Map<String, String>();

String val = sm.put('1', 'val1');
System.debug(val);
val = sm.put('1', 'val2');
System.debug(val);


UUID randomUUID = UUID.randomUUID();
system.debug(randomUUID); // Prints the UUID string that was randomly generated
System.debug(randomUUID.toString().length());


Event_Throttling_Queue__c batchThrottlingQueue = [SELECT Id, Async_Job_Id__c FROM Event_Throttling_Queue__c WHERE Event_Type__c = ''];
Event_Throttling_Queue__c batchThrottlingQueue = new Event_Throttling_Queue__c(
    Event_Type__c = 'TestType',
    Async_Job_Id__c = '707Kc000019XffeIAC'

);
insert batchThrottlingQueue;

AsyncApexJob batchJob = [SELECT Id FROM AsyncApexJob WHERE Id=(SELECT Async_Job_Id__c FROM Event_Throttling_Queue__c)];
AsyncApexJob batchJob = [SELECT Id FROM AsyncApexJob WHERE Id='707Kc000019XffeIAC'];

StaticResource sr = [SELECT Id, Body FROM StaticResource WHERE Name = 'evtSyncEngineSample'];
String body = sr.Body.toString();
Map<String, Object> sam = (Map<String, Object>)JSON.deserializeUntyped(body);
Map<String, Object> pe = (Map<String, Object>)sam.get('addonEvent');
String planEvtJSON = JSON.serialize(((Map<String, Object>)pe.get('event')).get('product'));

// System.debug(planEvtJSON);

TMFEntity.Product p = (TMFEntity.Product)JSON.deserialize(planEvtJSON, TMFEntity.Product.class);


System.debug(JSON.serialize(p, true));