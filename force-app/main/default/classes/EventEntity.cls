/**
 * @description Base Event entity class which has to be extended by the child event service provider
 */
public virtual with sharing class EventEntity {
    // The correlation id for this event.
    public String correlationId;
    // The identifier of the notification.
    public String eventId;
    // Time of the event occurrence.
    public Datetime eventTime;
    // The type of the notification.
    public String eventType;   
    // A list of characteristics (Characteristic [*]).
    public CharacteristicEntity[] analyticCharacteristic;

    // The event resource of the notification.
    public Object event;
    


    public EventEntity() {
        analyticCharacteristic = new List<CharacteristicEntity>();
    }

    public EventEntity(String eventId, String eventType, Datetime eventTime) {
        this();

        this.eventId = eventId;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }
}