/**
 * @description Base Event entity class which has to be extended by the child event service provider
 */
public virtual with sharing  class EventEntity {
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
    private transient Map<String, CharacteristicEntity> analyticCharacteristicMap;
    // The event resource of the notification.
    // public Object event;

    public EventEntity() {
        analyticCharacteristic = new List<CharacteristicEntity>();
        analyticCharacteristicMap = new Map<String, CharacteristicEntity>();
    }
    
    public EventEntity(String eventId, String eventType, Datetime eventTime) {
        this();

        this.eventId = eventId;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public Object getCharacteristicValue(String name) {
        return analyticCharacteristicMap.get(name.toLowerCase())?.value;
    }

    public void setCharacteristicValue(String name, Object value, String valueType) {
        CharacteristicEntity ch = new CharacteristicEntity(name, value, valueType);
        analyticCharacteristicMap.put(name.toLowerCase(), ch);
    }

    public void setCharacteristicValue(String name, String value) {
        setCharacteristicValue(name, value, 'String');
    }

}