public inherited sharing class ProductInventoryEventService extends EventService {
    public class ProductEventResource {
        public TMFEntity.Product product;
    }
    public class ProductAttributeValueChangeEvent extends EventEntity {
        public ProductEventResource event;
    }

    public ProductInventoryEventService() {

    }

    protected override EventEntity deserializeEventPayload(String eventType, String eventPayload) {
        if (eventType == 'ProductAttributeValueChangeEvent') {
            return (ProductAttributeValueChangeEvent)JSON.deserialize(eventPayload, ProductAttributeValueChangeEvent.class);
        }
        return null;

    }

    protected override void processEvent(String eventType, String eventId, Datetime eventTime, EventEntity event) {
        if (eventType == 'ProductAttributeValueChangeEvent') {
            ProductAttributeValueChangeEvent myEvent = (ProductAttributeValueChangeEvent)event;
            ProductInventoryAccess access = new ProductInventoryAccess();
            access.upsertProduct(myEvent.event.product);

        }
    }

    protected override String getEventMasterId(EventEntity event) {
        if (event.eventType == 'ProductAttributeValueChangeEvent') {
            ProductAttributeValueChangeEvent myEvent = (ProductAttributeValueChangeEvent)event;
            return myEvent.event.product.id;
        }
        return null;

    }
    protected override String getEventTag(EventEntity event) {
        if (event.eventType == 'ProductAttributeValueChangeEvent') {
            ProductAttributeValueChangeEvent myEvent = (ProductAttributeValueChangeEvent)event;
            for (TMFEntity.RelatedParty rp : myEvent.event.product.relatedParty) {
                if (rp.role == 'Customer') {
                    return rp.id;
                }
            }
        }
        return null;
    }
}