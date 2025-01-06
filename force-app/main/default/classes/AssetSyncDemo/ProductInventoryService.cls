public inherited sharing class ProductInventoryService {
    public ProductInventoryService() {

    }

    /**
     * Upsert Assets from Product inventory entity
     */
    public TMFEntity.product upsertEntity(TMFEntity.Product pi) {
        ProductInventoryAccess access = new ProductInventoryAccess();
        access.upsertProduct(pi);

        return pi;
    }
}