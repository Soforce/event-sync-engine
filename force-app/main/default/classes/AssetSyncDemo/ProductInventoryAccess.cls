/**
 * Data Access class for Salesforce Asset related objects
 */
public inherited sharing class ProductInventoryAccess {
    public class MissingPrimaryAssetException extends Exception {
        public String primaryAssetId;
        public MissingPrimaryAssetException(String primaryAssetId, String errorMessage) {
            super.setMessage(errorMessage);
            this.primaryAssetId = primaryAssetId;
        }
    }

    public ProductInventoryAccess() {

    }


    /**
     * 
     */
    public Asset upsertProduct(TMFEntity.Product tmfProduct) {
        Map<String, Product2> exIdProduct2Map = new Map<String, Product2>();
        Map<String, Asset> exIdAssetMap = new Map<String, Asset>();
        Map<String, Account> exIdBusinessAccountMap = new Map<String, Account>();
        Map<String, Account> exIdBillingAccountMap = new Map<String, Account>();
        Map<String, Object> ctx = new Map<String, Object> {
            'exIdProduct2Map' => exIdProduct2Map,
            'exIdBusinessAccountMap' => exIdBusinessAccountMap,
            'exIdBillingAccountMap' => exIdBillingAccountMap,
            'exIdAssetMap' => exIdAssetMap
        };

        // Prepare the related external Ids
        prepare(tmfProduct, ctx);

        // Query the related objects
        Account[] bizAccounts = [SELECT Id, External_ID__c, Name FROM Account WHERE RecordType.DeveloperName = 'Business' AND External_ID__c = :exIdBusinessAccountMap.keySet()];
        Account[] billAccounts = [SELECT Id, External_ID__c, Name FROM Account WHERE RecordType.DeveloperName = 'Billing' AND External_ID__c = :exIdBillingAccountMap.keySet()];
        Product2[] product2s = [SELECT Id, Name, ProductCode FROM Product2 WHERE ProductCode =:exIdProduct2Map.keySet()];
        Asset[] relatedAssets = [SELECT Id, Name, External_ID__c FROM Asset WHERE External_ID__c =:exIdAssetMap.keySet()];

        for (Account ba : bizAccounts) {
            exIdBusinessAccountMap.put(ba.External_ID__c, ba);
        }
        for (Account ba : billAccounts) {
            exIdBillingAccountMap.put(ba.External_ID__c, ba);
        }
        for (Product2 pd : product2s) {
            exIdProduct2Map.put(pd.ProductCode, pd);
        }
        for (Asset ast : relatedAssets) {
            exIdAssetMap.put(ast.External_ID__c, ast);
        }



        // Prepare assets to upsert
        Map<String, Asset> myAssets = new Map<String, Asset>();
        Asset rootAsset = initiaizeAsset(tmfProduct, ctx, myAssets);
        
        // Upsert asset objects
        upsert myAssets.values() External_ID__c;
        exIdAssetMap.putAll(myAssets);


        // Process asset hierarchy 
        Asset[] childAssets = buildAssetHierarchy(tmfProduct, ctx, myAssets);
        if (childAssets.size() > 0) update childAssets; // Update hierarchy

        // Process asset relationship
        AssetRelationship[] existingRelationships = [SELECT Id, AssetId, RelatedAssetId, RelationshipType FROM AssetRelationship WHERE RelatedAssetId=:rootAsset.Id];
        Map<String, AssetRelationship> existingRelationshipMap = new Map<String, AssetRelationship>();
        AssetRelationship[] newRelationships = new List<AssetRelationship>();

        for (AssetRelationship rs : existingRelationships) {
            existingRelationshipMap.put(rs.AssetId + '-' + rs.RelationshipType + '-' + rs.RelatedAssetId, rs);
        }
        if (tmfProduct.productRelationship != null) {

            for (TMFEntity.ProductRelationship pr : tmfProduct.productRelationship) {
                Asset primaryAsset = exIdAssetMap.get(pr.product.id);
                if (primaryAsset == null) {
                    throw new MissingPrimaryAssetException(pr.product.id, 'The primary asset with external id "' + pr.product.id + '" is missing.');
                } 
                String rsKey = primaryAsset.Id + '-' + pr.relationshipType + '-' + rootAsset.Id;

                if (existingRelationshipMap.remove(rsKey) == null) {
                    // Needs to insert new relationship
                    AssetRelationship rs = new AssetRelationship(
                        AssetId = primaryAsset.Id,
                        RelatedAssetId = rootAsset.Id,
                        RelationshipType = pr.relationshipType
                    );

                    newRelationships.add(rs);
                }
            }
        }

        if (newRelationships.size() > 0) insert newRelationships;
        if (existingRelationshipMap.size() > 0) delete existingRelationshipMap.values();

        return rootAsset;
    }

    private void prepare(TMFEntity.Product tmfProduct, Map<String, Object> ctx) {
        Map<String, Asset> exIdAssetMap = (Map<String, Asset>)ctx.get('exIdAssetMap'); 
        Map<String, Product2> exIdProduct2Map = (Map<String, Product2>)ctx.get('exIdProduct2Map'); 
        Map<String, Account> exIdBusinessAccountMap =  (Map<String, Account>)ctx.get('exIdBusinessAccountMap'); 
        Map<String, Account> exIdBillingAccountMap = (Map<String, Account>) ctx.get('exIdBillingAccountMap'); 

        exIdProduct2Map.put(tmfProduct.productOffering.id, null);
        exIdBillingAccountMap.put(tmfProduct.billingAccount.id, null);
        for (TMFEntity.RelatedParty rp : tmfProduct.relatedParty) {
            if (rp.role == 'Customer') {
                exIdBusinessAccountMap.put(rp.id, null);
            }
        }
        
        if (tmfProduct.productRelationship != null) {
            for (TMFEntity.ProductRelationship pr : tmfProduct.productRelationship) {
                exIdAssetMap.put(pr.product.id, null);
            }
        }

        if (tmfProduct.product != null) {
            for (TMFEntity.Product childProduct : tmfProduct.product) {
                prepare(childProduct, ctx);
            }
        }
    }

    /**
     * @return root asset
     */
    private Asset initiaizeAsset(TMFEntity.Product tmfProduct, Map<String, Object> ctx, Map<String, Asset> myAssets) {
        Map<String, Asset> exIdAssetMap = (Map<String, Asset>)ctx.get('exIdAssetMap'); 
        Map<String, Product2> exIdProduct2Map = (Map<String, Product2>)ctx.get('exIdProduct2Map'); 
        Map<String, Account> exIdBusinessAccountMap =  (Map<String, Account>)ctx.get('exIdBusinessAccountMap'); 
        Map<String, Account> exIdBillingAccountMap = (Map<String, Account>) ctx.get('exIdBillingAccountMap'); 

        Asset myAsset = toAsset(tmfProduct);
        myAssets.put(tmfProduct.id, myAsset);
        for (TMFEntity.RelatedParty rp : tmfProduct.relatedParty) {
            if (rp.role == 'Customer') {
                myAsset.AccountId = exIdBusinessAccountMap.get(rp.id)?.Id;
                break;
            }
        }
        myAsset.vlocity_cmt__BillingAccountId__c = exIdBillingAccountMap.get(tmfProduct.billingAccount.id)?.Id;
        myAsset.Product2Id = exIdProduct2Map.get(tmfProduct.productOffering.id)?.Id;

        if (tmfProduct.product != null) {
            for (TMFEntity.Product childProduct : tmfProduct.product) {
                initiaizeAsset(childProduct, ctx, myAssets);
            }
        }

        return myAsset;
    }

    private Asset[] buildAssetHierarchy(TMFEntity.Product tmfProduct, Map<String, Object> ctx, Map<String, Asset> myAssets) {
        Asset[] childAssets = new List<Asset>();

        if (tmfProduct.product == null || tmfProduct.product.size() == 0) return childAssets;

        Asset parentAsset = myAssets.get(tmfProduct.id);
        if (parentAsset == null) {
            // throw exception
        }

        for (TMFEntity.Product cp : tmfProduct.product) {
            Asset childAsset = myAssets.get(cp.id);
            childAssets.add(childAsset);

            childAsset.ParentId = parentAsset.Id;

            childAssets.addAll(buildAssetHierarchy(cp, ctx, myAssets));
        }
    
        return childAssets;
    }
    /**
     * 
     */
    public Asset toAsset(TMFEntity.Product tmfProduct) {
        Asset myAsset = new Asset(
			Name = tmfProduct.name,
			External_ID__c = tmfProduct.id,
			vlocity_cmt__DisconnectDate__c = tmfProduct.terminationDate,
			vlocity_cmt__ActivationDate__c = Date.today(),
			vlocity_cmt__ProvisioningStatus__c = tmfProduct.status,
			Quantity = tmfProduct.quantity
        );

        // Pricings
        if (tmfProduct.productPrice != null) {
            for (TMFEntity.ProductPrice pp : tmfProduct.productPrice) {
                if (pp.priceType == 'RC') {
                    myAsset.vlocity_cmt__RecurringCharge__c = pp.price.taxIncludedAmount.value;
                } else if (pp.priceType == 'OC') {
                    myAsset.vlocity_cmt__OneTimeCharge__c = pp.price.taxIncludedAmount.value;
                }
            }
            myAsset.vlocity_cmt__PricingLogData__c = JSON.serialize(tmfProduct.productPrice);
        }

        // Attributes
		Map<String, Object> selectedVals = new Map<String, Object>();
        for (TMFEntity.Characteristic ch : tmfProduct.productCharacteristic) {
            selectedVals.put(ch.name, ch.value);
        }
        myAsset.vlocity_cmt__AttributeSelectedValues__c = JSON.serialize(selectedVals, true);


		return myAsset;

    } 
    
}