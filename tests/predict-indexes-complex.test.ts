import { predictIndexes } from "../src/helpers/keys";
import { MetadataHeader } from "../src/config/types";

describe("predictIndexes function", () => {
    
    test("ensures composite primary key is only set when necessary", () => {
        const config = {
            meta_data: {
                order_id: { type: "int", pseudounique: true, allowNull: false, length: 11 },
                product_id: { type: "int", pseudounique: true, allowNull: false, length: 11 },
                created_at: { type: "datetime", length: 10 }
            } as MetadataHeader
        };
        const data : Record<string, any>[] = [
            { order_id: 1, product_id: 101, created_at: "2024-03-01 12:00:00" },
            { order_id: 2, product_id: 102, created_at: "2024-03-02 12:00:00" },
            { order_id: 2, product_id: 104, created_at: "2024-03-09 12:00:00" }, // Duplicate product_id
            { order_id: 3, product_id: 103, created_at: "2024-03-03 12:00:00" },
            { order_id: 3, product_id: 105, created_at: "2024-03-10 12:00:00" }, // Duplicate order_id
            { order_id: 4, product_id: 104, created_at: "2024-03-04 12:00:00" },
            { order_id: 5, product_id: 105, created_at: "2024-03-05 12:00:00" },
            { order_id: 6, product_id: 101, created_at: "2024-03-06 12:00:00" }, // Duplicate product_id
            { order_id: 6, product_id: 102, created_at: "2024-03-07 12:00:00" }, // Duplicate product_id
            { order_id: 6, product_id: 103, created_at: "2024-03-08 12:00:00" }, // Duplicate product_id                
        ]
    
        const result = predictIndexes(config, undefined, data);
    
        expect(result.order_id.primary).toBe(true); // ✅ Composite PK needed
        expect(result.product_id.primary).toBe(true); // ✅ Composite PK needed
        expect(result.created_at.primary).toBeUndefined(); // ✅ Should not be primary
    });    

    test("handles pseudo-unique columns with no combination that allows a unique key", () => {
        const config = {
            meta_data: {
                page_id: { type: "int", pseudounique: true, allowNull: false, length: 4 },
                user_id: { type: "int", pseudounique: true, allowNull: false, length: 4 },
                page_view: { type: "int", allowNull: false, length: 11 }
            } as MetadataHeader
        };
        const data : Record<string, any>[] = [
            { page_id: 1001, user_id: 1, page_view: 5 },
            { page_id: 1002, user_id: 1, page_view: 5 },
            { page_id: 1003, user_id: 2, page_view: 8 },
            { page_id: 1001, user_id: 3, page_view: 8 },
            { page_id: 1001, user_id: 3, page_view: 4 },
            { page_id: 1006, user_id: 4, page_view: 8 },
            { page_id: 1007, user_id: 5, page_view: 6 },
            { page_id: 1008, user_id: 5, page_view: 7 },
            { page_id: 1009, user_id: 6, page_view: 18 },
            { page_id: 1010, user_id: 6, page_view: 9 }
        ]
        const result = predictIndexes(config, undefined, data);
        expect(result.page_id.primary).toBeUndefined(); // ✅ No primary key set
        expect(result.user_id.primary).toBeUndefined(); // ✅ No primary key set
        expect(result.page_view.primary).toBeUndefined(); // ✅ No primary key set
    });
    
    test("uses a date column to create a unique composite key", () => {
        const config = {
            meta_data: {
                event_id: { type: "int", pseudounique: true, allowNull: false, length: 11 },
                user_id: { type: "int", pseudounique: true, allowNull: false, length: 11 },
                event_date: { type: "datetime", allowNull: false, length: 10 }
            } as MetadataHeader            
        };
        const data: Record<string, any>[] = [
            { event_id: 2001, user_id: 1, event_date: "2024-03-01" },
            { event_id: 2001, user_id: 1, event_date: "2024-03-02" }, // Duplicate event_id
            { event_id: 2002, user_id: 1, event_date: "2024-03-02" },
            { event_id: 2002, user_id: 3, event_date: "2024-03-03" }, // Duplicate event_id
            { event_id: 2003, user_id: 4, event_date: "2024-03-03" },
            { event_id: 2003, user_id: 5, event_date: "2024-03-04" }, // Duplicate event_id
            { event_id: 2004, user_id: 6, event_date: "2024-03-04" },
            { event_id: 2005, user_id: 6, event_date: "2024-03-05" }, // User_id also duplicated here
            { event_id: 2005, user_id: 7, event_date: "2024-03-05" },
            { event_id: 2006, user_id: 8, event_date: "2024-03-07" }
        ]
    
        const result = predictIndexes(config, undefined, data);
    
        expect(result.event_id.primary).toBe(true); // ✅ Composite PK formed
        expect(result.user_id.primary).toBe(true); // ✅ Composite PK formed
        expect(result.event_date.primary).toBe(true); // ✅ Date column assists but isn’t primary
    });
    
    
    test("handles multiple date columns and selects the correct one for uniqueness", () => {
        const config = {
            meta_data: {
                transaction_id: { type: "int", pseudounique: true, allowNull: false, length: 4 },
                user_id: { type: "int", pseudounique: true, allowNull: false, length: 4 },
                purchase_date: { type: "datetime", allowNull: false, length: 10 },
                refund_date: { type: "datetime", allowNull: true, length: 10 },
                shipment_date: { type: "datetime", allowNull: false, length: 10 } // ✅ New date column
            } as MetadataHeader,
        };
    
        const data: Record<string, any>[] = [
            { transaction_id: 3001, user_id: 1, purchase_date: "2024-03-01", refund_date: null, shipment_date: "2024-03-02" },
            { transaction_id: 3001, user_id: 1, purchase_date: "2024-03-01", refund_date: "2024-03-05", shipment_date: "2024-03-08" },
            { transaction_id: 3002, user_id: 1, purchase_date: "2024-03-02", refund_date: "2024-03-06", shipment_date: "2024-03-08" },
            { transaction_id: 3002, user_id: 3, purchase_date: "2024-03-02", refund_date: "2024-03-07", shipment_date: "2024-03-04" },
            { transaction_id: 3003, user_id: 4, purchase_date: "2024-03-03", refund_date: null, shipment_date: "2024-03-04" },
            { transaction_id: 3003, user_id: 5, purchase_date: "2024-03-03", refund_date: null, shipment_date: "2024-03-04" },
            { transaction_id: 3004, user_id: 6, purchase_date: "2024-03-04", refund_date: null, shipment_date: "2024-03-05" },
            { transaction_id: 3005, user_id: 6, purchase_date: "2024-03-05", refund_date: null, shipment_date: "2024-03-06" },
            { transaction_id: 3005, user_id: 7, purchase_date: "2024-03-05", refund_date: null, shipment_date: "2024-03-06" },
            { transaction_id: 3006, user_id: 8, purchase_date: "2024-03-06", refund_date: null, shipment_date: "2024-03-07" }
        ];
    
        const result = predictIndexes(config, undefined, data);
        expect(result.transaction_id.primary).toBe(true); // ✅ Composite PK
        expect(result.user_id.primary).toBe(true); // ✅ Composite PK
        expect(result.purchase_date.primary).toBeUndefined(); // ✅ Required to form a unique composite key
        expect(result.refund_date.primary).toBeUndefined(); // ✅ Should not be primary
        expect(result.shipment_date.primary).toBe(true); // ✅ Should not be primary
    });    
    
});
