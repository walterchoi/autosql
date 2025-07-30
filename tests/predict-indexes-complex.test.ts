import { predictIndexes } from "../src/helpers/keys";
import { MetadataHeader } from "../src/config/types";
import { getMetaData } from "../src/helpers/metadata";
import { DB_CONFIG, Database } from "./utils/testConfig";

describe("predictIndexes function", () => {
    
    test("ensures composite primary key is only set when necessary", () => {
        const meta_data: MetadataHeader = {
            order_id: { type: "int", pseudounique: true, allowNull: false, length: 11 },
            product_id: { type: "int", pseudounique: true, allowNull: false, length: 11 },
            created_at: { type: "datetime", length: 10 }
        }
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
    
        const result = predictIndexes(meta_data, undefined, undefined, data);
    
        expect(result.order_id.primary).toBe(true); // ✅ Composite PK needed
        expect(result.product_id.primary).toBe(true); // ✅ Composite PK needed
        expect(result.created_at.primary).toBeUndefined(); // ✅ Should not be primary
    });    

    test("handles pseudo-unique columns with no combination that allows a unique key", () => {
        const meta_data: MetadataHeader = {
            page_id: { type: "int", pseudounique: true, allowNull: false, length: 4 },
            user_id: { type: "int", pseudounique: true, allowNull: false, length: 4 },
            page_view: { type: "int", allowNull: false, length: 11 }
        }
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
        const result = predictIndexes(meta_data, undefined, undefined, data);
        expect(result.page_id.primary).toBeUndefined(); // ✅ No primary key set
        expect(result.user_id.primary).toBeUndefined(); // ✅ No primary key set
        expect(result.page_view.primary).toBeUndefined(); // ✅ No primary key set
    });
    
    test("uses a date column to create a unique composite key", () => {
        const meta_data: MetadataHeader = {
            event_id: { type: "int", pseudounique: true, allowNull: false, length: 11 },
            user_id: { type: "int", pseudounique: true, allowNull: false, length: 11 },
            event_date: { type: "datetime", allowNull: false, length: 10 }
        }
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
    
        const result = predictIndexes(meta_data, undefined, undefined, data);
    
        expect(result.event_id.primary).toBe(true); // ✅ Composite PK formed
        expect(result.user_id.primary).toBe(true); // ✅ Composite PK formed
        expect(result.event_date.primary).toBe(true); // ✅ Date column assists but isn’t primary
    });
    
    test("handles multiple date columns and selects the correct one for uniqueness", () => {
        const meta_data: MetadataHeader = {
            transaction_id: { type: "int", pseudounique: true, allowNull: false, length: 4 },
            user_id: { type: "int", pseudounique: true, allowNull: false, length: 4 },
            purchase_date: { type: "datetime", allowNull: false, length: 10 },
            refund_date: { type: "datetime", allowNull: true, length: 10 },
            shipment_date: { type: "datetime", allowNull: false, length: 10 } // ✅ New date column
        }
    
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
    
        const result = predictIndexes(meta_data, undefined, undefined, data);
        expect(result.transaction_id.primary).toBe(true); // ✅ Composite PK
        expect(result.user_id.primary).toBe(true); // ✅ Composite PK
        expect(result.purchase_date.primary).toBeUndefined(); // ✅ Required to form a unique composite key
        expect(result.refund_date.primary).toBeUndefined(); // ✅ Should not be primary
        expect(result.shipment_date.primary).toBe(true); // ✅ Should not be primary
    });

    test("handles complex data with a multi-column", () => {
        const meta_data: MetadataHeader = {
            transaction_id: { type: "int", pseudounique: true, allowNull: false, length: 4 },
            user_id: { type: "int", pseudounique: true, allowNull: false, length: 4 },
            purchase_date: { type: "datetime", allowNull: false, length: 10 },
            refund_date: { type: "datetime", allowNull: true, length: 10 },
            shipment_date: { type: "datetime", allowNull: false, length: 10 } // ✅ New date column
        }
    
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
    
        const result = predictIndexes(meta_data, undefined, undefined, data);
        expect(result.transaction_id.primary).toBe(true); // ✅ Composite PK
        expect(result.user_id.primary).toBe(true); // ✅ Composite PK
        expect(result.purchase_date.primary).toBeUndefined(); // ✅ Required to form a unique composite key
        expect(result.refund_date.primary).toBeUndefined(); // ✅ Should not be primary
        expect(result.shipment_date.primary).toBe(true); // ✅ Should not be primary
    });

    Object.values(DB_CONFIG).forEach((config) => {
        test(`infers a multi-column composite key when the combination of fields is unique for ${config.sqlDialect.toUpperCase()}`, async () => {
            const data: Record<string, any>[] = [
                { date: "2024-07-01", channel: "Paid Social", campaign: "Summer Push A", sessions: 300, users: 240 },
                { date: "2024-07-01", channel: "Paid Social", campaign: "Promo A", sessions: 180, users: 150 },
                { date: "2024-07-01", channel: "Paid Social", campaign: "Summer Push B", sessions: 300, users: 240 },
                { date: "2024-07-01", channel: "Paid Social", campaign: "Promo B", sessions: 180, users: 150 },
                { date: "2024-07-01", channel: "Email", campaign: "Promo A", sessions: 200, users: 160 },
                { date: "2024-07-01", channel: "Paid Search", campaign: "Promo A", sessions: 320, users: 300 },
                { date: "2024-07-01", channel: "Paid Search", campaign: "Brand Awareness", sessions: 500, users: 420 },
                { date: "2024-07-01", channel: "Paid Search", campaign: "Promo B", sessions: 320, users: 300 },
                { date: "2024-07-01", channel: "Email", campaign: "Abandoned Cart", sessions: 80, users: 60 },
                { date: "2024-07-01", channel: "Organic", campaign: "(not set)", sessions: 150, users: 110 },

                { date: "2024-07-02", channel: "Paid Social", campaign: "Summer Push A", sessions: 300, users: 240 },
                { date: "2024-07-02", channel: "Paid Social", campaign: "Promo A", sessions: 180, users: 150 },
                { date: "2024-07-02", channel: "Paid Social", campaign: "Summer Push B", sessions: 300, users: 240 },
                { date: "2024-07-02", channel: "Paid Social", campaign: "Promo B", sessions: 180, users: 150 },
                { date: "2024-07-02", channel: "Email", campaign: "Promo A", sessions: 200, users: 160 },
                { date: "2024-07-02", channel: "Paid Search", campaign: "Promo A", sessions: 320, users: 300 },
                { date: "2024-07-02", channel: "Paid Search", campaign: "Brand Awareness", sessions: 500, users: 420 },
                { date: "2024-07-02", channel: "Paid Search", campaign: "Promo B", sessions: 320, users: 300 },
                { date: "2024-07-02", channel: "Email", campaign: "Abandoned Cart", sessions: 80, users: 60 },
                { date: "2024-07-02", channel: "Organic", campaign: "(not set)", sessions: 150, users: 110 },

                { date: "2024-07-03", channel: "Paid Social", campaign: "Summer Push A", sessions: 300, users: 240 },
                { date: "2024-07-03", channel: "Paid Social", campaign: "Promo A", sessions: 180, users: 150 },
                { date: "2024-07-03", channel: "Paid Social", campaign: "Summer Push B", sessions: 300, users: 240 },
                { date: "2024-07-03", channel: "Paid Social", campaign: "Promo B", sessions: 180, users: 150 },
                { date: "2024-07-03", channel: "Email", campaign: "Promo A", sessions: 200, users: 160 },
                { date: "2024-07-03", channel: "Paid Search", campaign: "Promo A", sessions: 320, users: 300 },
                { date: "2024-07-03", channel: "Paid Search", campaign: "Brand Awareness", sessions: 500, users: 420 },
                { date: "2024-07-03", channel: "Paid Search", campaign: "Promo B", sessions: 320, users: 300 },
                { date: "2024-07-03", channel: "Email", campaign: "Abandoned Cart", sessions: 80, users: 60 },
                { date: "2024-07-03", channel: "Organic", campaign: "(not set)", sessions: 150, users: 110 },

                { date: "2024-07-04", channel: "Paid Social", campaign: "Summer Push A", sessions: 300, users: 240 },
                { date: "2024-07-04", channel: "Paid Social", campaign: "Promo A", sessions: 180, users: 150 },
                { date: "2024-07-04", channel: "Paid Social", campaign: "Summer Push B", sessions: 300, users: 240 },
                { date: "2024-07-04", channel: "Paid Social", campaign: "Promo B", sessions: 180, users: 150 },
                { date: "2024-07-04", channel: "Email", campaign: "Promo A", sessions: 200, users: 160 },
                { date: "2024-07-04", channel: "Paid Search", campaign: "Promo A", sessions: 320, users: 300 },
                { date: "2024-07-04", channel: "Paid Search", campaign: "Brand Awareness", sessions: 500, users: 420 },
                { date: "2024-07-04", channel: "Paid Search", campaign: "Promo B", sessions: 320, users: 300 },
                { date: "2024-07-04", channel: "Email", campaign: "Abandoned Cart", sessions: 80, users: 60 },
                { date: "2024-07-04", channel: "Organic", campaign: "(not set)", sessions: 150, users: 110 }
            ];

            const meta_data = await getMetaData(config, data);
            const result = predictIndexes(meta_data, undefined, undefined, data);
            expect(result.date.primary).toBe(true);      // ✅ part of composite PK
            expect(result.channel.primary).toBe(true);   // ✅ part of composite PK
            expect(result.campaign.primary).toBe(true);  // ✅ part of composite PK
            expect(result.sessions.primary).toBeFalsy(); // ✅ not part of PK
            expect(result.users.primary).toBeFalsy();    // ✅ not part of PK
        });
    });
});