import { predictIndexes } from "../src/helpers/keys";

describe("predictIndexes function", () => {
    test("predicts indexes for date-related, unique, and pseudo-unique columns", () => {
        const config = {
            meta_data: [
                { created_at: { type: "datetime", length: 10 } },
                { email: { type: "varchar", unique: true, length: 50 } },
                { order_id: { type: "int", pseudounique: true, length: 11 } },
                { description: { type: "text", length: 200 } }
            ],
            max_key_length: 100
        };

        const result = predictIndexes(config);

        expect(result[0].created_at.index).toBe(true); // Date field → index
        expect(result[1].email.index).toBe(true); // Unique field → index
        expect(result[2].order_id.index).toBe(true); // Pseudo-unique field → index
        expect(result[3].description.index).toBeUndefined(); // Text field → no index
    });

    test("sets primary keys when explicitly provided", () => {
        const config = {
            meta_data: [
                { user_id: { type: "int", length: 11 } },
                { email: { type: "varchar", unique: true, length: 50 } }
            ]
        };

        const result = predictIndexes(config, ["user_id"]);

        expect(result[0].user_id.primary).toBe(true); // Explicit primary key
        expect(result[1].email.primary).toBeUndefined(); // No primary key set
    });

    test("predicts composite primary keys when no explicit primary key is provided", () => {
        const config = {
            meta_data: [
                { order_id: { type: "int", pseudounique: true, allowNull: false, length: 11 } },
                { customer_id: { type: "int", unique: true, allowNull: false, length: 11 } },
                { created_at: { type: "datetime", length: 10 } }
            ],
            max_key_length: 50
        };

        const result = predictIndexes(config);

        expect(result[0].order_id.primary).toBeUndefined(); // Not a key column
        expect(result[1].customer_id.primary).toBe(true);
        expect(result[2].created_at.primary).toBeUndefined(); // Not a key column
    });

    test("does not set primary keys if all candidates allow nulls", () => {
        const config = {
            meta_data: [
                { order_id: { type: "int", pseudounique: true, allowNull: true, length: 11 } },
                { customer_id: { type: "int", unique: true, allowNull: true, length: 11 } }
            ]
        };

        const result = predictIndexes(config);

        expect(result[0].order_id.primary).toBeUndefined();
        expect(result[1].customer_id.primary).toBeUndefined();
    });

    test("respects max_key_length constraint", () => {
        const config = {
            meta_data: [
                { long_text: { type: "text", length: 5000 } },
                { short_text: { type: "varchar", unique: true, length: 30 } }
            ],
            max_key_length: 100
        };

        const result = predictIndexes(config);

        expect(result[0].long_text.index).toBeUndefined(); // Too long for index
        expect(result[1].short_text.index).toBe(true); // Within key length
    });

    test("handles empty metadata gracefully", () => {
        const config = { meta_data: [] };

        expect(predictIndexes(config)).toEqual([]);
    });

    test("throws an error when no config is provided", () => {
        expect(() => predictIndexes(undefined as any)).toThrow("Error in predictIndexes");
    });

    test("does not set primary keys if primaryKey list contains non-existent columns", () => {
        const config = {
            meta_data: [
                { user_id: { type: "int", unique: true, allowNull: false, length: 11 } },
                { email: { type: "varchar", unique: true, length: 50 } }
            ]
        };
    
        const result = predictIndexes(config, ["non_existent_column", "user_id"]);
    
        expect(result[0].user_id.primary).toBe(true); // Only existing column should be set
        expect(result.some(col => col.non_existent_column)).toBe(false); // Non-existent column should not exist
    });
    
    test("does not assign primary key to long text columns", () => {
        const config = {
            meta_data: [
                { description: { type: "text", unique: true, allowNull: false, length: 5000 } },
                { title: { type: "varchar", unique: true, allowNull: false, length: 255 } }
            ],
            max_key_length: 1000
        };
    
        const result = predictIndexes(config);
    
        expect(result[0].description.primary).toBeUndefined(); // Long text field should not be primary
        expect(result[1].title.primary).toBe(true); // Short varchar should be primary
    });
    
    test("does not assign indexes to long text columns", () => {
        const config = {
            meta_data: [
                { content: { type: "longtext", length: 10000 } },
                { summary: { type: "varchar", unique: true, length: 100 } }
            ],
            max_key_length: 1000
        };
    
        const result = predictIndexes(config);
    
        expect(result[0].content.index).toBeUndefined(); // Long text field should not be indexed
        expect(result[1].summary.index).toBe(true); // Unique short varchar should be indexed
    });
    
    test("ensures composite primary key is only set when necessary", () => {
        const config = {
            meta_data: [
                { order_id: { type: "int", pseudounique: true, allowNull: false, length: 11 } },
                { product_id: { type: "int", pseudounique: true, allowNull: false, length: 11 } },
                { created_at: { type: "datetime", length: 10 } }
            ]
        };
    
        const result = predictIndexes(config);
    
        expect(result[0].order_id.primary).toBe(true);
        expect(result[1].product_id.primary).toBe(true);
        expect(result[2].created_at.primary).toBeUndefined(); // Should not be primary
    });
    
    test("handles missing or empty meta_data gracefully", () => {
        const emptyConfig = { meta_data: [] };
        expect(predictIndexes(emptyConfig)).toEqual([]);
    
        expect(() => predictIndexes(undefined as any)).toThrow("Error in predictIndexes");
    });    
});
