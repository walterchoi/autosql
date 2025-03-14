import { predictIndexes } from "../src/helpers/keys";
import { MetadataHeader } from "../src/config/types";

describe("predictIndexes function", () => {
    test("predicts indexes for date-related, unique, and pseudo-unique columns", () => {
        const meta_data: MetadataHeader = {
            created_at: { type: "datetime", length: 10 },
            email: { type: "varchar", unique: true, length: 50 },
            order_id: { type: "int", pseudounique: true, length: 11 },
            description: { type: "text", length: 200 }
        }
        const maxKeyLength : number = 100

        const result = predictIndexes(meta_data, maxKeyLength);

        expect(result.created_at.index).toBe(true); // Date field → index
        expect(result.email.index).toBe(true); // Unique field → index
        expect(result.order_id.index).toBe(true); // Pseudo-unique field → index
        expect(result.description.index).toBeUndefined(); // Text field → no index
    });

    test("sets primary keys when explicitly provided", () => {
        const meta_data : MetadataHeader = {
            user_id: { type: "int", length: 11 },
            email: { type: "varchar", unique: true, length: 50 }
        }

        const result = predictIndexes(meta_data, undefined, ["user_id"]);

        expect(result.user_id.primary).toBe(true); // Explicit primary key
        expect(result.email.primary).toBeUndefined(); // No primary key set
    });

    test("predicts composite primary keys when no explicit primary key is provided", () => {
        const meta_data: MetadataHeader = {
            order_id: { type: "int", pseudounique: true, allowNull: false, length: 11 },
            customer_id: { type: "int", unique: true, allowNull: false, length: 11 },
            created_at: { type: "datetime", length: 10 }
        }
        const maxKeyLength: number = 50

        const result = predictIndexes(meta_data, maxKeyLength);

        expect(result.order_id.primary).toBeUndefined(); // Not a key column
        expect(result.customer_id.primary).toBe(true);
        expect(result.created_at.primary).toBeUndefined(); // Not a key column
    });

    test("does not set primary keys if all candidates allow nulls", () => {
        const meta_data: MetadataHeader = {
            order_id: { type: "int", pseudounique: true, allowNull: true, length: 11 },
            customer_id: { type: "int", unique: true, allowNull: true, length: 11 }
        }

        const result = predictIndexes(meta_data);

        expect(result.order_id.primary).toBeUndefined();
        expect(result.customer_id.primary).toBeUndefined();
    });

    test("respects maxKeyLength constraint", () => {
        const meta_data: MetadataHeader = {
            long_text: { type: "text", length: 5000 },
            short_text: { type: "varchar", unique: true, length: 30 }
        }
        const maxKeyLength: number = 100
        const result = predictIndexes(meta_data, maxKeyLength);

        expect(result.long_text.index).toBeUndefined(); // Too long for index
        expect(result.short_text.index).toBe(true); // Within key length
    });

    test("handles empty metadata gracefully", () => {
        const meta_data: MetadataHeader = {};

        expect(predictIndexes(meta_data)).toEqual({});
    });

    test("throws an error when no config is provided", () => {
        expect(() => predictIndexes(undefined as any)).toThrow("Error in predictIndexes");
    });

    test("does not set primary keys if primaryKey list contains non-existent columns", () => {
        const meta_data: MetadataHeader = {
            user_id: { type: "int", unique: true, allowNull: false, length: 11 },
            email: { type: "varchar", unique: true, length: 50 }
        }

        const result = predictIndexes(meta_data, undefined, ["non_existent_column", "user_id"]);

        expect(result.user_id.primary).toBe(true); // Only existing column should be set
        expect(result["non_existent_column"]).toBeUndefined(); // Non-existent column should not exist
    });

    test("does not assign primary key to long text columns", () => {
        const meta_data: MetadataHeader = {
            description: { type: "text", unique: true, allowNull: false, length: 5000 },
            title: { type: "varchar", unique: true, allowNull: false, length: 255 }
        }
        const maxKeyLength: number = 1000

        const result = predictIndexes(meta_data, maxKeyLength);

        expect(result.description.primary).toBeUndefined(); // Long text field should not be primary
        expect(result.title.primary).toBe(true); // Short varchar should be primary
    });

    test("does not assign primary key to long text columns", () => {
        const meta_data: MetadataHeader = {
            description: { type: "text", unique: true, allowNull: false, length: 5000 },
            title: { type: "varchar", unique: true, allowNull: false, length: 2255 }
        }
        const maxKeyLength: number = 1000

        const result = predictIndexes(meta_data, maxKeyLength);

        expect(result.description.primary).toBeUndefined(); // Long text field should not be primary
        expect(result.title.primary).toBeUndefined(); // Short varchar should be primary
    });

    test("does not assign indexes to long text columns", () => {
        const meta_data: MetadataHeader =  {
            content: { type: "longtext", length: 10000 },
            summary: { type: "varchar", unique: true, length: 100 }
        }
        const maxKeyLength: number = 1000

        const result = predictIndexes(meta_data, maxKeyLength);

        expect(result.content.index).toBeUndefined(); // Long text field should not be indexed
        expect(result.summary.index).toBe(true); // Unique short varchar should be indexed
    });

    test("handles missing or empty meta_data gracefully", () => {
        const meta_data: MetadataHeader = {};
        expect(predictIndexes(meta_data)).toEqual({});

        expect(() => predictIndexes(undefined as any)).toThrow("Error in predictIndexes");
    });
});
