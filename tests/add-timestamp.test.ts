import { DB_CONFIG, Database } from "./utils/testConfig";
import { ColumnDefinition } from "../src/config/types";

let db: Database;

beforeAll(() => {
    db = Database.create(DB_CONFIG.mysql);
});

describe("Sample Test", () => {
    test("should add numbers correctly", () => {
        expect(2 + 3).toBe(5);
    });
});