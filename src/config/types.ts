export type MetadataHeader = Record<string, {
    type: string | null;
    length: number;
    allowNull: boolean;
    unique: boolean;
    index: boolean;
    pseudounique: boolean;
    primary: boolean;
    autoIncrement: boolean;
    default?: any;
    decimal?: number; // Ensure decimal exists
}>;