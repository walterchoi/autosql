import { MetadataHeader, QueryInput, AlterTableChanges, DatabaseConfig, InsertInput } from "../../../config/types";
import { pgsqlConfig } from "../../config/pgsqlConfig";
import { compareMetaData } from '../../../helpers/metadata';
const dialectConfig = pgsqlConfig

export class PostgresInsertQueryBuilder {
    static getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader): QueryInput {
        return ''
    }   
}