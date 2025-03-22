import { MetadataHeader, QueryInput, AlterTableChanges, DatabaseConfig, InsertInput } from "../../../config/types";
import { mysqlConfig } from "../../config/mysqlConfig";
import { compareMetaData } from '../../../helpers/metadata';
const dialectConfig = mysqlConfig

export class MySQLInsertQueryBuilder {
    static getInsertStatementQuery(tableOrInput: string | InsertInput, data?: Record<string, any>[], metaData?: MetadataHeader): QueryInput {
        return ''
    }   
}