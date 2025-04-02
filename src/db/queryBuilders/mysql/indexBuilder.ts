import { QueryInput } from "../../../config/types";
import { getTempTableName } from "../../../helpers/utilities";

export class MySQLIndexQueryBuilder {
    static getPrimaryKeysQuery(table: string, schema?: string): QueryInput {
        return {
            query: `
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE TABLE_NAME = ? 
                ${schema ? "AND TABLE_SCHEMA = ?" : ""}
                AND CONSTRAINT_NAME = 'PRIMARY';
            `,
            params: schema ? [table, schema] : [table]
        };
    }
    
    static getForeignKeyConstraintsQuery(table: string, schema?: string): QueryInput {
        return {
            query: `
                SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE REFERENCED_TABLE_NAME = ? 
                ${schema ? "AND TABLE_SCHEMA = ?" : ""};
            `,
            params: schema ? [table, schema] : [table]
        };
    }
    
    static getViewDependenciesQuery(table: string, schema?: string): QueryInput {
        return {
            query: `
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.VIEWS 
                WHERE VIEW_DEFINITION LIKE CONCAT('%', ?, '%') 
                ${schema ? "AND TABLE_SCHEMA = ?" : ""};
            `,
            params: schema ? [table, schema] : [table]
        };
    }    

    static getDropPrimaryKeyQuery(table: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `\`${schema}\`.` : "";
        return {
            query: `ALTER TABLE ${schemaPrefix}\`${table}\` DROP PRIMARY KEY;`,
            params: []
        };
    }

    static getDropUniqueConstraintQuery(table: string, indexName: string, schema?: string): QueryInput {
        const schemaPrefix = schema ? `\`${schema}\`.` : "";
        return {
          query: `DROP INDEX \`${indexName}\` ON ${schemaPrefix}\`${table}\`;`,
          params: []
        };
    }

    static getAddPrimaryKeyQuery(table: string, primaryKeys: string[], schema?: string): QueryInput {
        const schemaPrefix = schema ? `\`${schema}\`.` : "";
        return {
            query: `ALTER TABLE ${schemaPrefix}\`${table}\` ADD PRIMARY KEY (${primaryKeys.map(pk => `\`${pk}\``).join(", ")});`,
            params: []
        };
    }

    static getUniqueIndexesQuery(table: string, columnName?: string, schema?: string): QueryInput {
        let query = `
            SELECT DISTINCT index_name, GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns
            FROM information_schema.statistics
            WHERE table_name = ?
            AND non_unique = 0
            AND index_name != 'PRIMARY'
        `;
        
        const params = [table];
    
        if (columnName) {
            query += " AND column_name = ?";
            params.push(columnName);
        }

        if (schema) {
            query += " AND table_schema = ?";
            params.push(schema);
        }
    
        query += " GROUP BY index_name;";
    
        return { query, params };
    }

    static generateConstraintConflictBreakdownQuery(table: string, structure: { uniques: Record<string, string[]>; primary: string[] } , schema?: string): QueryInput {
        const schemaPrefix = schema ? `\`${schema}\`.` : "";
        const tempTable = getTempTableName(table);
        const t1 = "t1";
        const t2 = "t2";
      
        const conflictColumns = Object.entries(structure.uniques).map(([index_name, cols]) => {
            const condition = cols.map(col => `t1.${col} = t2.${col}`).join(" AND ");
            const alias = index_name;
      
          return `  SUM(CASE WHEN ${condition} THEN 1 ELSE 0 END) AS \`${alias}\``;
        });
      
        const primaryMismatch = structure.primary.length
          ? structure.primary.map(col => `${t1}.${col} != ${t2}.${col}`).join(" OR ")
          : "FALSE";
      
        const query = `SELECT
                        ${conflictColumns.join(",\n")}
                        FROM ${schemaPrefix}\`${table}\` ${t1}
                        JOIN ${schemaPrefix}\`${tempTable}\` ${t2}
                        ON (${primaryMismatch});
                        `.trim();
      
        return {
          query,
          params: []
        };
    }
}