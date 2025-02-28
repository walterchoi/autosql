export interface TranslateMap {
    server_to_local: Record<string, string>;
    local_to_server: Record<string, string>;
  }
  
export interface DialectConfig {
    require_length: string[];
    optional_length: string[];
    no_length: string[];
    decimals: string[];
    translate: TranslateMap;
    default_translation: Record<string, string>;
    sqlize: Array<{
      find: string;
      replace: string;
      regex: string;
      type: boolean | string[];
    }>;
}
  