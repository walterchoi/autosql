// Matches whole numbers with optional thousands separators (1,000 or 1.000)
const number = new RegExp(/^-?[0-9]{1,3}([.,]?[0-9]{3})*$/);

// Matches decimal numbers (both US and European formats)
const decimal = new RegExp(/^-?[0-9]{1,3}([.,]?[0-9]{3})*([.,][0-9]+)$/);

// Matches scientific notation (1.23e10, -3.5E-5)
const exponential = new RegExp(/^(-?[0-9]+(\.[0-9]+)?[eE][-+]?[0-9]+)$/);

// Matches datetime with timezone (ISO 8601, Unix timestamps, and JS Date formats)
const datetimetz = new RegExp(
    /^\/Date\(\-?\d{12,14}\+\d{4}\)\/$|^["]?(?:\d{4})[-\/\.]?(0[1-9]|1[0-2])[-\/\.]?(0[1-9]|[12][0-9]|3[01])[T\s]([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(?:\.\d+)?(?:Z|[+-](?:[01][0-9]|2[0-3]):?[0-5][0-9]| [A-Z]{3})["]?$|^["]?[A-Za-z]{3} [A-Za-z]{3} [0-9]{2} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT[+-]?[0-9]{4}( \([A-Z]{3}\))?["]?$|^["]?[A-Za-z]{3} [0-9]{2}, [0-9]{4} [0-9]{2}:[0-9]{2} [AP]M [A-Z]{3}["]?$/
);

// Matches standard datetime (YYYY-MM-DD HH:MM:SS)
const datetime = new RegExp(
    /^\/Date\(\-?\d{12,14}\)\/$|^["]?(?:\d{4})[-\/\.]?(0[1-9]|1[0-2])[-\/\.]?(0[1-9]|[12][0-9]|3[01])[T\s]([01][0-9]|2[0-3]):([0-5][0-9])(:([0-5][0-9]))?(?:\.\d+)?["]?$|^["]?[A-Za-z]{3} [A-Za-z]{3} [0-9]{2} [0-9]{4} [0-9]{2}:[0-5][0-9](:[0-5][0-9])?["]?$|^["]?[A-Za-z]{3} [0-9]{2}, [0-9]{4} [0-9]{2}:[0-5][0-9](:[0-5][0-9])? [AP]M["]?$/
);
  
// Matches date-only formats (YYYY-MM-DD, MM/DD/YYYY)
const date = new RegExp(
    /^(?:\d{4}[-\/\.](0[1-9]|1[0-2])[-\/\.](0[1-9]|[12][0-9]|3[01]))|(?:[0-9]{2}[-\/\.][0-9]{2}[-\/\.][0-9]{4})$/
);
  
// Matches time-only formats (HH:MM:SS)
const time = new RegExp(/^([01]?[0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9])?$/);

// Matches binary values (0s and 1s)
const binary = new RegExp(/^[01]+$/i);

// Matches boolean values (`true`, `false`, `0`, `1`)
const boolean = new RegExp(/^(?:[01]|true|false)$/i);

export const regexPatterns = {
    number,
    decimal,
    exponential,
    datetimetz,
    datetime,
    date,
    time,
    binary,
    boolean
};