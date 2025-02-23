// Matches whole numbers with optional thousands separators (1,000 or 1.000)
const number = new RegExp(/^-?[0-9]{1,3}([.,]?[0-9]{3})*$/);

// Matches decimal numbers (both US and European formats)
const decimal = new RegExp(/^-?[0-9]{1,3}([.,]?[0-9]{3})*([.,][0-9]+)$/);

// Matches scientific notation (1.23e10, -3.5E-5)
const exponential = new RegExp(/^(-?[0-9]+(\.[0-9]+)?[eE][-+]?[0-9]+)$/);

// Matches datetime with timezone (ISO 8601, Unix timestamps, and JS Date formats)
const datetimetz = new RegExp(
    /^\/Date\([0-9]{12,14}(\+[0-9]{4})?\)\/$|^["]?(?:\d{4})[-\/\.]?(0[1-9]|1[0-2])[-\/\.]?(0[1-9]|[12][0-9]|3[01])[T\s]?([01][0-9]|2[0-3]):([0-5][0-9]):?([0-5][0-9])?(\.[0-9]+)?(Z|([+-](?:[01][0-9]|2[0-3]):?[0-5][0-9]))?["]?$/i
);  

// Matches standard datetime (YYYY-MM-DD HH:MM:SS)
const datetime = new RegExp(
    /^\/Date\([0-9]{12,14}\)\/$|^["]?(?:\d{4})[-\/\.](0[1-9]|1[0-2])[-\/\.](0[1-9]|[12][0-9]|3[01])[T\s]?([01][0-9]|2[0-3]):([0-5][0-9]):?([0-5][0-9])?(\.[0-9]+)?["]?$/i
);
  
// Matches date-only formats (YYYY-MM-DD, MM/DD/YYYY)
const date = new RegExp(
    /^(?:\d{4}[-\/\.](0[1-9]|1[0-2])[-\/\.](0[1-9]|[12][0-9]|3[01]))|(?:[0-9]{2}[-\/\.][0-9]{2}[-\/\.][0-9]{4})$/
);
  
// Matches time-only formats (HH:MM:SS)
const time = new RegExp(/^([01]?[0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9])?$/);

// Matches JSON objects
const json = new RegExp(/^{.+:.+}$/gmi);

// Matches binary values (0s and 1s)
const binary = new RegExp(/^[0-1]*$/gmi);

// Matches boolean values (`true`, `false`, `0`, `1`)
const boolean = new RegExp(/^[0-1]{1}$|^true$|^false$/gmi);

export const regexPatterns = {
    number,
    decimal,
    exponential,
    datetimetz,
    datetime,
    date,
    time,
    json,
    binary,
    boolean
};