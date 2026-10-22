// Optional thousands separators (1,000 or 1.000)
const number = new RegExp(/^-?[0-9]{1,3}([.,]?[0-9]{3})*$/);

// US and European decimal formats
const decimal = new RegExp(/^-?[0-9]{1,3}([.,]?[0-9]{3})*([.,][0-9]+)$/);

// Scientific notation (1.23e10, -3.5E-5)
const exponential = new RegExp(/^(-?[0-9]+(\.[0-9]+)?[eE][-+]?[0-9]+)$/);

// ISO 8601, Unix timestamps, and JS Date formats
const datetimetz = new RegExp(
    /^\/Date\(\-?\d{12,14}\+\d{4}\)\/$|^["]?(?:\d{4})[-\/\.]?(0[1-9]|1[0-2])[-\/\.]?(0[1-9]|[12][0-9]|3[01])[T\s]([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(?:\.\d+)?(?:Z|[+-](?:[01][0-9]|2[0-3]):?[0-5][0-9]| [A-Z]{3})["]?$|^["]?[A-Za-z]{3} [A-Za-z]{3} [0-9]{2} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT[+-]?[0-9]{4}( \([A-Z]{3}\))?["]?$|^["]?[A-Za-z]{3} [0-9]{2}, [0-9]{4} [0-9]{2}:[0-9]{2} [AP]M [A-Z]{3}["]?$/
);

// YYYY-MM-DD HH:MM:SS
const datetime = new RegExp(
    /^\/Date\(\-?\d{12,14}\)\/$|^["]?(?:\d{4})[-\/\.]?(0[1-9]|1[0-2])[-\/\.]?(0[1-9]|[12][0-9]|3[01])[T\s]([01][0-9]|2[0-3]):([0-5][0-9])(:([0-5][0-9]))?(?:\.\d+)?["]?$|^["]?[A-Za-z]{3} [A-Za-z]{3} [0-9]{2} [0-9]{4} [0-9]{2}:[0-5][0-9](:[0-5][0-9])?["]?$|^["]?[A-Za-z]{3} [0-9]{2}, [0-9]{4} [0-9]{2}:[0-5][0-9](:[0-5][0-9])? [AP]M["]?$/
);
  
// YYYY-MM-DD, MM/DD/YYYY
const date = new RegExp(
    /^(?:(?:\d{4}[-\/\.](0[1-9]|1[0-2])[-\/\.](0[1-9]|[12][0-9]|3[01]))|(?:(?:(0[1-9]|1[0-2])[-\/\.](0[1-9]|[12][0-9]|3[01])|(0[1-9]|[12][0-9]|3[01])[-\/\.](0[1-9]|1[0-2]))[-\/\.][0-9]{4}))$/
);
  
// HH:MM:SS
const time = new RegExp(/^([01]?[0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9])?$/);

const binary = new RegExp(/^[01]+$/i);

// `0`/`1` deliberately NOT matched: a bare 0/1 is more often a small integer than a flag, and
// mis-typing it as boolean breaks integer comparisons. 0/1 flags opt in via `booleanColumns` (R3).
const boolean = new RegExp(/^(?:true|false)$/i);

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