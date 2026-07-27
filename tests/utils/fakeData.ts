/**
 * Deterministic fake-data generator for larger-scale integration tests.
 *
 * Everything is seeded so datasets are byte-for-byte reproducible across runs (a failing
 * seed reproduces exactly). The multilingual corpus is the extension point: adding a new
 * language / script is a single line in `LANGUAGE_SAMPLES`, and every generated dataset picks
 * it up automatically.
 */

/** Mulberry32 — a tiny, fast, seeded PRNG. Deterministic given the same seed. */
export function makeRng(seed: number): () => number {
    let a = seed >>> 0;
    return function () {
        a |= 0;
        a = (a + 0x6d2b79f5) | 0;
        let t = Math.imul(a ^ (a >>> 15), 1 | a);
        t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
}

/**
 * Multilingual text corpus. ADD A LANGUAGE HERE — one entry — and it flows into every dataset.
 * Includes multiple scripts, RTL (Arabic/Hebrew), combining marks, and multi-codepoint emoji
 * (4-byte + ZWJ sequence + regional-indicator flag) to stress charset/encoding end to end.
 */
export const LANGUAGE_SAMPLES: { lang: string; text: string }[] = [
    { lang: "english", text: "The quick brown fox jumps" },
    { lang: "spanish", text: "El veloz murciélago añejo comía" },
    { lang: "french", text: "Portez ce vieux whisky au juge" },
    { lang: "german", text: "Zwölf Boxkämpfer über den Deich" },
    { lang: "portuguese", text: "Olá, coração — ação e emoção" },
    { lang: "chinese", text: "快速的棕色狐狸跳过懒狗" },
    { lang: "japanese", text: "いろはにほへと 日本語のテスト" },
    { lang: "korean", text: "다람쥐 헌 쳇바퀴에 타고파" },
    { lang: "russian", text: "Съешь же ещё этих мягких булок" },
    { lang: "ukrainian", text: "Їжте ще цих м'яких булочок" },
    { lang: "arabic", text: "نص عربي للاختبار من اليمين" },
    { lang: "hebrew", text: "עברית לבדיקה מימין לשמאל" },
    { lang: "thai", text: "ภาษาไทยสำหรับการทดสอบ" },
    { lang: "hindi", text: "हिन्दी परीक्षण वाक्य" },
    { lang: "greek", text: "Ελληνικά δοκιμαστική πρόταση" },
    { lang: "vietnamese", text: "Tiếng Việt có dấu thanh điệu" },
    { lang: "emoji", text: "🚀 😀 🌍 👨‍👩‍👧‍👦 🇺🇸 café ☕" },
];

export interface FakeRow {
    id: number;
    region: string;
    category: string;
    amount: number;
    score: number;
    active: boolean;
    event_date: string;
    note: string;
}

/**
 * Generate `n` rows with a mix of inferred types (int, decimal, boolean, date, varchar) and a
 * multilingual `note`. `id` is a unique integer key (starting at 1000, outside the 0/1 boolean
 * range so it infers as int), and `note` embeds the row index so id→note is an exact mapping
 * for round-trip integrity checks.
 */
export function makeRows(n: number, seed = 12345): FakeRow[] {
    const rng = makeRng(seed);
    const regions = ["north", "south", "east", "west", "central"];
    const categories = ["alpha", "beta", "gamma", "delta"];
    const rows: FakeRow[] = [];
    for (let i = 0; i < n; i++) {
        const lang = LANGUAGE_SAMPLES[i % LANGUAGE_SAMPLES.length];
        rows.push({
            id: 1000 + i,
            region: regions[Math.floor(rng() * regions.length)],
            category: categories[Math.floor(rng() * categories.length)],
            amount: Math.round(rng() * 1_000_000) / 100,
            score: 100 + Math.floor(rng() * 900),
            active: rng() > 0.5,
            event_date: `2026-${String(1 + (i % 9)).padStart(2, "0")}-15`,
            note: `${lang.lang}: ${lang.text} #${i}`,
        });
    }
    return rows;
}

/**
 * Generate `n` rows drawn from a small categorical space so no single-column or composite
 * natural key is unique — forces the surrogate-key path. `tag` carries multilingual text.
 */
export function makeKeylessRows(n: number, seed = 999): { region: string; bucket: number; tag: string }[] {
    const rng = makeRng(seed);
    const regions = ["north", "south", "east"];
    const rows: { region: string; bucket: number; tag: string }[] = [];
    for (let i = 0; i < n; i++) {
        const lang = LANGUAGE_SAMPLES[i % LANGUAGE_SAMPLES.length];
        rows.push({
            region: regions[Math.floor(rng() * regions.length)],
            bucket: Math.floor(rng() * 5), // 0-4, heavily repeated
            tag: lang.text,
        });
    }
    return rows;
}
