// Helpers communs aux deux documents Word du bloc 2 (docx-js).
const fs = require("fs");
const {
  Document, Paragraph, TextRun, Table, TableRow, TableCell, ImageRun, Header, Footer,
  AlignmentType, LevelFormat, HeadingLevel, BorderStyle, WidthType, ShadingType,
  PageNumber, PageBreak, TableOfContents, TabStopType,
} = require("docx");

const ACCENT = "6C63FF";
const INK = "1A1F36";
const MUTED = "5F6B7A";
const FONT = "Arial";
const MONO = "Consolas";
const CONTENT_W = 9026; // A4, marges 2,54 cm

// Mini-markup : **gras**, `code`, _italique_
function runs(text, base = {}) {
  const out = [];
  const re = /(\*\*[^*]+\*\*|`[^`]+`|_[^_]+_)/g;
  let last = 0, m;
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) out.push(new TextRun({ text: text.slice(last, m.index), ...base }));
    const t = m[0];
    if (t.startsWith("**")) out.push(new TextRun({ text: t.slice(2, -2), bold: true, ...base }));
    else if (t.startsWith("`")) out.push(new TextRun({ text: t.slice(1, -1), font: MONO, size: (base.size || 21) - 2, color: "3B3F8F" }));
    else out.push(new TextRun({ text: t.slice(1, -1), italics: true, ...base }));
    last = m.index + t.length;
  }
  if (last < text.length) out.push(new TextRun({ text: text.slice(last), ...base }));
  return out;
}

const H1_TITLES = [];
const h1 = (t, pageBreak = true) => (H1_TITLES.push(t), new Paragraph({ heading: HeadingLevel.HEADING_1, pageBreakBefore: pageBreak, children: [new TextRun(t)] }));
const h2 = (t) => new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(t)] });
const h3 = (t) => new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun(t)] });
const p = (t, opts = {}) => new Paragraph({ spacing: { after: 140, line: 288 }, ...opts, children: runs(t, opts.run || {}) });
const bullets = (items) => items.map((t) => new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 60, line: 276 }, children: runs(t) }));
const numbered = (items, ref = "numbers") => items.map((t) => new Paragraph({ numbering: { reference: ref, level: 0 }, spacing: { after: 60, line: 276 }, children: runs(t) }));

function code(text) {
  return text.split("\n").map((line, i, arr) => new Paragraph({
    shading: { type: ShadingType.CLEAR, fill: "F4F5FA" },
    spacing: { before: i === 0 ? 80 : 0, after: i === arr.length - 1 ? 160 : 0, line: 240 },
    indent: { left: 120, right: 120 },
    children: [new TextRun({ text: line.length ? line : " ", font: MONO, size: 17, color: INK })],
  }));
}

function note(text, kind = "info") {
  const colors = { info: ["EEF0FF", ACCENT], warn: ["FFF6E5", "C98A00"], ok: ["EAF7EF", "2E7D32"] }[kind];
  return new Paragraph({
    shading: { type: ShadingType.CLEAR, fill: colors[0] },
    border: { left: { style: BorderStyle.SINGLE, size: 24, color: colors[1], space: 8 } },
    spacing: { before: 120, after: 180, line: 276 },
    indent: { left: 160, right: 120 },
    children: runs(text, { size: 20 }),
  });
}

const border = { style: BorderStyle.SINGLE, size: 4, color: "D5D8E3" };
const borders = { top: border, bottom: border, left: border, right: border };

function table(headers, rows, widths) {
  const total = widths.reduce((a, b) => a + b, 0);
  const scaled = widths.map((w) => Math.floor((w / total) * CONTENT_W));
  scaled[scaled.length - 1] += CONTENT_W - scaled.reduce((a, b) => a + b, 0);
  const cell = (text, i, header) => new TableCell({
    borders,
    width: { size: scaled[i], type: WidthType.DXA },
    shading: header ? { fill: ACCENT, type: ShadingType.CLEAR } : undefined,
    margins: { top: 60, bottom: 60, left: 100, right: 100 },
    children: String(text).split("\n").map((line) => new Paragraph({
      spacing: { after: 20 },
      children: header ? [new TextRun({ text: line, bold: true, color: "FFFFFF", size: 18 })] : runs(line, { size: 18 }),
    })),
  });
  return [
    new Table({
      width: { size: CONTENT_W, type: WidthType.DXA },
      columnWidths: scaled,
      rows: [
        new TableRow({ tableHeader: true, children: headers.map((h, i) => cell(h, i, true)) }),
        ...rows.map((r) => new TableRow({ children: r.map((c, i) => cell(c, i, false)) })),
      ],
    }),
    new Paragraph({ spacing: { after: 120 }, children: [] }),
  ];
}

function image(path, caption, maxWidthPx = 600) {
  const buf = fs.readFileSync(path);
  // Dimensions PNG : octets 16-23 de l'en-tête IHDR
  const w = buf.readUInt32BE(16), h = buf.readUInt32BE(20);
  const width = Math.min(maxWidthPx, 600);
  const height = Math.round((h / w) * width);
  return [
    new Paragraph({
      alignment: AlignmentType.CENTER, spacing: { before: 120, after: 60 }, keepNext: true,
      children: [new ImageRun({ type: "png", data: buf, transformation: { width, height },
        altText: { title: caption, description: caption, name: caption } })],
    }),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 200 },
      children: [new TextRun({ text: caption, italics: true, size: 18, color: MUTED })] }),
  ];
}

function cover(title, subtitle, tagline, fields) {
  return [
    new Paragraph({ spacing: { before: 2200 }, alignment: AlignmentType.CENTER,
      children: [new TextRun({ text: "STRIPE", bold: true, size: 72, color: ACCENT })] }),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 200 },
      children: [new TextRun({ text: title, bold: true, size: 36, color: INK })] }),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 120 },
      children: [new TextRun({ text: subtitle, size: 24, color: ACCENT })] }),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 160, after: 900 },
      children: [new TextRun({ text: tagline, italics: true, size: 19, color: MUTED })] }),
    ...table(["Champ", "Valeur"], fields, [30, 70]),
  ];
}

function makeDoc({ headerLeft, footerText, children }) {
  return new Document({
    creator: "Patrice Duclos",
    title: headerLeft,
    styles: {
      default: { document: { run: { font: FONT, size: 21, color: INK } } },
      paragraphStyles: [
        { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 32, bold: true, font: FONT, color: ACCENT },
          paragraph: { spacing: { before: 120, after: 200 }, outlineLevel: 0 } },
        { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 25, bold: true, font: FONT, color: INK },
          paragraph: { spacing: { before: 280, after: 120 }, outlineLevel: 1 } },
        { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 22, bold: true, font: FONT, color: "3B3F8F" },
          paragraph: { spacing: { before: 200, after: 80 }, outlineLevel: 2 } },
      ],
    },
    numbering: {
      config: [
        { reference: "bullets", levels: [{ level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 540, hanging: 280 } } } }] },
        { reference: "numbers", levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 540, hanging: 320 } } } }] },
        { reference: "numbers2", levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 540, hanging: 320 } } } }] },
        { reference: "numbers3", levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 540, hanging: 320 } } } }] },
      ],
    },
    sections: [{
      properties: { page: { size: { width: 11906, height: 16838 }, margin: { top: 1440, right: 1440, bottom: 1300, left: 1440 } } },
      headers: { default: new Header({ children: [new Paragraph({
        border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: ACCENT, space: 4 } },
        tabStops: [{ type: TabStopType.RIGHT, position: CONTENT_W }],
        children: [new TextRun({ text: headerLeft, size: 16, color: MUTED }), new TextRun({ text: "\tCertification AIA RNCP41993 — Bloc 2", size: 16, color: MUTED })],
      })] }) },
      footers: { default: new Footer({ children: [new Paragraph({
        alignment: AlignmentType.CENTER,
        border: { top: { style: BorderStyle.SINGLE, size: 6, color: ACCENT, space: 4 } },
        children: [new TextRun({ text: `${footerText}  ·  Page `, size: 16, color: MUTED }), new TextRun({ children: [PageNumber.CURRENT], size: 16, color: MUTED })],
      })] }) },
      children: children.flat(Infinity).flatMap((c) => (c === TOC_TOKEN ? renderToc() : [c])),
    }],
  });
}

// Sommaire statique : un champ TOC Word reste vide tant qu'il n'est pas mis à
// jour manuellement ; on liste donc directement les titres de niveau 1.
const TOC_TOKEN = { __toc: true };
const toc = () => [TOC_TOKEN];
function renderToc() {
  return [
    new Paragraph({ pageBreakBefore: true, spacing: { after: 240 }, children: [new TextRun({ text: "Sommaire", bold: true, size: 32, color: ACCENT })] }),
    ...H1_TITLES.map((t) => new Paragraph({
      spacing: { after: 90 },
      border: { bottom: { style: BorderStyle.DOTTED, size: 4, color: "D5D8E3", space: 2 } },
      children: [new TextRun({ text: t, size: 22 })],
    })),
  ];
}
const spacer = () => new Paragraph({ children: [] });

module.exports = { h1, h2, h3, p, bullets, numbered, code, note, table, image, cover, makeDoc, toc, spacer, runs, PageBreak, Paragraph };
