const DEFAULT_VALID_TYPES = new Set([
  "pdf",
  "txt",
  "md",
  "docx",
  "doc",
  "pptx",
  "ppt",
  "epub",
  "xmind",
  "html",
  "htm",
  "mhtml",
  // 宏格式与 OpenDocument / RTF：与后端 supportedImportFileExtensions 同口径
  // （docreader 都能解析；这里漏一个就会出现"能传但传上去被拒"的错位）
  "docm",
  "pptm",
  "xlsm",
  "odt",
  "ods",
  "odp",
  "rtf",
  "jpg",
  "jpeg",
  "png",
  "csv",
  "xlsx",
  "xls",
  "mp3",
  "wav",
  "m4a",
  "flac",
  "ogg",
]);

export function shouldRejectKnowledgeFileType(
  filename: string,
  validTypes?: Set<string> | string[],
) {
  const provided = validTypes
    ? validTypes instanceof Set
      ? validTypes
      : new Set(validTypes)
    : undefined;
  const allowed = provided && provided.size > 0 ? provided : DEFAULT_VALID_TYPES;
  const type = filename.substring(filename.lastIndexOf(".") + 1).toLowerCase();

  return !allowed.has(type);
}
