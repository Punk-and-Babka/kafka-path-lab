import { readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

const outputDirectory = join(process.cwd(), "dist", "client");
const rawBasePath = process.env.GITHUB_PAGES_BASE_PATH ?? "";
const basePath = rawBasePath.replace(/\/+$/, "");

async function htmlFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await htmlFiles(path)));
    else if (entry.isFile() && entry.name.endsWith(".html")) files.push(path);
  }

  return files;
}

for (const file of await htmlFiles(outputDirectory)) {
  const html = await readFile(file, "utf8");
  const fontDirectory = `${basePath}/assets/_vinext_fonts/`;
  const prepared = html
    .replace(/href="[^"]*?\/\.vinext\/fonts\//g, `href="${fontDirectory}`)
    .replace(/url\((?:"|')?[^)"']*?\/\.vinext\/fonts\//g, `url(${fontDirectory}`);

  if (prepared.includes("/.vinext/fonts/")) throw new Error(`Unresolved vinext font path in ${file}`);
  await writeFile(file, prepared);
}

console.log(`Prepared static GitHub Pages output in ${outputDirectory}`);
