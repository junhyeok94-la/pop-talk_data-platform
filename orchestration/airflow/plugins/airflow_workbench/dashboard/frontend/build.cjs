const esbuild = require("esbuild");
const fs = require("node:fs");
const path = require("node:path");
esbuild.buildSync({
  entryPoints: [path.join(__dirname, "libraries.js")],
  bundle: true,
  minify: true,
  format: "iife",
  target: "es2022",
  outfile: path.join(__dirname, "../static/studio-vendor.js"),
  legalComments: "linked",
});
const lock = JSON.parse(
  fs.readFileSync(path.join(__dirname, "package-lock.json"), "utf8"),
);
const packages = Object.entries(lock.packages).filter(
  ([name, info]) => name.startsWith("node_modules/") && !info.dev,
);
let notices = "Bundled UI libraries; no CDN or runtime Node server.\n\n";
for (const [name, info] of packages) {
  const root = path.join(__dirname, name);
  notices += `\n=== ${name.replace("node_modules/", "")} ${info.version} ===\n`;
  for (const filename of fs
    .readdirSync(root)
    .filter((x) => /^(LICENSE|NOTICE)(\..*)?$/i.test(x)))
    notices += fs.readFileSync(path.join(root, filename), "utf8") + "\n";
}
fs.writeFileSync(
  path.join(__dirname, "../static/THIRD-PARTY-NOTICES.txt"),
  notices,
);
