#!/usr/bin/env node

import { cpSync, existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const websiteRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(websiteRoot, "..");
const distRoot = path.join(websiteRoot, "dist");
const rustdocTarget = path.join(repoRoot, "target", "rustdoc-site");
const rustdocSource = path.join(rustdocTarget, "doc");
const rustdocDest = path.join(distRoot, "api", "rustdoc");
const customDomain = process.env.RS_DOCS_CUSTOM_DOMAIN ?? "rs.graphrefly.dev";

const args = new Set(process.argv.slice(2));
const rustdocOnly = args.has("--rustdoc-only");
const checkOnly = args.has("--check");

function run(command, commandArgs) {
	const result = spawnSync(command, commandArgs, {
		cwd: repoRoot,
		stdio: "inherit",
		env: {
			...process.env,
			RUSTDOCFLAGS: "-D warnings",
		},
	});
	if (result.status !== 0) {
		process.exit(result.status ?? 1);
	}
}

function cargoCommand() {
	if (process.env.CARGO) return process.env.CARGO;
	const homeCargo = path.join(process.env.HOME ?? "", ".cargo", "bin", "cargo");
	if (existsSync(homeCargo)) return homeCargo;
	return "cargo";
}

run(cargoCommand(), [
	"doc",
	"-p",
	"graphrefly-rs",
	"--all-features",
	"--no-deps",
	"--target-dir",
	rustdocTarget,
]);

if (checkOnly) {
	console.log("rustdoc generated cleanly");
	process.exit(0);
}

if (rustdocOnly) {
	console.log(`rustdoc generated at ${rustdocSource}`);
	process.exit(0);
}

mkdirSync(path.dirname(rustdocDest), { recursive: true });
rmSync(rustdocDest, { recursive: true, force: true });
cpSync(rustdocSource, rustdocDest, { recursive: true });
writeFileSync(
	path.join(rustdocDest, "index.html"),
	`<!doctype html>
<html lang="en">
	<head>
		<meta charset="utf-8">
		<meta http-equiv="refresh" content="0; url=graphrefly/index.html">
		<title>GraphReFly Rust API Reference</title>
	</head>
	<body>
		<a href="graphrefly/index.html">Open the graphrefly crate API reference</a>
	</body>
</html>
`,
);

if (customDomain.trim().length > 0) {
	writeFileSync(path.join(distRoot, "CNAME"), `${customDomain.trim()}\n`);
}

writeFileSync(
	path.join(distRoot, "artifact-manifest.json"),
	`${JSON.stringify(
		{
			package: "graphrefly-rs",
			crate: "graphrefly",
			framework: "astro-starlight",
			route: process.env.ASTRO_BASE_PATH ?? "/",
			source: "website/src/content/docs",
			apiGenerator: "rustdoc",
			apiPath: "/api/rustdoc/",
		},
		null,
		2,
	)}\n`,
);

console.log("prepared Starlight graphrefly Rust docs artifact");
