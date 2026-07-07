import starlight from "@astrojs/starlight";
import { defineConfig } from "astro/config";
import { apiSidebar } from "./src/generated/api-sidebar.mjs";

const base = process.env.ASTRO_BASE_PATH ?? "/";

export default defineConfig({
	site: process.env.ASTRO_SITE_URL ?? "https://rs.graphrefly.dev",
	base,
	server: { port: 4325 },
	integrations: [
		starlight({
			title: "GraphReFly Rust",
			description: "Package-local Rust documentation for graphrefly-rs.",
			components: {
				Header: "./src/components/Header.astro",
				Footer: "./src/components/Footer.astro",
				MobileMenuFooter: "./src/components/MobileMenuFooter.astro",
				Sidebar: "./src/components/Sidebar.astro",
				ThemeSelect: "./src/components/ThemeSelect.astro",
			},
			customCss: ["./src/styles/custom.css"],
			head: [
				{
					tag: "script",
					content:
						"(function(){try{var k='starlight-theme';if(localStorage.getItem(k)===null){localStorage.setItem(k,'light');document.documentElement.dataset.theme='light'}}catch(e){}})()",
				},
				{ tag: "link", attrs: { rel: "preconnect", href: "https://fonts.googleapis.com" } },
				{
					tag: "link",
					attrs: { rel: "preconnect", href: "https://fonts.gstatic.com", crossorigin: "" },
				},
				{
					tag: "script",
					content: `(function(){var p=location.pathname,l=p.toLowerCase();if(p!==l)location.replace(l+location.search+location.hash)})()`,
				},
			],
			social: [
				{
					icon: "github",
					label: "graphrefly-rs",
					href: "https://github.com/graphrefly/graphrefly-rs",
				},
			],
			sidebar: [
				{
					label: "Start",
					items: [
						{ label: "Overview", link: "/" },
						{ label: "Quick Start", link: "/quickstart" },
					],
				},
				{
					label: "API Reference",
					collapsed: true,
					items: [{ label: "Overview", link: "/api" }, ...apiSidebar],
				},
				{
					label: "Examples",
					items: [{ label: "Overview", link: "/examples" }],
				},
				{
					label: "Recipes",
					items: [{ label: "Overview", link: "/recipes" }],
				},
				{
					label: "Integrations",
					items: [{ label: "Overview", link: "/integrations" }],
				},
				{
					label: "Release",
					items: [{ label: "Release", link: "/release" }],
				},
			],
		}),
	],
});
