import type * as Preset from "@docusaurus/preset-classic";
import type { Config } from "@docusaurus/types";

import { themes as prismThemes } from "prism-react-renderer";

const config: Config = {
  title: "SWAT Streaming Documentation",
  tagline: "Stream Kafka topics into SAS Viya CAS tables with SWAT",
  favicon: "https://raw.githubusercontent.com/sassoftware/.github/refs/heads/main/assets/sas.png",

  // Set the production url of your site here
  url: "https://sassoftware.github.io",
  // Set the /<baseUrl>/ pathname under which your site is served
  baseUrl: "/swat-streaming/",

  // GitHub pages deployment config.
  organizationName: "sassoftware",
  projectName: "swat-streaming",

  onBrokenLinks: "throw",

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "classic",
      {
        docs: {
          routeBasePath: "/",
          // Remove this to remove the "edit this page" links.
          editUrl:
            "https://github.com/sassoftware/swat-streaming/tree/main/website/",
        },
        blog: false,
        theme: {
          customCss: ["./src/css/custom.css"],
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    docs: {
      sidebar: {
        hideable: true,
      },
    },
    navbar: {
      title: "SWAT Streaming",
      logo: {
        alt: "SAS",
        src: "https://raw.githubusercontent.com/sassoftware/.github/refs/heads/main/assets/sas.png",
      },
      items: [
        {
          href: "https://github.com/sassoftware/swat-streaming",
          className: "header-github-link",
          title: "GitHub repository",
          position: "right",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Documentation",
          items: [
            { label: "Introduction", to: "/" },
            { label: "Deployment", to: "/deployment" },
            { label: "Configuration", to: "/configuration" },
          ],
        },
        {
          title: "Resources",
          items: [
            {
              label: "SWAT (Python)",
              href: "https://sassoftware.github.io/python-swat/",
            },
            {
              label: "CAS REST API",
              href: "https://developer.sas.com/rest-apis/cas",
            },
            {
              label: "confluent-kafka",
              href: "https://pypi.org/project/confluent-kafka/",
            },
          ],
        },
        {
          title: "Support",
          items: [
            {
              label: "GitHub Repository",
              href: "https://github.com/sassoftware/swat-streaming",
            },
            {
              label: "Issues",
              href: "https://github.com/sassoftware/swat-streaming/issues",
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()}, SAS Institute Inc., Cary, NC, USA. All Rights Reserved. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ["bash", "json"],
    },
  } satisfies Preset.ThemeConfig,

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: "throw",
    },
  },

  themes: [
    [
      "@easyops-cn/docusaurus-search-local",
      {
        docsRouteBasePath: "/",
        explicitSearchResultPath: true,
        hashed: true,
        highlightSearchTermsOnTargetPage: true,
      },
    ],
    "@docusaurus/theme-mermaid",
  ],
};

export default config;
