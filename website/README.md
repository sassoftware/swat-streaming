# Website

The swat-streaming documentation site, built with [Docusaurus](https://docusaurus.io/). Pages live in `docs/`; sidebar order comes from each page's `sidebar_position` front matter.

### Installation

```
$ npm ci
```

### Local Development

```
$ npm start
```

Starts a local development server at http://localhost:3000/swat-streaming/. Most changes are reflected live without restarting the server.

### Build

```
$ npm run build
```

Generates static content into the `build` directory. The build fails on broken links.

### Deployment

The `Deploy Doc Website to GitHub Pages` workflow (`.github/workflows/deploy-doc.yml`) builds and publishes the site on every push to `main`.
