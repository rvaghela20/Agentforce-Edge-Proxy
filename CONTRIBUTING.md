# Contributing

## Setup

1. Clone the repo
2. `npm install` at root (prettier + Apex plugins)
3. `cd cloudflare-worker && npm install`
4. Copy `cloudflare-worker/.dev.vars.example` to `cloudflare-worker/.dev.vars` and fill in your values

## Local dev

```bash
cd cloudflare-worker
npm run dev        # starts wrangler dev server
```

Without Redis/Salesforce creds, the worker falls back to mock data automatically.

## Tests

```bash
cd cloudflare-worker
npm test           # run once
npm run test:watch # watch mode
```

## Code style

- We use Prettier. Run `npm run prettier` from root before committing.
- Keep comments short. If the function name explains it, skip the comment.
- No JSDoc on obvious functions.

## Commits

- Keep commits small and focused
- Include tests alongside the code they cover when possible
- Use plain lowercase messages: `add search caching tests`, `fix token refresh on 401`

## Deploying

```bash
cd cloudflare-worker
npm run deploy
```

Make sure wrangler secrets are set first (see README).
