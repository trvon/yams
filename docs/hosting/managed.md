# Managed Hosting

Managed YAMS with zero-ops setup, reliability, and scalability.

**Status:** Private preview. Join the early access list below.

## Features

- **Provisioning:** Fast projects, tokens, isolation
- **Reliability:** Automated backups, rolling maintenance
- **Observability:** Usage metrics, request logs
- **Security:** Least-privilege tokens, IP allow lists (preview), SSO (planned)
- **Support:** Community (preview), paid tiers (later)

## Architecture

- Control plane: projects, tokens, billing
- Data plane: YAMS engine + storage
- Cloudflare integration (under validation)

## Use Cases

- Teams building LLM-powered apps needing persistent memory with minimal ops
- Developers using YAMS locally who want a hosted counterpart

Need strict controls or air-gapped deployment? See [Self-hosting](self_hosting.md).

## Managed vs Self-hosted

| Managed | Self-hosted |
|---------|-------------|
| Click-to-provision | Full control |
| Backups + metrics included | You run backups, monitoring, upgrades |
| Minimal ops | Complete infrastructure control |

## Early Access

Help shape YAMS Managed Hosting. We're onboarding teams to validate:
- Provisioning and control plane (projects, tokens, isolation)
- Backups, observability, operational flows

### Waitlist

<form action="https://formspree.io/f/xgvzbbzy" method="POST" class="waitlist-form">
  <input type="email" name="email" placeholder="email@domain.com" required />
  <input type="text" name="name" placeholder="Full name (optional)" />
  <input type="text" name="company" placeholder="Company (optional)" />
  <textarea name="requirements" placeholder="What do you need from YAMS hosting? (optional)"></textarea>
  <input type="text" name="_gotcha" style="display:none" />
  <input type="hidden" name="_redirect" value="/thanks/" />
  <input type="hidden" name="list" value="hosting-early-access" />
  <button type="submit">Request access</button>
</form>

<p class="privacy-note">We only email about hosting updates. Unsubscribe anytime.</p>

### Process

1. Confirmation email
2. Outreach as slots open
3. Early users influence quotas and integrations

## FAQ

- **OSS compatibility:** Yes, same core engine. Version notes published.
- **SSO & audit logs:** Planned for beta/GA
- **Data portability:** Exports are a design goal

See also: [Pricing (Preview)](pricing.md), [Self-hosting](self_hosting.md)
