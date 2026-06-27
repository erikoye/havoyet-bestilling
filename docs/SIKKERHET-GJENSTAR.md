# Sikkerhet/GDPR – gjenstående steg (det DU må gjøre)

Opprettet 2026-06-27. Alt det rent tekniske er gjort og deployet. Disse tre
krever en handling fra deg fordi de trenger nøkler/tilgang jeg ikke har.
Kryss av (`[x]`) etter hvert.

> **Merk om Render-tjenesten:** backend-tjenesten heter **`havoyet-nettside`** i
> Render (Python 3, Oregon) — det er denne ene Python-tjenesten. GitHub-repoet
> bak den heter `havoyet-bestilling`, og URL-en er `havoyet-bestilling.onrender.com`.
> (React-frontenden ligger på Vercel, ikke Render.) Alle env-variabler under
> legges på **`havoyet-nettside`**.

---

## 1. Cloudflare Turnstile – ekte bot-beskyttelse  ⏳

Akkurat nå er honningfelle + rate-limit aktivt (stopper enkle boter). Turnstile
gir den «ekte» Cloudflare-utfordringen, men venter på nøkler. Den er gratis.

- [ ] Gå til https://dash.cloudflare.com → **Turnstile** (lag gratis konto om du ikke har).
- [ ] **Add site**: navn «Havøyet», domene `havoyet.no` (legg gjerne til `bestilling.havoyet.no` også), widget-modus **Managed**.
- [ ] Kopiér de to nøklene du får: **Site Key** (offentlig) og **Secret Key** (hemmelig).
- [ ] I **Render** → tjenesten `havoyet-nettside` (backend) → **Environment** → **Add Environment Variable**:
      - Key: `TURNSTILE_SECRET`  · Value: *(Secret Key)* → **Save** (Render redeployer automatisk).
- [ ] Send **Site Key** til meg her i chatten → jeg legger inn widgeten i kassen + kontaktskjema og deployer frontend.

➡️ Resultat: bot-utfordring aktiveres. (Backend er allerede klar: verifiserer mot Cloudflare, og slipper alle gjennom helt til `TURNSTILE_SECRET` er satt – så ingen risiko for å blokkere ekte kunder underveis.)

---

## 2. Bekreft `ADMIN_API_TOKEN` i Render  ⏳

Jeg endret admin-sjekken til «fail-closed»: hvis tokenet mangler, nektes
token-basert tilgang (script/cron). Admin-innlogging i nettleseren fungerer
uansett. Vi bør likевel bekrefte at tokenet er satt.

- [ ] **Render** → `havoyet-nettside` → **Environment** → se etter `ADMIN_API_TOKEN`.
- [ ] Hvis den finnes med en verdi → ✅ ingenting mer å gjøre.
- [ ] Hvis den mangler/er tom → lag en lang tilfeldig verdi og legg den inn:
      - Terminal: `openssl rand -hex 32` → kopier resultatet inn som verdi → **Save**.
- [ ] Si fra til meg om du endret den (i tilfelle et script bruker den gamle).

---

## 3. Git-historikk-purge – fjern kunde-PII fra gammel historikk  ⏳

Filene `data/customers_baseline.json`, `vipps_baseline.json`,
`card_payments_baseline.json` er fjernet fra git **fremover**, men ligger
fortsatt i **tidligere commits**. For full GDPR-sletting må historikken skrives om.

⚠️ Dette er en destruktiv operasjon: historikken endres og må **force-pushes**.
Alle som har en klon av repoet må klone på nytt etterpå.

- [ ] Bekreft at du har en **backup** av de tre filene (de ligger lokalt + på Render-disken). Si fra, så tar jeg en sikkerhetskopi til en egen mappe først.
- [ ] Gi meg **klarsignal** her i chatten.
- [ ] *(Jeg gjør resten:)* kjører `git filter-repo` for å slette de tre filene fra hele historikken, force-pusher, og verifiserer at Render fortsatt deployer + at admin/nettside virker etterpå.

---

### Allerede ferdig (ingen handling nødvendig)
- Vipps-callback verifiseres mot Vipps-API før «betalt».
- Microsoft Clarity kjører kun ved samtykke.
- GDPR art. 6 samtykke-boks i alle kasse-flyter.
- Serverside-validering: e-post/telefon-format + leveringssone.
- Honningfelle + durabel rate-limit på ordre/kontakt.
- Admin fail-closed; sletting av ordrer krever nå admin.
- Kunde-PII fjernet fra git fremover (+ `.gitignore`).
