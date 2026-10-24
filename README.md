# ZSR monitor

Monitoring meškaní a polohy vlakov ŽSR cez neoficiálne API `mapa.zsr.sk`.

Funkcie: ukladanie snímkov do SQLite, štatistiky meškaní (kategória, dopravca, hodina), vlaky s narušením a NAD,
vlaky bez zmeny polohy, upozornenia na sledované vlaky (Telegram), interaktívna mapa (Leaflet) a publikovanie reportu a mapy na GitHub.

## Inštalácia

```bash
pip install -r requirements.txt
cp .env.example .env     # vyplň WATCH a prípadne Telegram
```

Cookies z prehliadača netreba: skript si session získa sám.

## Použitie

```bash
python zsr_monitor.py collect                 # jeden snímok
python zsr_monitor.py run --interval 60       # zber každú minútu + upozornenia
python zsr_monitor.py run --interval 60 --publish-every 30   # + každých 30 min push na GitHub
python zsr_monitor.py report --hours 24       # docs/report.md
python zsr_monitor.py map [--routes]          # docs/index.html
python zsr_monitor.py publish                 # report + mapa + git commit + push
```

## GitHub

1. `git init && git add . && git commit -m "init"`
2. Vytvor prázdny repozitár na GitHube, potom `git remote add origin <URL> && git push -u origin main`
3. Settings → Pages → zdroj: branch `main`, priečinok `/docs` (mapa bude na `https://<user>.github.io/<repo>/`)

Na GitHub sa posiela iba `docs/` (report a mapa). Databáza (`*.db`) a `.env` sú v `.gitignore`.

## Poznámky

- Pole `Position` je v praxi posledná prejdená stanica, nie GPS poloha, takže odhad rýchlosti a „stojace vlaky“ sú len orientačné.
- Nedokumentované API: používaj rozumný interval (30 s a viac) a over podmienky používania.
- Zber musí niekde bežať nepretržite (PC, Raspberry Pi, VPS). GitHub Actions na to nie sú vhodné (cron min. 5 min a nespoľahlivý).
