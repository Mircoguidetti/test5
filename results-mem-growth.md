# Crescita della memoria del motore (branch `mem-growth`)

Tutte le misure sono su MinIO locale (nessuna richiesta ad AWS), motore legato a un core con `taskset`, documenti di ~1 KB
inseriti con `tools/round3/load.py`. Ogni numero è etichettato: **MISURATO** (uscito da una prova di questo branch) o
**STIMATO** (calcolato da misure, non osservato direttamente). Binario "prima": `main` al commit `4e2217a`.
I numeri di round3 citati sono quelli di `results-round3.md` (sezione A3).

## 1. La crescita, riprodotta

Script ripetibile: `tools/mem/memcheck.py` (N database, R scritture al secondo per database, RSS ogni 5 s; poi le
scritture si fermano, i database si chiudono per inattività e lo script continua a misurare).

- 50 database × 2 scritture/s per 10 minuti, binario prima: RSS da 35 a 401 MB con 60.035 scritture, cioè 5,9 KB per
  scrittura (**MISURATO**).
- Stesso carico per 15 minuti, due volte: 523 e 548 MB a fine scritture, 4,5 e 4,8 KB per scrittura (**MISURATO**).
- Dopo la chiusura di tutti i 50 database la RSS resta dov'era: 522 MB su 523, 548 su 548 (**MISURATO**). È la
  "memoria che non torna indietro".
- Riferimento round3, stesso carico: 374 MB a 10 minuti, 874 a 30, 967 a 60.

## 2. Le cause, misurate

Strumento: una build di misura con jemalloc e il suo profilatore (`--features heap-prof`), che scrive un profilo
dell'heap ogni N secondi; `jeprof --base` dà la differenza tra due istanti per punto di allocazione.

**Causa 1, la più grande: la memtable dei database scritti lentamente.**
- In 3 minuti di carico (circa 18.000 scritture) l'heap vivo campionato è cresciuto di 26,4 MB: 20,1 MB sono le copie di
  chiave e valore che slatedb fa in `WriteBatch::put` e tiene nella memtable, 7,1 MB i nodi della skiplist della memtable
  (**MISURATO**, profilo a campione). Quindi ~1,5 KB di heap vivo per scrittura da 1 KB, e la memtable è quasi tutta la
  crescita viva (**MISURATO**).
- slatedb scarica la memtable solo quando è piena (8 MB stimati) o dopo 4096 flush del WAL: a 2 scritture/s un database
  teneva in memoria ~34 minuti di scritture, circa 6 MB vivi (**STIMATO**: 4096 × 1,5 KB). Con 50 database fa ~300 MB vivi
  (**STIMATO**), coerente con la salita di round3 fino a ~30 minuti e il rallentamento dopo.

**Causa 2: il malloc di sistema (glibc) moltiplica la memoria e non la restituisce.**
- Stesso carico (50 × 2, 5 minuti): con glibc 5,5 KB di RSS per scrittura, con jemalloc 1,9 KB (**MISURATO**), per lo stesso
  heap vivo di ~1,5 KB.
- Con `MALLOC_ARENA_MAX=2` la pendenza con glibc resta 6,2 KB per scrittura (**MISURATO**): non è il numero di arene, è la
  frammentazione dentro l'arena (le scritture di lunga vita sono mescolate a quelle temporanee delle richieste).
- Dopo la chiusura dei database glibc restituisce lo 0,5% (**MISURATO**).

**Causa 3: la cache dei blocchi condivisa si riempie con i blocchi appena scritti.**
- slatedb, per default, mette in cache i blocchi dati di ogni SST L0 che scrive allo scarico della memtable. Il motore usa
  uno scope di cache nuovo a ogni apertura, quindi dopo la chiusura del database quei blocchi non vengono più letti, ma
  restano finché la cache (256 + 64 MB) non è piena.
- Dopo la chiusura di tutti i database (prova da 3 minuti) l'heap vivo è sceso da 97 a 60 MB, contro 42 alla partenza
  (**MISURATO**); dei MB rimasti, ~19 MB sono blocchi inseriti in cache dallo scarico della memtable (**MISURATO**, profilo a
  campione).

**Causa 4: i cursori non scadevano mai.**
- Un cursore tiene in memoria tutto il resto del risultato. Il motore non aveva un timeout: un client che smette di leggere
  senza `killCursors` lasciava il cursore aperto per sempre (mongod lo chiude dopo 10 minuti).
- Prova con il binario prima: 40 cursori abbandonati su una collezione di 500 documenti da 1 KB, dopo 700 s sono ancora 40,
  ancora utilizzabili, e la RSS è salita di 27 MB senza tornare (**MISURATO**).
- Non pesa nel carico di sole scritture di round3, ma è una struttura che non si libera.

**Controllati e non colpevoli.**
- Log dei change stream: già limitato (`--change-log-max-mb 32`); nel profilo pesa ~10 MB e non sale oltre (**MISURATO**).
- Strutture per database dopo la chiusura: nel profilo dopo la chiusura non resta altro di rilevante oltre a cache e log dei
  change stream (**MISURATO**).
- Costo fisso per database aperto: 0,13 MB di RSS prima, 0,095 MB di RSS e 0,195 MB di heap vivo dopo, con 200 database
  aperti (**MISURATO**, `tools/mem/perdb.py`). È sotto gli 0,26-0,4 MB della versione precedente: la crescita era per
  scrittura, non per database.

## 3. Le correzioni

1. **jemalloc come allocatore** (`mongo-server/src/main.rs`), con `background_thread` perché le pagine liberate tornino al
   sistema anche quando il processo è fermo. `serverStatus` ora riporta `mem.heapAllocatedBytes` (heap vivo) e
   `mem.heapResidentBytes` (memoria tenuta dall'allocatore).
2. **Età massima della memtable**, `--memtable-max-age-secs` (default 600). Un database scarica la memtable quando la sua
   scrittura più vecchia non ancora scaricata ha quell'età; ogni database sceglie a caso un istante tra 3/4 dell'età e l'età,
   così database aperti insieme non scaricano tutti nello stesso momento (senza questo, 50 database che scaricavano insieme
   facevano salire la RSS di ~40 MB per un minuto, **MISURATO**). Un database scritto velocemente riempie la memtable prima e
   non è toccato: è la differenza con lo "scarico ogni 256 scritture" provato e scartato in round3, che colpiva proprio i
   database veloci. 0 = spento.
3. **Niente blocchi dati appena scritti in cache**, `--cache-flushed-blocks` (default false). Restano in cache i blocchi di
   indice e filtro; un blocco dati entra in cache quando viene letto. `true` torna al comportamento di slatedb.
4. **Scadenza dei cursori**, `--cursor-timeout-secs` (default 600, come `cursorTimeoutMillis` di mongod) per find,
   aggregate, cursori tailable e change stream. Un `find` con `noCursorTimeout: true` scade dopo 30 minuti di inattività
   (come la sessione di mongod). `serverStatus` riporta `metrics.cursor.timedOut`.

## 4. Risultati dopo la correzione

**50 database × 2 scritture/s, 15 minuti (89.965 scritture), stesso carico per tutte e tre le prove:**
- prima: 548 MB a fine scritture, 548 MB dopo la chiusura, 4,8 KB per scrittura (**MISURATO**);
- solo jemalloc (le altre correzioni spente con i flag): 223 MB, poi 205 MB, 1,7 KB per scrittura (**MISURATO**);
- tutte le correzioni: 152 MB, poi 105 MB, 0,6 KB per scrittura sull'intera prova (**MISURATO**). Dopo il primo scarico per
  età (a ~10 minuti) la RSS resta tra 128 e 141 MB (**MISURATO**).

**Prova lunga, 60 minuti, 50 × 2 (359.606 scritture), tutte le correzioni:** RSS 127 MB a 5 minuti, 137 a 10, 148 a 30,
202 a 60, 152 dopo la chiusura (**MISURATO**). Round3 con lo stesso carico: 374, 874 e 967 MB. A 60 minuti la RSS è circa
4,8 volte più bassa (**MISURATO**, dal confronto dei due numeri).

**Cosa sale ancora dopo 30 minuti.** Una prova da 45 minuti con la build di misura (stesse correzioni, 269.840 scritture):
heap vivo nelle punte 114 MB a 5 minuti, 132 a 15, 144 a 25, 167 a 40, 156 a 45 (**MISURATO**; l'heap oscilla di ~50 MB
con gli scarichi delle memtable). Tra 15 e 40 minuti il profilo cresce di 38,2 MB, e 41,7 MB sono i buffer di lettura delle
connessioni HTTP verso S3 (`hyper`, `poll_read_from_io`) mentre le altre voci restano ferme o scendono (**MISURATO**, profilo
a campione). Il pool di connessioni di `object_store` non ha un limite di connessioni inattive e le chiude dopo 90 s senza
uso, cosa che con un motore sempre attivo non succede: il pool sale fino al picco di richieste contemporanee (descrittori
aperti da ~230 a ~300, **MISURATO**) e ogni connessione tiene un buffer che hyper allarga fino a ~400 KB dopo una risposta
grande. È limitato dal numero di connessioni, non dalle scritture: al massimo ~125 MB con 300 connessioni (**STIMATO**).
Non l'ho cambiato senza una prova su S3 vero (meno connessioni tenute vuol dire più handshake TLS); è una voce del tetto
globale proposto sotto.

**Costo in richieste** (contatori del motore, 50 × 2, 15 minuti): richieste di classe PUT 90.317 contro 90.198 (+0,13%),
GET 2.006 contro 1.550 (**MISURATO**). In dollari: ~+0,045 $ al mese per database (**STIMATO** dai prezzi del brief: 119 PUT
e 456 GET in più ogni 15 minuti per 50 database), su un costo PUT di ~26 $ al mese per database scritto 2 volte al secondo
(**STIMATO**: una PUT del WAL per scrittura).

**Carico misto** (50 database × 4 richieste/s, metà letture di 200 documenti precaricati, 15 minuti, ~90.000 letture e
~90.000 scritture): RSS 175 MB contro 525 a fine prova, 121 contro 523 dopo la chiusura (**MISURATO**). Le GET salgono da
1.550 a 4.479 (**MISURATO**): circa una GET per blocco letto dopo che la memtable è stata scaricata (2.500 blocchi da 4 KB
nei 50 database, **STIMATO**). In dollari ~+0,07 $ al mese per database (**STIMATO**). Latenza delle letture p50 1,45 contro
1,50 ms, p99 7,4 contro 10,3 ms; latenza delle scritture p99 182 contro 183 ms (**MISURATO**, MinIO, 4 prove in parallelo
sulla stessa macchina).

**Non misurato:** il comportamento su S3 vero (niente spese AWS in questo lavoro). Lo scarico per età produce al massimo uno
scarico L0 in più ogni 7,5-10 minuti per database lento; il problema di round3 su S3 vero era uno scarico al minuto su un
database a 100 richieste/s, che con l'età non viene toccato. Va comunque verificato con un run S3 prima di un rilascio.

## 5. Test di regressione

`ctl-tests/test_27_memory_per_write.py`:
- `test_memory_per_write_is_bounded`: 20 database × 10 scritture/s, 60 s di riscaldamento e 120 s misurati, con memtable a
  20 s e log dei change stream a 4 MB perché le strutture limitate arrivino al loro tetto durante il riscaldamento. Fallisce
  se l'heap vivo cresce più di 0,5 KB per scrittura, se la RSS cresce più di 0,8 KB per scrittura, o se dopo la chiusura
  l'heap resta più di 16 MB sopra la partenza.
  - Con la correzione: 0,185 KB di heap e 0,24 KB di RSS per scrittura; heap 7,8 MB prima, 27,2 a fine scritture, 14,8
    dopo la chiusura: passa (**MISURATO**).
  - Con il comportamento vecchio (`CTL_MEM_EXTRA="--memtable-max-age-secs 0 --cache-flushed-blocks true"`): 1,58 KB di heap
    e 1,66 KB di RSS per scrittura, heap 48 MB dopo la chiusura contro 8 alla partenza: fallisce (**MISURATO**).
- `test_idle_cursors_expire`: 40 cursori abbandonati si chiudono dopo `--cursor-timeout-secs 6`, uno usato ogni 2 s resta
  aperto, uno `noCursorTimeout` resta aperto; heap da 39 a 7,6 MB (**MISURATO**).

Il residuo di 0,185 KB per scrittura nel test, guardato col profilatore: buffer di lettura delle connessioni HTTP verso S3
in volo al momento del campione e blocchi letti dalle compattazioni (cache limitata), non una struttura che cresce con le
scritture (**MISURATO**, 7,4 MB in ~36.000 scritture).

CTL_PLACEHOLDER

## 6. Proposta: tetto globale di memoria con conto per database (non implementata)

**Perché.** Dopo le correzioni ogni struttura ha un limite, ma i limiti sono per database o per processo e non si
sommano in un numero solo: la memtable è limitata per database (8 MB stimati o 10 minuti di scritture), la cache a 256 + 64
MB, il log dei change stream a 32 MB, i cursori dal tempo. Con centinaia di database scritti velocemente la somma può
ancora superare la memoria della macchina: 500 database con la memtable piena farebbero ~5,5 GB di heap vivo (**STIMATO**:
500 × 8 MB × 1,4).

**Il conto per database.** Un registro in memoria, per database, con questi contatori aggiornati nel percorso già esistente:
- `memtable`: byte messi in memtable dall'ultimo scarico (sommati in `Tenant::stage`, azzerati quando `last_l0_seq` del
  manifest supera la scrittura più vecchia, lo stesso controllo che usa già lo scarico per età), moltiplicati per il
  rapporto misurato tra memoria vera e stima di slatedb (~1,4);
- `unflushed`: byte in attesa del WAL (già limitati da `max_unflushed_bytes`);
- `cursors`: dimensione BSON dei documenti tenuti dai cursori aperti del database;
- `changes`: la quota del database nel log dei change stream (la stima per evento c'è già);
- `fixed`: un costo fisso per database aperto (~0,2 MB misurati).
La cache e il pool di connessioni verso S3 restano voci di processo: la cache con il suo tetto fisso, il pool con un
limite di connessioni inattive (`pool_max_idle_per_host` di `object_store`) da tarare su S3 vero. Il totale del processo si confronta con l'heap vivo di jemalloc
(`stats.allocated`), che diventa il controllo di taratura delle stime.

**Il tetto.** `--memory-budget-mb` (default: il 70% del limite di memoria del cgroup, o della RAM). Due soglie:
- **sopra l'80% (morbida)**, in quest'ordine, senza mai rifiutare una richiesta: scaricare la memtable dei database con la
  quota più grande (al massimo una volta al minuto per database, per non ripetere lo stallo su S3 vero di round3);
  chiudere i database aperti e fermi da più tempo; ridurre il log dei change stream; chiudere prima i cursori fermi da più
  tempo; chiudere le connessioni S3 inattive in più;
- **sopra il 100% (dura)**: le scritture dei database sopra la loro quota equa (budget diviso per i database attivi, con un
  minimo garantito) aspettano nella coda equa già esistente (`fair.rs`); se l'attesa supera un limite rispondono con
  l'errore 462 già usato per i limiti di sicurezza, che i driver ritentano. Le letture e i database sotto quota non
  aspettano.

**Visibilità.** `GET /v1/metrics/databases` mostra per database le voci del conto, e per il processo il budget, il totale
stimato, l'heap vivo e quante volte ogni soglia è scattata.

**Come provarlo prima di attivarlo.** 500 database scritti a velocità diverse con un budget piccolo: la RSS deve restare
sotto il budget, nessuna scrittura confermata persa, latenza dei database sotto quota invariata; poi un run su S3 vero per
il costo e le latenze degli scarichi anticipati.

## File

- `tools/mem/memcheck.py`, `tools/mem/perdb.py`: gli script di misura.
- `ctl-tests/test_27_memory_per_write.py`: il test di regressione.
- `src/lifecycle.rs`: età della memtable, politica della cache.
- `mongo-server/src/main.rs`: jemalloc, flag nuovi, build di misura `heap-prof`.
- `mongo-server/src/cmd.rs`, `cmd_changes.rs`, `cmd_data.rs`, `diag.rs`: scadenza dei cursori e contatori in `serverStatus`.
- `results-mem-growth/`: le uscite grezze delle prove.
