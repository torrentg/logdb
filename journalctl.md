# journalctl

## Objectiu

Crear un programa anomenat journalctl.
Aquest programa permet de fer un conjunt d'operacions bàsiques sobre un fitxer
creat amb la llibreria `journal`. No preten fer insercions ni cerques avançades,
això es fa programaticament, sinó operacions de manteniment (purge, roolback) 
i consulta básiques (quantes dades conté? quines son aquestes dades?, etc).

## Descripció

El programa consta d'un únic fitxer: journalctl.c.
El programa retorna EXIT_SUCCESS (tasca encomanada realitzada) o EXIT_FAILURE 
(errors validació paràmetres o imposibilitat de realitzar la tasca encomenada).
El programa reporta els errors bloquejants a stderr (els errors que condueixen
a EXIT_FAILURE). La resta de la sortida la fa a stdout.
La aplicació utilitza getopt_long() per parsejar les opcions de la linia de comandes.
La aplicació suporta varis modes de funcionament (veure detalls a sota).
En tots ells, el nom del fitxer correspon al basename (nom del fitxer sense extensió)
En tots els modes, al obrir el journal, es reconstrueix automaticament el fitxer 
idx si aquest no existeix o està corromput i s'actualitza el fitxer dat si conté 
dades parcials (no commitades).
El programa honora la variable d'entorn TZ que indica el timezone a emprar. Els
timestamps es mostren amb format ISO-8601 amb timezone.

### Mode help

Linia de comandes:
    > journalctl -h

Opcions:

* -h, --help: Mostra l'ajuda.

Mostra l'informació d'ajuda en anglés seguint el format estàndar en les apps de 
linux. Inclou una petita descripció, linia de comandes esperada, descripció de 
les opcions, exemples, indica quins son el return codes, informació de 
llicencia, etc. Indica que empra la variable d'entorn TZ per mostrar els 
timestamps.

### Mode summary (default)

Linia de comandes:
    > journalctl [-p path] <NAME>

Opcions:

* -p, --path=PATH: Path dels fitxers (opcional, per defecte path actual)
* -c, --check: Comprova totes les entrades (opcional, default = false).

Errors:

* no s'ha indicat NAME
* PATH no existeix
* PATH/name.dat no existeix

L'opció check obre el journal amb la opció `check`. En aquest cas es reporten
els warnings de validació en el summary. Mostra un resum del contingut del 
journal. Proposo el següent:

* Data: <nom fitxer>, tamany, estat [OK, invalid header, corrupted, to_rebuilt, etc]
* Index: <nom fitxer>, tamany, estat [OK, corrupted, etc]
* Format: format number
* Metadata: Content in hexadecimal
* First entry: num, timestamp
* Last entry: num, timestamp
* Number of entries: num
* Warnings: <check warnings>
* Mostra el procés que està utilitzan el fitxer (si existeix)

## Mode details

Linia de comandes:
    > journalctl --details [-f NUM] [-t NUM] [-p path] <NAME>

Options:

* -p, --path=PATH: Path dels fitxers (opcional, per defecte path actual)
* -f, --from=NUM: primer seqnum (opcional, default al primer registre del fitxer)
* -t, --to=NUM: darrer seqnum (opcional, default al darrer registre del fitxer)
* -b, --bulk: Mostra el contingut de les dades binaries (optional, false per defecte)

Errors:

* no s'ha indicat NAME
* PATH no existeix
* PATH/name.dat no existeix
* from > to

Llista els registres de `from` a `to`, ambdós inclosos. Si `from` és inferior al
primer registre, considerem from=first_reg, si `to` és superior al darrer 
registre, considerem to=last_reg. Mostra un missatge si no hi ha registres 
en el rang indicat.

Format de la llista de sortida:

> seqnum, timestamp (YYYY-MM-DDThh:mm:ss.xxx), length
>    <contingut en hexadecimal, 80-col>

Exemple:

25, 2026-01-11T08:06:41.399Z, 234, 0x1234ABCD
    0000: 00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F  ................
    0010: 10 11 12 13 14 15 16 17 18 19 1A 1B 1C 1D 1E 1F  ................
    0020: 20 21 22 23 24 25 26 27 28 29 2A 2B 2C 2D 2E 2F   !"#$%&'()*+,-./
    0030: 30 31 32 33 34 35 36 37 38 39 3A 3B 3C 3D 3E 3F  0123456789:;<=>?
    0040: 40 41 42 43 44 45 46 47 48 49 4A 4B 4C 4D 4E 4F  @ABCDEFGHIJKLMNO
    0050: 50 51 52 53 54 55 56 57 58 59 5A 5B 5C 5D 5E 5F  PQRSTUVWXYZ[\]^_
    0060: 60 61 62 63 64 65 66 67 68 69 6A 6B 6C 6D 6E 6F  `abcdefghijklmno
    0070: 70 71 72 73 74 75 76 77 78 79 7A 7B 7C 7D 7E 7F  pqrstuvwxyz{|}~.
    0080: 80 81 82 83 84 85 86 87 88 89 8A 8B 8C 8D 8E 8F  ................
    0090: 90 91 92 93 94 95 96 97 98 99 9A 9B 9C 9D 9E 9F  ................
    00A0: A0 A1 A2 A3 A4 A5 A6 A7 A8 A9 AA AB AC AD AE AF  ................
    00B0: B0 B1 B2 B3 B4 B5 B6 B7 B8 B9 BA BB BC BD BE BF  ................
    00C0: C0 C1 C2 C3 C4 C5 C6 C7 C8 C9 CA CB CC CD CE CF  ................
    00D0: D0 D1 D2 D3 D4 D5 D6 D7 D8 D9 DA DB DC DD DE DF  ................
    00E0: E0 E1 E2 E3 E4 E5 E6 E7 E8 E9                    ..........
26, 2026-01-11T08:06:41.409Z, 67, 0x5678ABCD
    etc

### Mode purge

Linia de comandes:
    > journalctl --purge [-n NUM] [-s SEQ] [-o file] [-p path] <NAME>

Options:

* -p, --path=PATH: Path dels fitxers (opcional, per defecte path actual)
* -n, --num=NUM: numero de primers registres a suprimir. Suprimeix de 
                 first_seq fins a fisrt_seq + NUM - 1 (ambdós inclosos).
* -s, --seq=SEQ: Primer seqnum del nou fitxer. Suprimeix de first_seq
                 fins SEQ - 1 (ambdós inclosos).

Errors:

* no s'ha indicat NAME
* PATH no existeix
* PATH/name.dat no existeix
* No s'ha indicat ni -n ni -s
* S'ha indicat simulataniament -n i -s
* NUM o SEQ no enters estrictament positius
* No hi ha registres a suprimir (SEQ < first_seq)
* -o te un nom de fitxer incorrecte o ja existent

L'opció -o crea una copia de PATH/name.dat a PATH/BCK.dat abans de modificar.
Aplica ldb_purge(). Mostrem warning si comporta suprimir tots els registres.

### Mode rollback

Linia de comandes:
    > journalctl --rollback [-n NUM] [-s SEQ] [-o file] [-p path] <NAME>

Options:

* -p, --path=PATH: Path dels fitxers (opcional, per defecte path actual)
* -n, --num=NUM: numero de darrers registres a suprimir. Suprimeix de 
                 last_seq - NUM + 1 fins a last_seq (ambdós inclosos).
* -s, --seq=SEQ: Darrer seqnum del nou fitxer. Suprimeix de SEQ + 1
                 fins last_seq (ambdós inclosos).

Errors:

* no s'ha indicat NAME
* PATH no existeix
* PATH/name.dat no existeix
* No s'ha indicat ni -n ni -s
* S'ha indicat simulataniament -n i -s
* NUM o SEQ no enters estrictament positius
* No hi ha registres a suprimir (SEQ > last_seq)
* -o te un nom de fitxer incorrecte o ja existent

L'opció -o crea una copia de PATH/name.dat a PATH/BCK.dat abans de modificar.
Aplica ldb_rollback(). Mostrem warning si comporta suprimir tots els registres.
