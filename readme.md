# Razpravljalnica 
Projekt pri predmetu Vzporedni in Porazdeljeni Sistemi in Algoritmi. 

Deluje na sistemu gRPC v jeziku go.

## Grajenje in uporaba projekta 

Če projekt želimo zagnati na svoji napravi lahko uporabimo naslednje ukaze.
Za delovanje potrebuje direktorij logs v katerem ustvari datoteko `server.log`.
```bash
mkdir -p logs
# strežnik
go run ./grpc/ 

# odjemalec
go run ./grpc/ -s localhost
```
### Grajenje izvršile datoteke
```bash
go build -o ./out/razpravljalnica ./grpc 
cd out
mkdir -p logs

# Strežnik 
# -p označuje port številko, privzeto 9876
# -n oznacuje koliko vozlišč replikacije, privzeto 3 (glava, rep in vmesno vozlišče)
./razpravljalnica

# Odjemalec 
# -s za naziv strežnika (nadzorne ravnine)
# -p port mora biti isti kot nadzorne ravnine
./razpravljalnica -s localhost

``` 
## Z uporabo Visual Studio Code
Lahko tudi uporabite `.vscode/launch.json` da zaženete Strežnik in nato Odjemalca znotraj Visual studio code 

# Ukazi:
  ```
   1. createuser <ime>                         - Ustvari novega uporabnika
   2. setuser <id>                             - Nastavi trenutnega uporabnika
   3. getuser <id>                             - Dobi uporabniške informacije
   4. createtopic <ime>                        - Ustvari novo temo
   5. listtopics                               - Seznam vseh tem
   6. postmessage <topic_id> <besedilo>        - Objavi sporočilo
   7. getmessages <topic_id> [from_id[limit]]  - Prejmi sporočila
   8. likemessage <topic_id> <msg_id>          - Všečkaj sporočilo
   9. subscribe <topic_id>[,<topic_id>]        - Naroči se na teme (seznam z vejicami)
  10. unsubscribe                              - Prekini vse naročnine
  11. exit                                     - Izhod
  ```
# Uporaba aplikacije 
Začnemo z ustvarjenjem novega uporabnika.
```bash
> createuser <ime uporabnika>
Uporabnik ustvarjen: ID=1, Ime=<ime uporabnika>
```
Nato prevzamemo identiteto z ID številko odgovora strežnika.
```bash
> setuser 1
```
Potem lahko uporabljamo ostale, zgoraj navedene ukaze ukaze. (ustvarjanje tem, pošiljanje sporočil, naročanje ipd.).

Omejimo lahko pridobivanje sporočil, vpišemo ID teme, nato ID uporabika (ali 0 - privzeto za vse uporabnike) nato pa omejitev števila sporočil (privzeto 100).

Pridobivanje zadnjih 10 sporočil teme ID=1:
```bash
> getmessages 1 0 10 
```

# Replikacija
Vozlišče glava -> pisanje in urejanje naročnin

Vozlišče rep -> branje

Vozlišča uporabljajo grpc za potrditve in komunikacijo z nadzorno ravnino
