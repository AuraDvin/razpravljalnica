# Razpravljalnica 
Projekt pri predmetu Vzporedni in Porazdeljeni Sistemi in Algoritmi. 

Deluje na sistemu gRPC v jeziku go 

## Grajenje in uporaba projekta 

Če projekt želimo zagnati na svoji napravi lahko uporabimo naslednje ukaze
```bash
go build -o ./out/razpravljalnica ./grpc 

# Strežnik 
./razpravljalnica

# Odjemalec
./razpravljalnica -s $(hostname)

``` 
## Z uporabo Visual Studio Code
Lahko tudi uporabite `.vscode/launch.json` da zaženete Strežnik in nato Odjemalca znotraj Visual studio code 

# Ukazi:
  ```
   1. createuser \<ime>                         - Ustvari novega uporabnika
   2. setuser \<id>                             - Nastavi trenutnega uporabnika
   3. getuser \<id>                             - Dobi uporabniške informacije
   4. createtopic \<ime>                        - Ustvari novo temo
   5. listtopics                                - Seznam vseh tem
   6. postmessage \<topic_id> \<besedilo>       - Objavi sporočilo
   7. getmessages \<topic_id> [from_id] [limit] - Prejmi sporočila
   8. likemessage \<topic_id> \<msg_id>         - Všečkaj sporočilo
   9. subscribe \<topic_id>[,\<topic_id>]       - Naroči se na teme (seznam z vejicami)
  10. unsubscribe                               - Prekini vse naročnine
  11. exit                                      - Izhod
  ```
  