# Foldbook Server

Foldbook Server tourne sur ton NAS, à côté de tes BD, mangas et livres. Il les rend lisibles par les personnes que tu invites dans l'app [Foldbook](https://foldbook.app), où qu'elles soient, et ne leur montre que ce que tu leur as ouvert.

- Aucun port à ouvrir sur ta box : le serveur se connecte lui-même au relais de Foldbook (`relay.foldbook.app`), qui transmet les pages sans les garder.
- Ta bibliothèque est montée en lecture seule : le serveur ne modifie jamais tes fichiers.
- Chaque requête porte le jeton du compte Foldbook de la personne ; le serveur le vérifie, puis applique ses droits (bibliothèques ouvertes, titres masqués).

## Installer

Guide complet : https://foldbook.app/serveur

```yaml
services:
  foldbook:
    image: ghcr.io/milrer/foldbook-server:latest
    container_name: foldbook-server
    restart: unless-stopped
    ports:
      - "8686:8686"
    volumes:
      - /volume1/Media:/library:ro
      - /volume1/docker/foldbook:/data
```

Ouvre ensuite `http://adresse-du-nas:8686` : la page affiche un code à six chiffres, à saisir dans Foldbook (Réglages › Bibliothèque partagée › Associer un serveur).

## Variables

| Variable | Par défaut | Rôle |
| --- | --- | --- |
| `LIBRARY_DIR` | `/library` | Le dossier des livres ; ses sous-dossiers sont les bibliothèques. |
| `DATA_DIR` | `/data` | L'identité du serveur (`server.json`). |
| `PORT` | `8686` | Le port local (page d'état et accès direct sur le réseau local). |
| `SERVER_NAME` | le nom de la machine | Le nom proposé à l'association. |
