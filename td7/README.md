# TD7 - Journalisation et Récupération sur Panne

## Objectif
Implémentation des mécanismes de reprise sur panne avec journalisation, checkpoints et algorithmes UNDO/REDO.

## Fonctionnalités implémentées

### 1. Journal de Transactions (TJT/FJT)
- **TJT** : Tampon de Journal de Transactions en mémoire
- **FJT** : Fichier de Journal de Transactions sur disque
- Enregistrement de toutes les opérations : BEGIN, INSERT, UPDATE, COMMIT, ROLLBACK, CHECKPOINT

### 2. Classe LogEntry
Structure d'une entrée de journal :
- `transaction_id` : ID de la transaction
- `record_id` : ID de l'enregistrement concerné
- `before_image` : Image avant modification (pour UNDO)
- `after_image` : Image après modification (pour REDO)
- `log_type` : Type d'opération (BEGIN, INSERT, UPDATE, etc.)

### 3. Checkpoints
- Méthode `checkpoint()` qui :
  - Force l'écriture de toutes les pages modifiées sur disque
  - Ajoute une entrée CHECKPOINT dans le journal
  - Écrit le journal sur disque

### 4. Algorithme de Récupération
La méthode `recover()` implémente trois phases :

#### Phase 1 : Analyse
- Lecture complète du journal
- Identification des transactions commitées
- Identification des transactions non commitées

#### Phase 2 : REDO
- Rejoue toutes les opérations des transactions commitées depuis le dernier checkpoint
- Réapplique les "images après" sur disque

#### Phase 3 : UNDO
- Annule toutes les opérations des transactions non commitées
- Restaure les "images avant" sur disque

### 5. Modifications du COMMIT et ROLLBACK

**COMMIT** :
- N'écrit plus directement sur disque (différé jusqu'au checkpoint ou récupération)
- Ajoute une entrée COMMIT dans le journal
- Force l'écriture du journal sur disque

**ROLLBACK** :
- Restaure les valeurs depuis le TIV
- Ajoute une entrée ROLLBACK dans le journal
- Force l'écriture du journal sur disque

### 6. Simulation de Crash
- Méthode `crash()` qui vide tous les buffers sans sauvegarder
- Simule une panne système

## Utilisation

```python
from mini_sgbd import MiniSGBD

# Créer une instance
db = MiniSGBD("etudiants.db")

# Commencer une transaction
db.begin()

# Insérer des enregistrements
db.insert_record_sync("Etudiant 1")
db.insert_record_sync("Etudiant 2")

# Mettre à jour un enregistrement
db.update_record(0, "Etudiant 1 modifié")

# Valider la transaction
db.commit()

# Créer un checkpoint
db.checkpoint()

# Simuler un crash
db.crash()

# Récupérer après le crash
db2 = MiniSGBD("etudiants.db")
db2.recover()

# Vérifier les données
print(db2.read_record(0))
```

## Tests

Le fichier `main_td7.py` contient trois tests :

1. **TEST 1** : Transactions commitées et non commitées avant crash
   - Vérifie que les transactions commitées sont préservées
   - Vérifie que les transactions non commitées sont annulées

2. **TEST 2** : Transactions avec UPDATE
   - Vérifie le REDO des UPDATE commitées
   - Vérifie le UNDO des UPDATE non commitées

3. **TEST 3** : Rollback explicite
   - Vérifie que le ROLLBACK annule correctement les modifications

## Exécution des tests

```bash
cd td7
python main_td7.py
```

## Points clés

- Le journal est écrit sur disque à chaque COMMIT/ROLLBACK
- Les checkpoints réduisent la quantité de log à rejouer lors de la récupération
- Les transactions non commitées au moment du crash sont annulées
- Les transactions commitées sont rejouées (REDO) pour garantir la durabilité
