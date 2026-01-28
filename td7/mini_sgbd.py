import os
import copy
import pickle
from enum import Enum

PAGE_SIZE = 4096
RECORD_SIZE = 100
RECORDS_PER_PAGE = PAGE_SIZE // RECORD_SIZE


class LogType(Enum):
    """Types d'entrées dans le journal"""
    BEGIN = "BEGIN"
    UPDATE = "UPDATE"
    INSERT = "INSERT"
    DELETE = "DELETE"
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"
    CHECKPOINT = "CHECKPOINT"


class LogEntry:
    """Entrée dans le journal de transactions"""
    def __init__(self, transaction_id, record_id=None, before_image=None, after_image=None, log_type=None):
        self.transaction_id = transaction_id
        self.record_id = record_id
        self.before_image = before_image
        self.after_image = after_image
        self.log_type = log_type

    def __repr__(self):
        return f"LogEntry(tid={self.transaction_id}, type={self.log_type}, record={self.record_id})"


class Page:
    def __init__(self, data=None):
        self.data = data if data else bytearray(PAGE_SIZE)
        self.dirty = False
        self.transactional = False
        self.locked = False

    def set_dirty(self, value=True):
        self.dirty = value

    def is_dirty(self):
        return self.dirty

    def set_transactional(self, value=True):
        self.transactional = value

    def is_transactional(self):
        return self.transactional

    def lock(self):
        self.locked = True

    def unlock(self):
        self.locked = False

    def get_data(self):
        return self.data

    def insert_record(self, record_data, record_index):
        start = record_index * RECORD_SIZE
        self.data[start:start + RECORD_SIZE] = record_data
        self.set_dirty()

    def read_record(self, record_index):
        start = record_index * RECORD_SIZE
        return self.data[start:start + RECORD_SIZE]


class MiniSGBD:
    def __init__(self, filename: str, log_filename: str = "transaction.log"):
        self.filename = filename
        self.log_filename = log_filename
        self.buffer = {}  # TIA (Tampon d'Images Après)
        self.TIV = {}  # Tampon d'images avant
        self.TJT = []  # Tampon de Journal de Transactions (en mémoire)
        self.locks = set()  # Liste des verrous
        self.inTransaction = False  # Indicateur de transaction en cours
        self.transaction_id = 0  # Compteur d'ID de transaction
        self.current_transaction_id = None  # ID de la transaction courante
        self.record_counter = 0  # Compteur d'enregistrements insérés
        
        # Créer le fichier de données s'il n'existe pas
        if not os.path.exists(filename):
            with open(filename, "wb") as f:
                pass
        else:
            # Initialiser le compteur avec le nombre d'enregistrements existants
            self.record_counter = self._get_file_size() // RECORD_SIZE
        self.file = open(filename, "r+b")

    def _get_file_size(self):
        return os.path.getsize(self.filename)

    def get_record_count(self):
        return self._get_file_size() // RECORD_SIZE

    def fix(self, page_id):
        if page_id not in self.buffer:
            page_data = bytearray(PAGE_SIZE)
            file_size = self._get_file_size()
            if page_id * PAGE_SIZE < file_size:
                with open(self.filename, "rb") as file:
                    file.seek(page_id * PAGE_SIZE)
                    file.readinto(page_data)
            self.buffer[page_id] = Page(page_data)
        return self.buffer[page_id]

    def unfix(self, page_id):
        if page_id in self.buffer:
            del self.buffer[page_id]

    def use(self, page_id):
        if page_id in self.buffer:
            self.buffer[page_id].set_dirty(True)

    def lock(self, record_id):
        if record_id in self.locks:
            raise Exception(f"Enregistrement {record_id} est déjà verrouillé")
        self.locks.add(record_id)

    def release_lock(self, record_id):
        if record_id in self.locks:
            self.locks.remove(record_id)

    def _write_log_to_file(self):
        """Écrire le TJT dans le FJT (Fichier de Journal de Transactions)"""
        with open(self.log_filename, "ab") as f:
            for entry in self.TJT:
                pickle.dump(entry, f)
        self.TJT.clear()

    def _read_log_from_file(self):
        """Lire le FJT et retourner toutes les entrées"""
        if not os.path.exists(self.log_filename):
            return []
        
        entries = []
        with open(self.log_filename, "rb") as f:
            try:
                while True:
                    entry = pickle.load(f)
                    entries.append(entry)
            except EOFError:
                pass
        return entries

    def begin(self):
        """Commencer une nouvelle transaction"""
        if self.inTransaction:
            self.commit()
        
        self.transaction_id += 1
        self.current_transaction_id = self.transaction_id
        self.inTransaction = True
        
        # Ajouter une entrée BEGIN dans le journal
        log_entry = LogEntry(self.current_transaction_id, log_type=LogType.BEGIN)
        self.TJT.append(log_entry)

    def commit(self):
        """Valider la transaction courante"""
        if not self.inTransaction:
            raise Exception("Aucune transaction en cours.")
        
        # Ajouter une entrée COMMIT dans le journal
        log_entry = LogEntry(self.current_transaction_id, log_type=LogType.COMMIT)
        self.TJT.append(log_entry)
        
        # Écrire le TJT dans le FJT
        self._write_log_to_file()
        
        # Libérer les verrous et nettoyer les états transactionnels
        for page_id, page in list(self.buffer.items()):
            if page.is_transactional():
                page.set_transactional(False)
                if page_id in self.locks:
                    self.release_lock(page_id)
        
        self.TIV.clear()
        self.inTransaction = False
        self.current_transaction_id = None

    def rollback(self):
        """Annuler la transaction courante"""
        if not self.inTransaction:
            raise Exception("Aucune transaction en cours.")

        # Compter combien d'enregistrements ont été insérés dans cette transaction
        inserts_count = 0
        for entry in self.TJT:
            if entry.transaction_id == self.current_transaction_id and entry.log_type == LogType.INSERT:
                inserts_count += 1
        
        # Restaurer les anciennes valeurs à partir de TIV
        for page_id, page in list(self.buffer.items()):
            if page.is_transactional():
                if page_id in self.TIV:
                    page_data = self.TIV[page_id]
                    page.get_data()[:] = page_data
                    page.set_transactional(False)
                if page_id in self.locks:
                    self.release_lock(page_id)
        
        # Réduire le compteur d'enregistrements
        self.record_counter -= inserts_count
        
        # Ajouter une entrée ROLLBACK dans le journal
        log_entry = LogEntry(self.current_transaction_id, log_type=LogType.ROLLBACK)
        self.TJT.append(log_entry)
        
        # Écrire le TJT dans le FJT
        self._write_log_to_file()
        
        self.TIV.clear()
        self.inTransaction = False
        self.current_transaction_id = None

    def insert_record_sync(self, data):
        """Insérer un enregistrement de manière synchrone"""
        if not self.inTransaction:
            raise Exception("Aucune transaction en cours. Appelez begin() d'abord.")
        
        record_id = self.record_counter
        page_id = record_id // RECORDS_PER_PAGE
        page = self.fix(page_id)

        record_data = bytearray(RECORD_SIZE)
        record_data[:len(data)] = data.encode('utf-8')

        # Sauvegarder les anciennes données dans TIV pour rollback
        if page_id not in self.TIV:
            self.TIV[page_id] = copy.deepcopy(page.get_data())
        
        # Ajouter une entrée INSERT dans le journal
        log_entry = LogEntry(
            self.current_transaction_id,
            record_id=record_id,
            before_image=None,
            after_image=copy.deepcopy(record_data),
            log_type=LogType.INSERT
        )
        self.TJT.append(log_entry)
        
        page.insert_record(record_data, record_id % RECORDS_PER_PAGE)
        page.set_transactional(True)
        self.use(page_id)
        
        self.record_counter += 1

    def update_record(self, record_id, data):
        """Mettre à jour un enregistrement"""
        if not self.inTransaction:
            raise Exception("Aucune transaction en cours. Appelez begin() d'abord.")
        
        page_id = record_id // RECORDS_PER_PAGE
        page = self.fix(page_id)
        
        # Lire l'ancienne valeur
        before_image = copy.deepcopy(page.read_record(record_id % RECORDS_PER_PAGE))
        
        # Sauvegarder les anciennes données dans TIV pour rollback
        if page_id not in self.TIV:
            self.TIV[page_id] = copy.deepcopy(page.get_data())
        
        # Nouvelle valeur
        record_data = bytearray(RECORD_SIZE)
        record_data[:len(data)] = data.encode('utf-8')
        
        # Ajouter une entrée UPDATE dans le journal
        log_entry = LogEntry(
            self.current_transaction_id,
            record_id=record_id,
            before_image=before_image,
            after_image=copy.deepcopy(record_data),
            log_type=LogType.UPDATE
        )
        self.TJT.append(log_entry)
        
        page.insert_record(record_data, record_id % RECORDS_PER_PAGE)
        page.set_transactional(True)
        self.use(page_id)

    def force(self, page_id):
        """Forcer l'écriture d'une page sur disque"""
        if page_id in self.buffer and self.buffer[page_id].is_dirty():
            page_data = self.buffer[page_id].get_data()
            with open(self.filename, "r+b") as file:
                file.seek(page_id * PAGE_SIZE)
                file.write(page_data)
            self.buffer[page_id].set_dirty(False)

    def checkpoint(self):
        """Créer un point de sauvegarde"""
        # Forcer l'écriture de toutes les pages modifiées sur disque
        for page_id, page in self.buffer.items():
            if page.is_dirty():
                self.force(page_id)
        
        # Ajouter une entrée CHECKPOINT dans le journal
        log_entry = LogEntry(None, log_type=LogType.CHECKPOINT)
        self.TJT.append(log_entry)
        
        # Écrire le TJT dans le FJT
        self._write_log_to_file()
        
        print("Checkpoint créé avec succès.")

    def crash(self):
        """Simuler un crash : vider tous les buffers sans sauvegarder"""
        print("Simulation d'un crash : vidage des buffers...")
        self.buffer.clear()
        self.TIV.clear()
        self.TJT.clear()
        self.locks.clear()
        self.inTransaction = False
        self.current_transaction_id = None
        print("Crash simulé avec succès.")

    def recover(self):
        """Récupération après un crash : algorithme UNDO/REDO"""
        print("Début de la récupération...")
        
        # Lire le journal depuis le fichier
        log_entries = self._read_log_from_file()
        
        if not log_entries:
            print("Aucune entrée dans le journal. Rien à récupérer.")
            return
        
        # Phase 1 : Analyse du journal
        committed_transactions = set()
        active_transactions = set()
        all_transactions = set()
        last_checkpoint_index = -1
        
        for i, entry in enumerate(log_entries):
            if entry.log_type == LogType.CHECKPOINT:
                last_checkpoint_index = i
                # Réinitialiser les transactions actives après un checkpoint
                active_transactions.clear()
            elif entry.log_type == LogType.BEGIN:
                active_transactions.add(entry.transaction_id)
                all_transactions.add(entry.transaction_id)
            elif entry.log_type == LogType.COMMIT:
                committed_transactions.add(entry.transaction_id)
                active_transactions.discard(entry.transaction_id)
            elif entry.log_type == LogType.ROLLBACK:
                active_transactions.discard(entry.transaction_id)
        
        # Les transactions non commitées sont celles qui sont encore actives
        uncommitted_transactions = active_transactions
        
        print(f"Transactions commitées : {committed_transactions}")
        print(f"Transactions non commitées : {uncommitted_transactions}")
        
        # Phase 2 : REDO (refaire les transactions commitées)
        print("Phase REDO...")
        start_index = last_checkpoint_index if last_checkpoint_index >= 0 else 0
        
        for entry in log_entries[start_index:]:
            if entry.transaction_id in committed_transactions:
                if entry.log_type == LogType.INSERT or entry.log_type == LogType.UPDATE:
                    # Réappliquer l'image après
                    record_id = entry.record_id
                    page_id = record_id // RECORDS_PER_PAGE
                    page = self.fix(page_id)
                    page.insert_record(entry.after_image, record_id % RECORDS_PER_PAGE)
                    self.use(page_id)
                    self.force(page_id)
                    print(f"  REDO: {entry.log_type.value} record {record_id}")
        
        # Phase 3 : UNDO (annuler les transactions non commitées)
        print("Phase UNDO...")
        for entry in reversed(log_entries[start_index:]):
            if entry.transaction_id in uncommitted_transactions:
                if entry.log_type == LogType.UPDATE:
                    # Restaurer l'image avant
                    record_id = entry.record_id
                    page_id = record_id // RECORDS_PER_PAGE
                    page = self.fix(page_id)
                    page.insert_record(entry.before_image, record_id % RECORDS_PER_PAGE)
                    self.use(page_id)
                    self.force(page_id)
                    print(f"  UNDO: UPDATE record {record_id}")
                elif entry.log_type == LogType.INSERT:
                    # Supprimer l'enregistrement inséré (le marquer comme vide)
                    record_id = entry.record_id
                    page_id = record_id // RECORDS_PER_PAGE
                    page = self.fix(page_id)
                    empty_record = bytearray(RECORD_SIZE)
                    page.insert_record(empty_record, record_id % RECORDS_PER_PAGE)
                    self.use(page_id)
                    self.force(page_id)
                    print(f"  UNDO: INSERT record {entry.record_id}")
        
        print("Récupération terminée avec succès.")

    def read_record(self, record_id):
        """Lire un enregistrement"""
        page_id = record_id // RECORDS_PER_PAGE
        page = self.fix(page_id)
        
        # Si la page est verrouillée, lire dans le TIV (ancienne valeur)
        if page_id in self.locks:
            if page_id in self.TIV:
                tiv_page = Page(self.TIV[page_id])
                record_bytes = tiv_page.read_record(record_id % RECORDS_PER_PAGE)
                record_bytes = record_bytes.rstrip(b'\x00')
                return record_bytes.decode('utf-8', errors='ignore')
        
        # Sinon, lire dans le TIA (nouvelle valeur)
        record_bytes = page.read_record(record_id % RECORDS_PER_PAGE)
        record_bytes = record_bytes.rstrip(b'\x00')
        return record_bytes.decode('utf-8', errors='ignore')

    def get_page(self, page_number):
        """Lire tous les enregistrements d'une page"""
        page_id = page_number
        page = self.fix(page_id)
        records = []
        for i in range(RECORDS_PER_PAGE):
            record = page.read_record(i)
            record = record.rstrip(b'\x00')
            decoded = record.decode('utf-8', errors='ignore')
            if decoded:
                records.append(decoded)
        return records

    def close(self):
        """Fermer le fichier de manière propre"""
        self.file.close()
