import os
import copy

PAGE_SIZE = 4096
RECORD_SIZE = 100
RECORDS_PER_PAGE = PAGE_SIZE // RECORD_SIZE

class MiniSGBD:
    def __init__(self, filename: str):
        self.filename = filename
        self.buffer = {}  # TIA
        self.TIV = {}  # Tampon d'images avant
        self.locks = set()  # Liste des verrous
        self.inTransaction = False  # Indicateur de transaction en cours
        self.file = open(filename, "r+b")  # Ouvre le fichier en mode lecture/écriture binaire

    def _get_file_size(self):
        return os.path.getsize(self.filename)

    def get_record_count(self):
        return self._get_file_size() // RECORD_SIZE

    def fix(self, page_id):
        if page_id not in self.buffer:
            page_data = bytearray(PAGE_SIZE)
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

    def begin(self):
        if self.inTransaction:
            self.commit()
        self.inTransaction = True

    def commit(self):
        if not self.inTransaction:
            raise Exception("Aucune transaction en cours.")
        
        for page_id, page in self.buffer.items():
            if page.is_transactional():
                self.force(page_id)  # Forcer les pages transactionnelles sur le disque
                self.release_lock(page_id)
        self.inTransaction = False

    def rollback(self):
        if not self.inTransaction:
            raise Exception("Aucune transaction en cours.")

        # Restaurer les anciennes valeurs à partir de TIV
        for page_id, page in self.buffer.items():
            if page.is_transactional():
                if page_id in self.TIV:
                    page_data = self.TIV[page_id]
                    page.get_data()[:] = page_data  # Restaurer les anciennes données
                    self.release_lock(page_id)
        self.TIV.clear()
        self.inTransaction = False

    def insert_record_sync(self, data):
        page_id = self.get_record_count() // RECORDS_PER_PAGE
        page = self.fix(page_id)

        record_data = bytearray(RECORD_SIZE)
        record_data[:len(data)] = data.encode('utf-8')

        # Sauvegarder les anciennes données dans TIV pour rollback
        self.TIV[page_id] = copy.deepcopy(page.get_data())
        
        page.insert_record(record_data, self.get_record_count() % RECORDS_PER_PAGE)
        page.set_transactional(True)  # Marquer la page comme transactionnelle
        self.use(page_id)

        self.force(page_id)

    def force(self, page_id):
        # Forcer l'écriture des pages modifiées sur le disque
        if page_id in self.buffer and self.buffer[page_id].is_dirty():
            page_data = self.buffer[page_id].get_data()
            with open(self.filename, "r+b") as file:
                file.seek(page_id * PAGE_SIZE)
                file.write(page_data)
            self.buffer[page_id].set_dirty(False)

    def read_record(self, record_id):
        page_id = record_id // RECORDS_PER_PAGE
        page = self.fix(page_id)
        
        # Si la page est verrouillée, lire dans le TIV (ancienne valeur)
        if page_id in self.locks:
            return self.TIV.get(page_id, bytearray(RECORD_SIZE))
        
        # Si la transaction est en cours et non verrouillée, lire dans le TIA (nouvelle valeur)
        return page.read_record(record_id % RECORDS_PER_PAGE).decode('utf-8', errors='ignore')

    def get_page(self, page_number):
        page_id = page_number
        page = self.fix(page_id)
        records = []
        for i in range(RECORDS_PER_PAGE):
            record = page.read_record(i)
            records.append(record.decode('utf-8', errors='ignore'))
        return records

    def close(self):
        """Ferme le fichier de manière propre."""
        self.file.close()



class Page:
    def __init__(self, data=None):
        self.data = data if data else bytearray(4096)  # Page de 4096 octets
        self.dirty = False  # Indique si la page a été modifiée
        self.transactional = False  # Indique si la page appartient à une transaction
        self.locked = False  # Indique si la page est verrouillée

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
        start = record_index * 100  # Chaque enregistrement fait 100 octets
        self.data[start:start + 100] = record_data
        self.set_dirty()

    def read_record(self, record_index):
        start = record_index * 100
        return self.data[start:start + 100]
