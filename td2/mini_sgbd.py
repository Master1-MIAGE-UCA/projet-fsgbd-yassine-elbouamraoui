import os

PAGE_SIZE = 4096
RECORD_SIZE = 100
RECORDS_PER_PAGE = PAGE_SIZE // RECORD_SIZE

# Classe Page pour gérer les pages en mémoire
class Page:
    def __init__(self, data=None):
        self.data = data if data else bytearray(PAGE_SIZE)
        self.dirty = False

    def set_dirty(self, value=True):
        self.dirty = value

    def is_dirty(self):
        return self.dirty

    def get_data(self):
        return self.data

    def insert_record(self, record_data, record_index):
        start = record_index * RECORD_SIZE
        self.data[start:start + RECORD_SIZE] = record_data
        self.set_dirty()

    def read_record(self, record_index):
        start = record_index * RECORD_SIZE
        return self.data[start:start + RECORD_SIZE]


# Classe MiniSGBD pour gérer les opérations de base de données
class MiniSGBD:
    def __init__(self, filename: str):
        self.filename = filename
        self.buffer = {}
        self.usage_count = {}
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

        self.usage_count[page_id] = self.usage_count.get(page_id, 0) + 1
        return self.buffer[page_id]

    def unfix(self, page_id):
        if page_id in self.usage_count:
            self.usage_count[page_id] -= 1
            if self.usage_count[page_id] == 0:
                del self.buffer[page_id]

    def use(self, page_id):
        if page_id in self.buffer:
            self.buffer[page_id].set_dirty(True)

    def force(self, page_id):
        if page_id in self.buffer and self.buffer[page_id].is_dirty():
            page_data = self.buffer[page_id].get_data()
            with open(self.filename, "r+b") as file:
                file.seek(page_id * PAGE_SIZE)
                file.write(page_data)
            self.buffer[page_id].set_dirty(False)

    def insert_record_sync(self, data):
        page_id = self.get_record_count() // RECORDS_PER_PAGE
        page = self.fix(page_id)

        record_data = bytearray(RECORD_SIZE)
        record_data[:len(data)] = data.encode('utf-8')

        page.insert_record(record_data, self.get_record_count() % RECORDS_PER_PAGE)
        self.use(page_id)
        self.force(page_id)

    def insert_record(self, data):
        page_id = self.get_record_count() // RECORDS_PER_PAGE
        page = self.fix(page_id)

        record_data = bytearray(RECORD_SIZE)
        record_data[:len(data)] = data.encode('utf-8')

        page.insert_record(record_data, self.get_record_count() % RECORDS_PER_PAGE)
        self.use(page_id)

    def read_record(self, record_id):
        page_id = record_id // RECORDS_PER_PAGE
        page = self.fix(page_id)
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
