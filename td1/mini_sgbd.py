import os

PAGE_SIZE = 4096        
RECORD_SIZE = 100         
RECORDS_PER_PAGE = PAGE_SIZE // RECORD_SIZE  


class MiniSGBD:
    def __init__(self, filename: str):
        self.filename = filename
        # Si le fichier n'existe pas, on le crée vide
        if not os.path.exists(self.filename):
            with open(self.filename, "wb") as f:
                pass  # juste créer le fichier

    def _get_file_size(self) -> int:
        """Retourne la taille du fichier en octets."""
        return os.path.getsize(self.filename)

    def get_record_count(self) -> int:
        """Nombre total d'enregistrements dans le fichier."""
        size = self._get_file_size()
        return size // RECORD_SIZE

    def insertRecord(self, data: str) -> None:
        # Encodage en bytes
        encoded = data.encode("utf-8")

        # Tronquer si trop long
        if len(encoded) > RECORD_SIZE:
            encoded = encoded[:RECORD_SIZE]

        # Padding avec des zéros si trop court
        padding_length = RECORD_SIZE - len(encoded)
        record_bytes = encoded + (b"\x00" * padding_length)

        # Écrire à la fin du fichier
        with open(self.filename, "ab") as f:
            f.write(record_bytes)

    def readRecord(self, record_id: int) -> str:
        total_records = self.get_record_count()
        if record_id < 0 or record_id >= total_records:
            raise IndexError(f"Record id {record_id} out of range (0..{total_records - 1})")

        offset = record_id * RECORD_SIZE

        with open(self.filename, "rb") as f:
            f.seek(offset)
            data = f.read(RECORD_SIZE)

        # Retirer les octets de padding (\x00) et décoder
        data = data.rstrip(b"\x00")
        return data.decode("utf-8", errors="ignore")

    def getPage(self, page_number: int):
        """
        Retourne tous les enregistrements d'une page sous forme de liste de strings.
        Exemple : getPage(0) -> étudiants 1 à 40 (si présents).
        """
        total_records = self.get_record_count()
        start_record = page_number * RECORDS_PER_PAGE
        end_record = start_record + RECORDS_PER_PAGE

        if start_record >= total_records:
            # Page vide / n'existe pas
            return []

        records = []
        for record_id in range(start_record, min(end_record, total_records)):
            records.append(self.readRecord(record_id))

        return records
